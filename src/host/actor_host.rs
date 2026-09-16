use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinError, JoinSet},
};
use tracing::error;

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure, ActorKey,
        ActorSocketEvent, ActorSocketInvocation, ActorSocketPublisher, ActorSocketSource,
    },
    actor_state::ActorStorageKey,
    state_transport::StateTransport,
};

use super::{
    HostEndpoint,
    actor_runtime::{ActorRuntime, InvocationTimings, StateCommitAuthority, socket_event_name},
};

const MAX_ADMITTED_INVOCATIONS_PER_ACTOR: usize = 33;
const HOST_COMMAND_CAPACITY: usize = 256;

pub(crate) struct ActorHost {
    endpoint: HostEndpoint,
    commands: mpsc::Sender<HostCommand>,
    activity: watch::Receiver<usize>,
}

impl ActorHost {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        namespace_id: String,
        executor: Arc<dyn ActorExecutor>,
        commits: Arc<dyn StateCommitAuthority>,
        state: Arc<dyn StateTransport>,
        sockets: Arc<dyn ActorSocketSource>,
        publisher: Arc<dyn ActorSocketPublisher>,
    ) -> Self {
        let (commands, incoming) = mpsc::channel(HOST_COMMAND_CAPACITY);
        let (activity_tx, activity) = watch::channel(0);
        let dispatcher = HostDispatcher::new(
            endpoint.clone(),
            namespace_id,
            executor,
            commits,
            state,
            sockets,
            publisher,
            activity_tx,
        );
        tokio::spawn(dispatcher.run(incoming));
        Self {
            endpoint,
            commands,
            activity,
        }
    }

    pub(crate) fn activity(&self) -> watch::Receiver<usize> {
        self.activity.clone()
    }

    pub(crate) fn id(&self) -> &super::HostId {
        &self.endpoint.id
    }

    pub(crate) async fn invoke_actor(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: String,
    ) -> Result<ActorExecutionResult> {
        self.submit(
            ActorOperation::Method(invocation),
            owner_epoch,
            state_version,
            state_read_url,
        )
        .await
    }

    pub(crate) async fn handle_socket_event(
        &self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: String,
    ) -> Result<ActorExecutionResult> {
        self.submit(
            ActorOperation::Socket(invocation),
            owner_epoch,
            state_version,
            state_read_url,
        )
        .await
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            let (reply, done) = oneshot::channel();
            self.commands
                .send(HostCommand::Drain(reply))
                .await
                .context("actor dispatcher stopped")?;
            done.await
                .context("actor dispatcher stopped while draining")
        })
        .await
        .context("actor invocations did not drain before shutdown")?
    }

    async fn submit(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: String,
    ) -> Result<ActorExecutionResult> {
        let (reply, result) = oneshot::channel();
        let request = ActorRequest {
            operation,
            owner_epoch,
            state_version,
            state_read_url,
            timings: InvocationTimings::new(),
            reply,
        };
        self.commands
            .send(HostCommand::Invoke(Box::new(request)))
            .await
            .context("actor dispatcher stopped")?;
        result.await.unwrap_or_else(|_| {
            Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            })
        })
    }
}

struct HostDispatcher {
    endpoint: HostEndpoint,
    namespace_id: String,
    executor: Arc<dyn ActorExecutor>,
    commits: Arc<dyn StateCommitAuthority>,
    state: Arc<dyn StateTransport>,
    sockets: Arc<dyn ActorSocketSource>,
    publisher: Arc<dyn ActorSocketPublisher>,
    actors: HashMap<ActorStorageKey, ActorMailbox>,
    tasks: JoinSet<()>,
    accepting: watch::Sender<bool>,
    activity: watch::Sender<usize>,
    active: usize,
    drained: Vec<oneshot::Sender<()>>,
}

impl HostDispatcher {
    fn new(
        endpoint: HostEndpoint,
        namespace_id: String,
        executor: Arc<dyn ActorExecutor>,
        commits: Arc<dyn StateCommitAuthority>,
        state: Arc<dyn StateTransport>,
        sockets: Arc<dyn ActorSocketSource>,
        publisher: Arc<dyn ActorSocketPublisher>,
        activity: watch::Sender<usize>,
    ) -> Self {
        Self {
            endpoint,
            namespace_id,
            executor,
            commits,
            state,
            sockets,
            publisher,
            actors: HashMap::new(),
            tasks: JoinSet::new(),
            accepting: watch::channel(true).0,
            activity,
            active: 0,
            drained: Vec::new(),
        }
    }

    async fn run(mut self, mut commands: mpsc::Receiver<HostCommand>) {
        let (completed, mut completions) = mpsc::channel(HOST_COMMAND_CAPACITY);
        loop {
            tokio::select! {
                biased;
                Some(completion) = completions.recv() => self.complete(completion),
                Some(result) = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => self.task_stopped(result),
                command = commands.recv() => match command {
                    Some(HostCommand::Invoke(request)) => self.admit(*request, &completed),
                    Some(HostCommand::Drain(reply)) => {
                        self.accepting.send_replace(false);
                        self.drained.retain(|waiter| !waiter.is_closed());
                        self.drained.push(reply);
                        self.publish_activity();
                    }
                    None => return,
                }
            }
        }
    }

    fn admit(&mut self, request: ActorRequest, completed: &mpsc::Sender<ActorCompletion>) {
        if let Some(result) = self.validate(&request) {
            request.finish(&self.endpoint, result);
            return;
        }
        let object = request.operation.actor().storage_key();
        if !self.actors.contains_key(&object) {
            self.start_actor(object.clone(), completed.clone());
        }
        let mailbox = self.actors.get_mut(&object).expect("actor mailbox created");
        if mailbox.admitted >= MAX_ADMITTED_INVOCATIONS_PER_ACTOR {
            request.finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
            return;
        }
        match mailbox.sender.try_send(request) {
            Ok(()) => {
                mailbox.admitted += 1;
                self.active += 1;
                self.publish_activity();
            }
            Err(error) => {
                error
                    .into_inner()
                    .finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
            }
        }
    }

    fn validate(&self, request: &ActorRequest) -> Option<Result<ActorExecutionResult>> {
        if !*self.accepting.borrow() && !request.operation.is_disconnect() {
            return Some(Ok(ActorExecutionResult::HostUnavailable));
        }
        if let Err(error) = request.operation.validate(&self.namespace_id) {
            return Some(Err(error));
        }
        if !self
            .executor
            .supports(&request.operation.actor().actor_type)
        {
            return Some(Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure {
                    code: "actor_error".into(),
                    message: "actor type is not loaded by this host".into(),
                },
            }));
        }
        None
    }

    fn start_actor(&mut self, object: ActorStorageKey, completed: mpsc::Sender<ActorCompletion>) {
        let runtime = ActorRuntime::new(
            self.endpoint.clone(),
            self.executor.clone(),
            self.commits.clone(),
            self.state.clone(),
            self.sockets.clone(),
            self.publisher.clone(),
        );
        let (sender, requests) = mpsc::channel(MAX_ADMITTED_INVOCATIONS_PER_ACTOR);
        let task = self.tasks.spawn(run_actor(
            object.clone(),
            runtime,
            requests,
            completed,
            self.accepting.subscribe(),
        ));
        self.actors.insert(
            object,
            ActorMailbox {
                sender,
                admitted: 0,
                task_id: task.id(),
            },
        );
    }

    fn complete(&mut self, completion: ActorCompletion) {
        let mailbox = self
            .actors
            .get_mut(&completion.object)
            .expect("completed actor mailbox");
        // A stopped task's remaining admissions may already have been released.
        if mailbox.admitted > 0 {
            mailbox.admitted -= 1;
            self.active -= 1;
        }
        self.publish_activity();
        let _ = completion.reply.send(completion.result);
    }

    fn task_stopped(&mut self, result: Result<(Id, ()), JoinError>) {
        let id = match result {
            Ok((id, ())) => id,
            Err(error) => {
                error!(error = %error, "actor task stopped unexpectedly");
                error.id()
            }
        };
        if let Some(mailbox) = self
            .actors
            .values_mut()
            .find(|mailbox| mailbox.task_id == id)
        {
            self.active -= mailbox.admitted;
            mailbox.admitted = 0;
        }
        self.publish_activity();
    }

    fn publish_activity(&mut self) {
        self.activity.send_replace(self.active);
        if self.active == 0 {
            for waiter in self.drained.drain(..) {
                let _ = waiter.send(());
            }
        }
    }
}

async fn run_actor(
    object: ActorStorageKey,
    mut runtime: ActorRuntime,
    mut requests: mpsc::Receiver<ActorRequest>,
    completed: mpsc::Sender<ActorCompletion>,
    accepting: watch::Receiver<bool>,
) {
    while let Some(request) = requests.recv().await {
        let result = if !*accepting.borrow() && !request.operation.is_disconnect() {
            let result = Ok(ActorExecutionResult::HostUnavailable);
            ActorRuntime::log_invocation(
                runtime.endpoint(),
                &request.operation.invocation(),
                &request.timings,
                &result,
            );
            result
        } else {
            match request.operation {
                ActorOperation::Method(invocation) => {
                    runtime
                        .invoke_actor(
                            invocation,
                            request.owner_epoch,
                            request.state_version,
                            request.state_read_url,
                            request.timings,
                        )
                        .await
                }
                ActorOperation::Socket(invocation) => {
                    runtime
                        .handle_socket_event(
                            invocation,
                            request.owner_epoch,
                            request.state_version,
                            request.state_read_url,
                            request.timings,
                        )
                        .await
                }
            }
        };
        if completed
            .send(ActorCompletion {
                object: object.clone(),
                reply: request.reply,
                result,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

enum HostCommand {
    Invoke(Box<ActorRequest>),
    Drain(oneshot::Sender<()>),
}

struct ActorMailbox {
    sender: mpsc::Sender<ActorRequest>,
    admitted: usize,
    task_id: Id,
}

struct ActorRequest {
    operation: ActorOperation,
    owner_epoch: u64,
    state_version: u64,
    state_read_url: String,
    timings: InvocationTimings,
    reply: oneshot::Sender<Result<ActorExecutionResult>>,
}

struct ActorCompletion {
    object: ActorStorageKey,
    reply: oneshot::Sender<Result<ActorExecutionResult>>,
    result: Result<ActorExecutionResult>,
}

impl ActorRequest {
    fn finish(self, endpoint: &HostEndpoint, result: Result<ActorExecutionResult>) {
        ActorRuntime::log_invocation(
            endpoint,
            &self.operation.invocation(),
            &self.timings,
            &result,
        );
        let _ = self.reply.send(result);
    }
}

enum ActorOperation {
    Method(ActorInvocation),
    Socket(ActorSocketInvocation),
}

impl ActorOperation {
    fn actor(&self) -> &ActorKey {
        match self {
            Self::Method(invocation) => &invocation.actor,
            Self::Socket(invocation) => &invocation.actor,
        }
    }

    fn is_disconnect(&self) -> bool {
        matches!(
            self,
            Self::Socket(ActorSocketInvocation {
                event: ActorSocketEvent::Disconnect { .. },
                ..
            })
        )
    }

    fn validate(&self, namespace: &str) -> Result<()> {
        ensure!(
            self.actor().namespace_id == namespace,
            "actor invocation crossed the host namespace"
        );
        self.invocation().validate()
    }

    fn invocation(&self) -> Cow<'_, ActorInvocation> {
        match self {
            Self::Method(invocation) => Cow::Borrowed(invocation),
            Self::Socket(invocation) => Cow::Owned(ActorInvocation {
                request_id: invocation.request_id.clone(),
                actor: invocation.actor.clone(),
                method: socket_event_name(&invocation.event).into(),
                args: Vec::new(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::actor_runtime::CommittedState;
    use crate::{
        actor::{ActorMethodInvocation, ActorMethodOutcome, ActorSocketEffect, ActorSocketOutcome},
        state_log::StateSnapshot,
        state_transport::StateWrite,
        storage_urls::StateWriteTicket,
    };
    use async_trait::async_trait;
    use serde_json::Value;
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    };

    use serde_json::json;

    use super::*;
    use crate::actor::ActorKey;

    struct EmptySocketSource;

    #[async_trait]
    impl ActorSocketSource for EmptySocketSource {
        async fn connections(
            &self,
            _: &ActorKey,
        ) -> Result<Vec<crate::actor::ActorSocketConnection>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ActorSocketPublisher for EmptySocketSource {
        async fn publish(&self, _: &ActorKey, _: Vec<ActorSocketEffect>) -> Result<()> {
            Ok(())
        }
    }

    struct IncrementingExecutor {
        invocations: AtomicU64,
    }

    struct UnavailableSocketSource;

    #[async_trait]
    impl ActorSocketSource for UnavailableSocketSource {
        async fn connections(
            &self,
            _: &ActorKey,
        ) -> Result<Vec<crate::actor::ActorSocketConnection>> {
            anyhow::bail!("gateway unavailable")
        }
    }

    #[tokio::test]
    async fn connection_lookup_failure_does_not_execute_or_commit_the_method() -> Result<()> {
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let state = Arc::new(FakeStateTransport::default());
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            executor.clone(),
            Arc::new(FakeAuthority::default()),
            state.clone(),
            Arc::new(UnavailableSocketSource),
            Arc::new(EmptySocketSource),
        );
        assert!(
            matches!(invoke(&host, "request-1").await?, ActorExecutionResult::Failed { failure } if failure.code == "socket_gateway_unavailable")
        );
        assert_eq!(executor.invocations.load(Ordering::Relaxed), 0);
        assert!(state.writes.lock().unwrap().is_empty());
        Ok(())
    }

    struct ExhaustedExecutor;

    struct InvalidEffectsExecutor;

    struct ControlledExecutor {
        started: tokio::sync::mpsc::UnboundedSender<String>,
        release: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait]
    impl ActorExecutor for ControlledExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }

        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            self.started.send(invocation.request_id.clone())?;
            if invocation.request_id == "panic" {
                panic!("actor executor panicked");
            }
            if invocation.request_id == "first" {
                self.release.acquire().await?.forget();
            }
            let count = state.and_then(|value| value["count"].as_u64()).unwrap_or(0) + 1;
            Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                state: json!({"count": count}),
                effects: Vec::new(),
            })
        }

        async fn handle_socket(
            &self,
            invocation: ActorSocketInvocation,
            state: Option<&Value>,
        ) -> Result<ActorSocketOutcome> {
            let result = self
                .invoke(
                    ActorMethodInvocation {
                        request_id: invocation.request_id,
                        actor: invocation.actor,
                        method: "onMessage".into(),
                        args: Vec::new(),
                        connections: invocation.connections,
                    },
                    state,
                )
                .await?;
            match result {
                ActorMethodOutcome::Completed { state, effects, .. } => {
                    Ok(ActorSocketOutcome::Handled { state, effects })
                }
                ActorMethodOutcome::Failed(failure) => Ok(ActorSocketOutcome::Failed(failure)),
            }
        }
    }

    fn controlled_host() -> (
        Arc<ActorHost>,
        tokio::sync::mpsc::UnboundedReceiver<String>,
        Arc<tokio::sync::Semaphore>,
    ) {
        let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(ControlledExecutor {
                started,
                release: release.clone(),
            }),
            Arc::new(FakeAuthority::default()),
            Arc::new(FakeStateTransport::default()),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );
        (Arc::new(host), receiver, release)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_actor_task_releases_admission_and_does_not_restart_unknown_state()
    -> Result<()> {
        for _ in 0..64 {
            let (host, mut started, release) = controlled_host();
            let mut activity = host.activity();
            let caller = host.clone();
            let first = tokio::spawn(async move { invoke(&caller, "first").await });
            assert_eq!(started.recv().await.as_deref(), Some("first"));
            let caller = host.clone();
            let panicking = tokio::spawn(async move { invoke(&caller, "panic").await });
            tokio::time::timeout(
                Duration::from_secs(2),
                activity.wait_for(|count| *count == 2),
            )
            .await??;
            release.add_permits(1);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), first).await???,
                completed(1)
            );
            let result = tokio::time::timeout(Duration::from_secs(2), panicking).await???;
            assert!(
                matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
            );
            assert_eq!(
                invoke(&host, "after-panic").await?,
                ActorExecutionResult::HostUnavailable
            );
            host.drain(Duration::from_secs(1)).await?;
            assert_eq!(*activity.borrow(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn caller_cancellation_cannot_release_an_actor_during_its_commit() -> Result<()> {
        let (commit_started, mut committing) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let authority = Arc::new(FakeAuthority {
            paused_commit: Some((commit_started, release.clone())),
            ..Default::default()
        });
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let host = Arc::new(ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            executor.clone(),
            authority.clone(),
            Arc::new(FakeStateTransport::default()),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        ));
        let caller = host.clone();
        let first = tokio::spawn(async move { invoke(&caller, "first").await });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), committing.recv()).await?,
            Some(())
        );
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let caller = host.clone();
        let mut second = tokio::spawn(async move { invoke(&caller, "second").await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut second)
                .await
                .is_err()
        );
        assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), second).await???,
            completed(2)
        );
        assert_eq!(*authority.commits.lock().unwrap(), [0, 1]);
        host.drain(Duration::from_secs(1)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_callers_do_not_interrupt_accepted_actor_operations() -> Result<()> {
        for socket in [false, true] {
            let (host, mut started, release) = controlled_host();
            let caller = host.clone();
            let first = tokio::spawn(async move {
                if socket {
                    caller
                        .handle_socket_event(
                            ActorSocketInvocation {
                                request_id: "first".into(),
                                actor: ActorKey {
                                    namespace_id: "project-1".into(),
                                    actor_type: "Counter".into(),
                                    actor_id: "counter-1".into(),
                                },
                                event: crate::actor::ActorSocketEvent::Message {
                                    connection_id: "socket-1".into(),
                                    message: crate::actor::ActorSocketMessage::Text {
                                        data: "increment".into(),
                                    },
                                },
                                connections: Vec::new(),
                            },
                            1,
                            0,
                            String::new(),
                        )
                        .await
                } else {
                    invoke(&caller, "first").await
                }
            });
            assert_eq!(started.recv().await.as_deref(), Some("first"));
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            let caller = host.clone();
            let second = tokio::spawn(async move { invoke(&caller, "second").await });
            assert!(
                tokio::time::timeout(Duration::from_millis(30), started.recv())
                    .await
                    .is_err(),
                "the next call overtook an accepted operation"
            );
            release.add_permits(1);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), second).await???,
                completed(2)
            );
            host.drain(Duration::from_secs(1)).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn actor_admission_is_bounded_without_blocking_other_actors_and_drain_rejects_queued_work()
    -> Result<()> {
        let (host, mut started, release) = controlled_host();
        let mut activity = host.activity();
        let caller = host.clone();
        let first = tokio::spawn(async move { invoke(&caller, "first").await });
        assert_eq!(started.recv().await.as_deref(), Some("first"));
        let mut queued = Vec::new();
        for index in 0..32 {
            let caller = host.clone();
            queued.push(tokio::spawn(async move {
                invoke(&caller, &format!("queued-{index}")).await
            }));
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            activity.wait_for(|count| *count == 33),
        )
        .await??;
        assert_eq!(
            invoke(&host, "overflow").await?,
            ActorExecutionResult::HostUnavailable
        );
        let other = host.invoke_actor(
            ActorInvocation {
                request_id: "other".into(),
                actor: ActorKey {
                    namespace_id: "project-1".into(),
                    actor_type: "Counter".into(),
                    actor_id: "other".into(),
                },
                method: "increment".into(),
                args: Vec::new(),
            },
            1,
            0,
            String::new(),
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), other).await??,
            completed(1)
        );
        assert!(host.drain(Duration::from_millis(30)).await.is_err());
        assert_eq!(
            invoke(&host, "draining").await?,
            ActorExecutionResult::HostUnavailable
        );
        release.add_permits(1);
        assert_eq!(first.await??, completed(1));
        for caller in queued {
            assert_eq!(caller.await??, ActorExecutionResult::HostUnavailable);
        }
        host.drain(Duration::from_secs(1)).await?;
        assert_eq!(*activity.borrow(), 0);
        Ok(())
    }

    #[async_trait]
    impl ActorExecutor for IncrementingExecutor {
        fn supports(&self, actor_type: &str) -> bool {
            actor_type == "Counter"
        }

        async fn invoke(
            &self,
            _invocation: ActorMethodInvocation,
            state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            self.invocations.fetch_add(1, Ordering::Relaxed);
            let count = state
                .and_then(|state| state.get("count"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                state: json!({ "count": count }),
                effects: Vec::new(),
            })
        }

        async fn handle_socket(
            &self,
            _invocation: ActorSocketInvocation,
            state: Option<&Value>,
        ) -> Result<ActorSocketOutcome> {
            let count = state
                .and_then(|state| state.get("count"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            Ok(ActorSocketOutcome::Handled {
                state: json!({ "count": count }),
                effects: vec![ActorSocketEffect::Send {
                    connection_id: "socket-1".into(),
                    message: crate::actor::ActorSocketMessage::Text {
                        data: "ready".into(),
                    },
                }],
            })
        }
    }

    #[async_trait]
    impl ActorExecutor for ExhaustedExecutor {
        fn supports(&self, _actor_type: &str) -> bool {
            true
        }

        async fn invoke(
            &self,
            _invocation: ActorMethodInvocation,
            _state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "resource_exhausted".into(),
                message: "actor session message is too large".into(),
            }))
        }
    }

    #[async_trait]
    impl ActorExecutor for InvalidEffectsExecutor {
        fn supports(&self, _actor_type: &str) -> bool {
            true
        }

        async fn invoke(
            &self,
            _invocation: ActorMethodInvocation,
            _state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            Ok(ActorMethodOutcome::Completed {
                result: Value::Null,
                state: json!({ "count": 1 }),
                effects: vec![ActorSocketEffect::Close {
                    connection_id: "socket-1".into(),
                    code: 1001,
                    reason: String::new(),
                }],
            })
        }
    }

    #[derive(Default)]
    struct FakeAuthority {
        preparations: Mutex<Vec<u64>>,
        commits: Mutex<Vec<u64>>,
        commit_failures: AtomicUsize,
        paused_commit: Option<(mpsc::UnboundedSender<()>, Arc<tokio::sync::Semaphore>)>,
    }

    #[async_trait]
    impl StateCommitAuthority for FakeAuthority {
        async fn prepare_state_write(
            &self,
            _actor: &ActorKey,
            _host_id: &super::super::HostId,
            _owner_epoch: u64,
            expected_version: u64,
        ) -> Result<StateWriteTicket> {
            self.preparations.lock().unwrap().push(expected_version);
            Ok(ticket(expected_version + 1))
        }

        async fn commit_state(
            &self,
            _actor: &ActorKey,
            _host_id: &super::super::HostId,
            _owner_epoch: u64,
            expected_version: u64,
            _state_object: &str,
            request_id: &str,
        ) -> Result<CommittedState> {
            self.commits.lock().unwrap().push(expected_version);
            if request_id == "first"
                && let Some((started, release)) = &self.paused_commit
            {
                started.send(())?;
                release.acquire().await?.forget();
            }
            if self
                .commit_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                anyhow::bail!("commit response was lost");
            }
            Ok(CommittedState {
                state_version: expected_version + 1,
                next_write: Some(ticket(expected_version + 2)),
            })
        }
    }

    #[derive(Default)]
    struct FakeStateTransport {
        writes: Mutex<Vec<Vec<u8>>>,
        reads: AtomicUsize,
        replicated: bool,
    }

    #[async_trait]
    impl StateTransport for FakeStateTransport {
        async fn read(&self, _signed_url: &str) -> Result<bytes::Bytes> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("new actor should not read storage")
        }

        async fn write(&self, _signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
            anyhow::ensure!(
                !self.replicated,
                "replicated writes must use their complete ticket"
            );
            self.writes.lock().unwrap().push(bytes);
            Ok(StateWrite::Written)
        }

        async fn write_ticket(
            &self,
            ticket: &StateWriteTicket,
            bytes: Vec<u8>,
        ) -> Result<StateWrite> {
            if !self.replicated {
                return self.write(&ticket.url, bytes).await;
            }
            self.writes.lock().unwrap().push(bytes);
            Ok(StateWrite::Replicated)
        }
    }

    #[tokio::test]
    async fn replicated_state_still_requires_the_authoritative_commit_before_success() -> Result<()>
    {
        let authority = Arc::new(FakeAuthority::default());
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(IncrementingExecutor {
                invocations: AtomicU64::new(0),
            }),
            authority.clone(),
            Arc::new(FakeStateTransport {
                replicated: true,
                ..Default::default()
            }),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );
        assert_eq!(invoke(&host, "request-1").await?, completed(1));
        assert_eq!(*authority.commits.lock().unwrap(), [0]);
        Ok(())
    }

    #[tokio::test]
    async fn resident_actor_uses_immutable_snapshots_and_replays_the_last_request() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        let state = Arc::new(FakeStateTransport::default());
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            executor.clone(),
            authority.clone(),
            state.clone(),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );

        assert_eq!(invoke(&host, "request-1").await?, completed(1));
        assert_eq!(invoke(&host, "request-2").await?, completed(2));
        assert_eq!(invoke(&host, "request-2").await?, completed(2));

        assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);
        assert_eq!(state.reads.load(Ordering::Relaxed), 0);
        assert_eq!(state.writes.lock().unwrap().len(), 2);
        assert_eq!(*authority.preparations.lock().unwrap(), [0]);
        assert_eq!(*authority.commits.lock().unwrap(), [0, 1]);
        let snapshots = state
            .writes
            .lock()
            .unwrap()
            .iter()
            .map(|bytes| StateSnapshot::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(snapshots[0].state_version, 1);
        assert_eq!(snapshots[1].state_version, 2);
        Ok(())
    }

    #[tokio::test]
    async fn retries_an_ambiguous_commit_without_executing_the_request_twice() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        authority.commit_failures.store(1, Ordering::SeqCst);
        let state = Arc::new(FakeStateTransport::default());
        let executor = Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        });
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            executor.clone(),
            authority.clone(),
            state.clone(),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );

        let first = invoke(&host, "request-1").await?;
        assert!(matches!(
            first,
            ActorExecutionResult::Failed { ref failure } if failure.code == "outcome_unknown"
        ));
        assert_eq!(invoke(&host, "request-1").await?, completed(1));

        assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
        assert_eq!(state.writes.lock().unwrap().len(), 1);
        assert_eq!(*authority.commits.lock().unwrap(), [0, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn preserves_executor_resource_exhaustion() -> Result<()> {
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(ExhaustedExecutor),
            Arc::new(FakeAuthority::default()),
            Arc::new(FakeStateTransport::default()),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );

        assert!(matches!(
            invoke(&host, "request-1").await?,
            ActorExecutionResult::Failed { ref failure } if failure.code == "resource_exhausted"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_socket_effects_do_not_commit_actor_state() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        let state = Arc::new(FakeStateTransport::default());
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(InvalidEffectsExecutor),
            authority.clone(),
            state.clone(),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );

        assert!(matches!(
            invoke(&host, "request-1").await?,
            ActorExecutionResult::Failed { ref failure }
                if failure.code == "actor_error" && failure.message.contains("invalid socket effects")
        ));
        assert!(state.writes.lock().unwrap().is_empty());
        assert!(authority.commits.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn publishes_automatic_state_after_commit_before_returning_to_rpc_callers() -> Result<()>
    {
        struct Emitter;
        #[async_trait]
        impl ActorExecutor for Emitter {
            fn supports(&self, _: &str) -> bool {
                true
            }
            async fn invoke(
                &self,
                _: ActorMethodInvocation,
                state: Option<&Value>,
            ) -> Result<ActorMethodOutcome> {
                let count = state.and_then(|state| state["count"].as_u64()).unwrap_or(0) + 1;
                Ok(ActorMethodOutcome::Completed {
                    result: json!(count),
                    state: json!({"count": count}),
                    effects: serde_json::from_value(
                        json!([{ "type":"state_update", "changes":{"count":count}, "removed":[] }]),
                    )?,
                })
            }
        }
        struct Publisher {
            authority: Arc<FakeAuthority>,
            values: Mutex<Vec<Value>>,
        }
        #[async_trait]
        impl crate::actor::ActorSocketPublisher for Publisher {
            async fn publish(&self, _: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
                let commits = self.authority.commits.lock().unwrap().len();
                let value = serde_json::to_value(effects)?;
                assert_eq!(value[0]["version"], commits as u64);
                self.values.lock().unwrap().push(value);
                Ok(())
            }
        }
        let authority = Arc::new(FakeAuthority::default());
        let publisher = Arc::new(Publisher {
            authority: authority.clone(),
            values: Mutex::new(vec![]),
        });
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(Emitter),
            authority.clone(),
            Arc::new(FakeStateTransport::default()),
            Arc::new(EmptySocketSource),
            publisher.clone(),
        );
        assert_eq!(invoke(&host, "one").await?, completed(1));
        assert_eq!(invoke(&host, "two").await?, completed(2));
        let values = publisher.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][0]["changes"]["count"], 1);
        assert_eq!(values[1][0]["changes"]["count"], 2);
        drop(values);
        authority.commit_failures.store(1, Ordering::SeqCst);
        assert!(matches!(
            invoke(&host, "failed").await?,
            ActorExecutionResult::Failed { .. }
        ));
        assert_eq!(publisher.values.lock().unwrap().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn socket_events_return_effects_only_after_committing_state() -> Result<()> {
        let authority = Arc::new(FakeAuthority::default());
        let state = Arc::new(FakeStateTransport::default());
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        let connection = crate::actor::ActorSocketConnection {
            id: "socket-1".into(),
            metadata: json!({ "userId": "user-1" }),
            tags: Vec::new(),
        };
        let host = ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            "project-1".into(),
            Arc::new(IncrementingExecutor {
                invocations: AtomicU64::new(0),
            }),
            authority.clone(),
            state.clone(),
            Arc::new(EmptySocketSource),
            Arc::new(EmptySocketSource),
        );

        let invocation = |request_id: &str| ActorSocketInvocation {
            request_id: request_id.into(),
            actor: actor.clone(),
            event: crate::actor::ActorSocketEvent::Connect {
                connection: connection.clone(),
            },
            connections: Vec::new(),
        };
        let result = host
            .handle_socket_event(invocation("committed"), 1, 0, String::new())
            .await?;

        assert!(matches!(
            result,
            ActorExecutionResult::Completed { result: Value::Null, ref effects } if effects.len() == 1
        ));
        assert_eq!(state.writes.lock().unwrap().len(), 1);
        assert_eq!(*authority.commits.lock().unwrap(), [0]);

        authority.commit_failures.store(1, Ordering::SeqCst);
        let failed = host
            .handle_socket_event(invocation("failed"), 1, 0, String::new())
            .await?;
        assert!(
            matches!(failed, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
        );

        host.drain(Duration::from_secs(1)).await?;
        assert_eq!(
            host.handle_socket_event(invocation("drained"), 1, 0, String::new())
                .await?,
            ActorExecutionResult::HostUnavailable
        );
        assert_eq!(state.writes.lock().unwrap().len(), 2);
        Ok(())
    }

    async fn invoke(host: &ActorHost, request_id: &str) -> Result<ActorExecutionResult> {
        host.invoke_actor(
            ActorInvocation {
                request_id: request_id.into(),
                actor: ActorKey {
                    namespace_id: "project-1".into(),
                    actor_type: "Counter".into(),
                    actor_id: "counter-1".into(),
                },
                method: "increment".into(),
                args: Vec::new(),
            },
            1,
            0,
            String::new(),
        )
        .await
    }

    fn completed(count: u64) -> ActorExecutionResult {
        ActorExecutionResult::Completed {
            result: json!(count),
            effects: Vec::new(),
        }
    }

    fn ticket(state_version: u64) -> StateWriteTicket {
        StateWriteTicket {
            replication: None,
            state_version,
            object_name: format!("snapshots/{state_version}.json"),
            url: format!("https://state.invalid/{state_version}"),
            expires_at_ms: i64::MAX,
        }
    }
}
