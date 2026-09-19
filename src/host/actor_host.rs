use crate::request_traces::{RequestKind, RequestOutcome, RequestSpan, TraceSender};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinError, JoinSet},
};
use tracing::error;

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure, ActorKey,
        ActorSocketEvent, ActorSocketInvocation, ActorSocketPublisher,
    },
    actor_state::ActorStorageKey,
};

use super::{
    HostEndpoint,
    actor_runtime::{
        ActorActivation, ActorRuntime, ActorStorage, InvocationTimings, socket_event_name,
    },
    queues::{ActorQueues, WaitingRequest},
};

const MAX_ADMITTED_INVOCATIONS_PER_ACTOR: usize = 33;
const HOST_COMMAND_CAPACITY: usize = 256;

pub(crate) struct ActorHost {
    traces: Option<TraceSender>,
    queues: ActorQueues,
    endpoint: HostEndpoint,
    commands: mpsc::Sender<HostCommand>,
    activity: watch::Receiver<usize>,
}

impl ActorHost {
    pub(crate) fn new(
        endpoint: HostEndpoint,

        executor: Arc<dyn ActorExecutor>,
        storage: Arc<dyn ActorStorage>,
        state: Arc<dyn crate::state_transport::SnapshotWriter>,
        publisher: Arc<dyn ActorSocketPublisher>,
    ) -> Self {
        let (commands, incoming) = mpsc::channel(HOST_COMMAND_CAPACITY);
        let (activity_tx, activity) = watch::channel(0);
        let queues = ActorQueues::new();
        let dispatcher = HostDispatcher::new(
            endpoint.clone(),
            executor,
            storage,
            state,
            publisher,
            activity_tx,
            queues.clone(),
        );
        tokio::spawn(dispatcher.run(incoming));
        Self {
            traces: None,
            queues,
            endpoint,
            commands,
            activity,
        }
    }

    pub(crate) fn queues(&self) -> ActorQueues {
        self.queues.clone()
    }

    pub(crate) fn activity(&self) -> watch::Receiver<usize> {
        self.activity.clone()
    }

    pub(crate) fn with_traces(mut self, traces: TraceSender) -> Self {
        self.traces = Some(traces);
        self
    }

    pub(crate) async fn handle_socket_event_since(
        &self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        started: Instant,
    ) -> Result<ActorExecutionResult> {
        self.submit_since(ActorOperation::Socket(invocation), owner_epoch, started)
            .await
    }

    pub(crate) fn discard_socket_event(
        &self,
        invocation: ActorSocketInvocation,
        started: Instant,
        outcome: RequestOutcome,
    ) {
        if let Some(mut span) = self.trace(&ActorOperation::Socket(invocation), started) {
            span.finish(outcome);
        }
    }

    pub(crate) fn id(&self) -> &super::HostId {
        &self.endpoint.id
    }

    pub(crate) async fn activate_actor(&self, actor: ActorKey) -> Result<ActorActivation> {
        let (reply, result) = oneshot::channel();
        self.submit(ActorOperation::Activate { actor, reply }, 0)
            .await?;
        result.await.context("actor activation was not completed")?
    }

    pub(crate) async fn invoke_actor(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
    ) -> Result<ActorExecutionResult> {
        self.submit(ActorOperation::Method(invocation), owner_epoch)
            .await
    }

    pub(crate) async fn handle_socket_event(
        &self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
    ) -> Result<ActorExecutionResult> {
        self.submit(ActorOperation::Socket(invocation), owner_epoch)
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
    ) -> Result<ActorExecutionResult> {
        self.submit_since(operation, owner_epoch, Instant::now())
            .await
    }

    async fn submit_since(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
        started: Instant,
    ) -> Result<ActorExecutionResult> {
        let (reply, result) = oneshot::channel();
        let request = ActorRequest {
            trace: self.trace(&operation, started),
            waiting: None,
            operation,
            owner_epoch,

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

    fn trace(&self, operation: &ActorOperation, started: Instant) -> Option<RequestSpan> {
        let sender = self.traces.as_ref()?.clone();
        let (kind, connection) = match operation {
            ActorOperation::Activate { .. } => return None,
            ActorOperation::Method(_) => (RequestKind::Method, None),
            ActorOperation::Socket(invocation) => (
                RequestKind::Websocket,
                Some(match &invocation.event {
                    ActorSocketEvent::Connect { connection }
                    | ActorSocketEvent::Disconnect { connection, .. } => connection.id.clone(),
                    ActorSocketEvent::Message { connection_id, .. } => connection_id.clone(),
                }),
            ),
        };
        Some(RequestSpan::new(
            sender,
            &operation.invocation(),
            kind,
            connection,
            started,
        ))
    }
}

struct HostDispatcher {
    endpoint: HostEndpoint,

    executor: Arc<dyn ActorExecutor>,
    storage: Arc<dyn ActorStorage>,
    state: Arc<dyn crate::state_transport::SnapshotWriter>,
    publisher: Arc<dyn ActorSocketPublisher>,
    actors: HashMap<ActorStorageKey, ActorMailbox>,
    tasks: JoinSet<()>,
    accepting: watch::Sender<bool>,
    activity: watch::Sender<usize>,
    active: usize,
    queues: ActorQueues,
    drained: Vec<oneshot::Sender<()>>,
}

impl HostDispatcher {
    fn new(
        endpoint: HostEndpoint,

        executor: Arc<dyn ActorExecutor>,
        storage: Arc<dyn ActorStorage>,
        state: Arc<dyn crate::state_transport::SnapshotWriter>,
        publisher: Arc<dyn ActorSocketPublisher>,
        activity: watch::Sender<usize>,
        queues: ActorQueues,
    ) -> Self {
        Self {
            endpoint,
            executor,
            storage,
            state,
            publisher,
            actors: HashMap::new(),
            tasks: JoinSet::new(),
            accepting: watch::channel(true).0,
            activity,
            active: 0,
            queues,
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

    fn admit(&mut self, mut request: ActorRequest, completed: &mpsc::Sender<ActorCompletion>) {
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
        if !matches!(&request.operation, ActorOperation::Activate { .. }) {
            request.waiting = Some(self.queues.enqueue(
                request.operation.actor(),
                request.operation.invocation().method.clone(),
            ));
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
        if let Err(error) = request.operation.validate() {
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
            self.storage.clone(),
            self.state.clone(),
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
    while let Some(mut request) = requests.recv().await {
        drop(request.waiting.take());
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
            if let Some(trace) = &mut request.trace {
                trace.admitted();
            }
            match request.operation {
                ActorOperation::Activate { actor, reply } => {
                    let _ = reply.send(runtime.activate_actor(&actor).await);
                    Ok(ActorExecutionResult::Completed {
                        result: serde_json::Value::Null,
                        effects: vec![],
                    })
                }

                ActorOperation::Method(invocation) => {
                    runtime
                        .invoke_actor(invocation, request.owner_epoch, request.timings)
                        .await
                }
                ActorOperation::Socket(invocation) => {
                    runtime
                        .handle_socket_event(invocation, request.owner_epoch, request.timings)
                        .await
                }
            }
        };
        if let Some(trace) = &mut request.trace {
            trace.complete(&result);
        }
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
    waiting: Option<WaitingRequest>,
    trace: Option<RequestSpan>,
    operation: ActorOperation,
    owner_epoch: u64,

    timings: InvocationTimings,
    reply: oneshot::Sender<Result<ActorExecutionResult>>,
}

struct ActorCompletion {
    object: ActorStorageKey,
    reply: oneshot::Sender<Result<ActorExecutionResult>>,
    result: Result<ActorExecutionResult>,
}

impl ActorRequest {
    fn finish(mut self, endpoint: &HostEndpoint, result: Result<ActorExecutionResult>) {
        if let Some(trace) = &mut self.trace {
            trace.complete(&result);
        }
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
    Activate {
        actor: ActorKey,
        reply: oneshot::Sender<Result<ActorActivation>>,
    },
    Method(ActorInvocation),
    Socket(ActorSocketInvocation),
}

impl ActorOperation {
    fn actor(&self) -> &ActorKey {
        match self {
            Self::Activate { actor, .. } => actor,
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

    fn validate(&self) -> Result<()> {
        self.invocation().validate()
    }

    fn invocation(&self) -> Cow<'_, ActorInvocation> {
        match self {
            Self::Activate { actor, .. } => Cow::Owned(ActorInvocation {
                request_id: "activate".into(),
                actor: actor.clone(),
                method: "activate".into(),
                args: vec![],
            }),
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
#[path = "../../tests/unit/host/actor_host.rs"]
mod tests;
