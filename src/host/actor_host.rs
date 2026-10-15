use crate::request_traces::{RequestKind, TraceRecorder, TraceSender};
use crate::request_tracking::HostState;
use crate::request_tracking::{RequestMetadata, RequestObserver, RequestTracker};
use std::{
    borrow::Cow,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::error;

use crate::actor::{
    ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure, ActorKey,
    ActorSocketEvent, ActorSocketInvocation, ActorSocketPublisher,
};

use super::{
    HostEndpoint,
    actor_database::{ActorActivation, HostDatabase},
    queues::{ActorQueues, WaitingRequest},
};

mod admission;
mod execution;
use execution::{log_invocation, socket_event_name};

pub(crate) struct ActorHost {
    traces: Option<TraceSender>,
    queues: ActorQueues,
    endpoint: HostEndpoint,
    actor: ActorKey,
    executor: Arc<dyn ActorExecutor>,
    mailbox: mpsc::UnboundedSender<ActorRequest>,
    activity: watch::Sender<ActorActivity>,
    accepting: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
}

#[derive(Clone, Copy)]
pub(super) struct ActorActivity {
    pub active: usize,
    pub last_active: tokio::time::Instant,
}

impl Default for ActorActivity {
    fn default() -> Self {
        Self {
            active: 0,
            last_active: tokio::time::Instant::now(),
        }
    }
}

impl ActorHost {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        actor: ActorKey,
        executor: Arc<dyn ActorExecutor>,
        database: Arc<HostDatabase>,
        publisher: Arc<dyn ActorSocketPublisher>,
    ) -> Self {
        let activity = watch::channel(ActorActivity::default()).0;
        let accepting = watch::channel(true).0;
        let (stopped_tx, stopped) = watch::channel(false);
        let (mailbox, requests) = mpsc::unbounded_channel();
        spawn_mailbox(
            admission::run(
                actor.clone(),
                endpoint.clone(),
                executor.clone(),
                database,
                publisher,
                requests,
                accepting.subscribe(),
            ),
            stopped_tx,
        );
        Self {
            traces: None,
            queues: ActorQueues::new(),
            endpoint,
            actor,
            executor,
            mailbox,
            activity,
            accepting,
            stopped,
        }
    }

    pub(crate) fn queues(&self) -> ActorQueues {
        self.queues.clone()
    }

    pub(super) fn activity(&self) -> watch::Receiver<ActorActivity> {
        self.activity.subscribe()
    }

    pub(crate) fn stopped(&self) -> watch::Receiver<bool> {
        self.stopped.clone()
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
        host_state: HostState,
        routing_ms: f64,
    ) -> Result<ActorExecutionResult> {
        self.submit_traced(
            ActorOperation::Socket(invocation),
            owner_epoch,
            started,
            host_state,
            routing_ms,
        )
        .await
        .0
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
        host_state: HostState,
        routing_ms: f64,
    ) -> (Result<ActorExecutionResult>, Option<RequestMetadata>) {
        self.submit_traced(
            ActorOperation::Method(invocation),
            owner_epoch,
            Instant::now(),
            host_state,
            routing_ms,
        )
        .await
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
        self.accepting.send_replace(false);
        let mut activity = self.activity.subscribe();
        tokio::time::timeout(timeout, activity.wait_for(|activity| activity.active == 0))
            .await
            .context("actor invocations did not drain before shutdown")??;
        Ok(())
    }

    async fn submit(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
    ) -> Result<ActorExecutionResult> {
        self.submit_traced(operation, owner_epoch, Instant::now(), HostState::Warm, 0.0)
            .await
            .0
    }

    async fn submit_traced(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
        started: Instant,
        host_state: HostState,
        routing_ms: f64,
    ) -> (Result<ActorExecutionResult>, Option<RequestMetadata>) {
        let (reply, result) = oneshot::channel();
        let (tracker, timing) =
            RequestTracker::new(started, host_state, routing_ms, self.observer(&operation));
        self.admit(ActorRequest {
            tracker,
            waiting: None,
            operation,
            owner_epoch,
            reply: ActorReply {
                sender: reply,
                in_flight: InFlight::new(self.activity.clone()),
            },
        });
        let outcome = result.await.unwrap_or_else(|_| {
            Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            })
        });
        let timing = timing.await.ok();
        (outcome, timing)
    }

    fn observer(&self, operation: &ActorOperation) -> Option<Box<dyn RequestObserver>> {
        let sender = self.traces.as_ref()?.clone();
        let (kind, connection, metadata) = match operation {
            ActorOperation::Activate { .. } => return None,
            ActorOperation::Method(_) => (RequestKind::Method, None, None),
            ActorOperation::Socket(invocation) => match &invocation.event {
                ActorSocketEvent::Connect { connection } => (
                    RequestKind::Websocket,
                    Some(connection.id.clone()),
                    Some(connection.metadata.clone()),
                ),
                ActorSocketEvent::Disconnect { connection, .. } => {
                    (RequestKind::Websocket, Some(connection.id.clone()), None)
                }
                ActorSocketEvent::Message { connection_id, .. } => {
                    (RequestKind::Websocket, Some(connection_id.clone()), None)
                }
            },
        };
        Some(Box::new(TraceRecorder::new(
            sender,
            &operation.invocation(),
            kind,
            connection,
            metadata,
        )))
    }

    fn admit(&self, mut request: ActorRequest) {
        if let Some(result) = self.validate(&request) {
            request.finish(&self.endpoint, result);
            return;
        }
        if request.operation.actor() != &self.actor {
            request.finish(&self.endpoint, Ok(ActorExecutionResult::Reroute));
            return;
        }
        if !matches!(&request.operation, ActorOperation::Activate { .. }) {
            request.waiting = Some(self.queues.enqueue(
                request.operation.actor(),
                request.operation.invocation().method.clone(),
            ));
        }
        if let Err(error) = self.mailbox.send(request) {
            error
                .0
                .finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
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
            .supports(&request.operation.actor().actor_name)
        {
            return Some(Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure {
                    code: "actor_error".into(),
                    message: "actor name is not loaded by this host".into(),
                },
            }));
        }
        None
    }
}

fn spawn_mailbox(mailbox: impl Future<Output = ()> + Send + 'static, stopped: watch::Sender<bool>) {
    // Runs in the mailbox's task so `stopped` flips without waiting for another task to be scheduled.
    tokio::spawn(async move {
        if AssertUnwindSafe(mailbox).catch_unwind().await.is_err() {
            error!("actor mailbox panicked");
        }
        stopped.send_replace(true);
    });
}

struct ActorRequest {
    waiting: Option<WaitingRequest>,
    tracker: RequestTracker,
    operation: ActorOperation,
    owner_epoch: u64,
    reply: ActorReply,
}

struct ActorReply {
    sender: oneshot::Sender<Result<ActorExecutionResult>>,
    in_flight: InFlight,
}

impl ActorReply {
    fn send(self, result: Result<ActorExecutionResult>) {
        drop(self.in_flight);
        let _ = self.sender.send(result);
    }
}

struct InFlight(watch::Sender<ActorActivity>);

impl InFlight {
    fn new(activity: watch::Sender<ActorActivity>) -> Self {
        activity.send_modify(|activity| activity.active += 1);
        Self(activity)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.send_modify(|activity| {
            activity.active -= 1;
            activity.last_active = tokio::time::Instant::now();
        });
    }
}

impl ActorRequest {
    fn finish(mut self, endpoint: &HostEndpoint, result: Result<ActorExecutionResult>) {
        self.tracker.complete(&result);
        log_invocation(
            endpoint,
            &self.operation.invocation(),
            self.tracker.completion(),
            &result,
        );
        self.reply.send(result);
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
    fn commit_origin(&self) -> super::actor_database::CommitOrigin {
        match self {
            Self::Socket(invocation) => {
                super::actor_database::CommitOrigin::socket(&invocation.event)
            }
            _ => super::actor_database::CommitOrigin::default(),
        }
    }

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
