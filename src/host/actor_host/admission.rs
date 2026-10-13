use std::collections::{BTreeMap, HashMap};

use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};

use super::*;
use crate::actor::{
    ActorDatabase, ActorInterleavedOutcome, ActorMethodInvocation, ActorMethodOutcome,
    ActorSocketOutcome, DatabaseCall, DatabaseResult,
};
use crate::host::actor_runtime::{
    CommitOrigin, DatabaseReply, PreparedActivation, PreparedInvocation,
};

type InvocationExecution = Result<ActorMethodOutcome>;
enum Execution {
    Invocation(InvocationExecution),
    Activation {
        activation: ActorActivation,
        reply: oneshot::Sender<Result<ActorActivation>>,
        result: Result<()>,
    },
}

type Completion = (u64, ActorRequest, Execution);

pub(super) async fn run(
    object: ActorStorageKey,
    runtime: ActorRuntime,
    mut requests: mpsc::UnboundedReceiver<ActorRequest>,
    completed: mpsc::Sender<ActorCompletion>,
    accepting: watch::Receiver<bool>,
    mut admission: Option<watch::Receiver<()>>,
) {
    let (database, mut calls) = mpsc::unbounded_channel();
    let mut mailbox = Mailbox {
        object,
        runtime,
        completed,
        database,
        running: FuturesUnordered::new(),
        active: HashMap::new(),
        ready: BTreeMap::new(),
        next_sequence: 1,
        next_invocation: 0,
        blocked_on: None,
        interleaved: false,
        hydrating: false,
    };
    let mut open = true;
    while open || !mailbox.running.is_empty() {
        tokio::select! {
            biased;
            Some((id, request, result)) = mailbox.running.next(), if !mailbox.running.is_empty() => {
                if !mailbox.executed(id, request, result).await { return; }
            }
            Some(call) = calls.recv() => {
                if !mailbox.database(call).await { return; }
            }
            permission = admitted(&mut admission), if mailbox.blocked_on.is_some() && !mailbox.hydrating => {
                if permission.is_err() { return; }
                mailbox.interleaved = true;
                mailbox.blocked_on = None;
            }
            request = requests.recv(), if open && mailbox.blocked_on.is_none() => match request {
                Some(request) => {
                    let accepting = *accepting.borrow();
                    // A completed invocation may have left an unused permission.
                    if let Some(admission) = &mut admission {
                        admission.borrow_and_update();
                    }
                    if !mailbox.admit(request, accepting).await { return; }
                }
                None => open = false,
            },
        }
    }
}

async fn admitted(
    admission: &mut Option<watch::Receiver<()>>,
) -> Result<(), watch::error::RecvError> {
    match admission {
        Some(admission) => admission.changed().await,
        None => std::future::pending().await,
    }
}

struct Mailbox {
    object: ActorStorageKey,
    runtime: ActorRuntime,
    completed: mpsc::Sender<ActorCompletion>,
    database: mpsc::UnboundedSender<DatabaseRequest>,
    running: FuturesUnordered<BoxFuture<'static, Completion>>,
    ready: BTreeMap<u64, ReadyCompletion>,
    active: HashMap<u64, ActiveRequest>,
    next_invocation: u64,
    blocked_on: Option<u64>,
    interleaved: bool,
    hydrating: bool,
    next_sequence: u64,
}

struct ActiveRequest {
    invocation: ActorInvocation,
    owner_epoch: u64,
    origin: CommitOrigin,
    committed: Option<u64>,
}

struct ReadyCompletion {
    id: u64,
    request: ActorRequest,
    outcome: ActorInterleavedOutcome,
    committed: Option<u64>,
}

struct DatabaseRequest {
    invocation: u64,
    call: DatabaseCall,
    reply: oneshot::Sender<Result<DatabaseResult>>,
}

struct MailboxDatabase {
    invocation: u64,
    sender: mpsc::UnboundedSender<DatabaseRequest>,
}

#[async_trait::async_trait]
impl ActorDatabase for MailboxDatabase {
    async fn call(&self, call: DatabaseCall) -> Result<DatabaseResult> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(DatabaseRequest {
                invocation: self.invocation,
                call,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("actor mailbox stopped"))?;
        result
            .await
            .context("actor mailbox stopped before the database call completed")?
    }
}

impl Mailbox {
    async fn admit(&mut self, mut request: ActorRequest, accepting: bool) -> bool {
        if !accepting && !request.operation.is_disconnect() {
            self.finish(request, Ok(ActorExecutionResult::HostUnavailable))
                .await;
            return true;
        }
        drop(request.waiting.take());
        request.tracker.admitted();
        if matches!(request.operation, ActorOperation::Activate { .. }) {
            return self.activate(request).await;
        }
        let invocation = request.operation.invocation().into_owned();
        match self
            .runtime
            .prepare_invocation(
                &invocation,
                request.owner_epoch,
                matches!(request.operation, ActorOperation::Method(_)),
                &mut request.tracker,
            )
            .await
        {
            Ok(PreparedInvocation::Execute) => {}
            Ok(PreparedInvocation::Completed(result)) => {
                self.finish(request, Ok(result)).await;
                return true;
            }
            Err(error) => return self.stop(request, error).await,
        };
        let origin = request.operation.commit_origin();
        let (id, database) = self.track(invocation, request.owner_epoch, origin);
        let executor = self.runtime.executor();
        self.running.push(
            async move {
                let result =
                    Execution::Invocation(execute(&*executor, &request.operation, database).await);
                (id, request, result)
            }
            .boxed(),
        );
        true
    }

    fn track(
        &mut self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        origin: CommitOrigin,
    ) -> (u64, Arc<dyn ActorDatabase>) {
        self.next_invocation += 1;
        let id = self.next_invocation;
        self.blocked_on = Some(id);
        self.active.insert(
            id,
            ActiveRequest {
                invocation,
                owner_epoch,
                origin,
                committed: None,
            },
        );
        let database = Arc::new(MailboxDatabase {
            invocation: id,
            sender: self.database.clone(),
        });
        (id, database)
    }

    async fn database(&mut self, request: DatabaseRequest) -> bool {
        let Mailbox {
            active, runtime, ..
        } = self;
        let Some(entry) = active.get_mut(&request.invocation) else {
            let _ = request.reply.send(Err(anyhow::anyhow!(
                "database call has no active invocation"
            )));
            return true;
        };
        let reply = runtime
            .database_call(
                &entry.invocation,
                entry.owner_epoch,
                entry.origin.clone(),
                request.call,
            )
            .await;
        match reply {
            Ok(DatabaseReply::Executed { result, committed }) => {
                entry.committed = committed.or(entry.committed);
                let _ = request.reply.send(Ok(result));
                true
            }
            Ok(DatabaseReply::Rejected(error)) => {
                let _ = request.reply.send(Err(error));
                true
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), actor = %self.object, "actor database write could not be confirmed");
                let _ = request.reply.send(Err(anyhow::anyhow!(
                    "actor database write could not be confirmed"
                )));
                false
            }
        }
    }

    async fn activate(&mut self, mut request: ActorRequest) -> bool {
        let ActorOperation::Activate { actor, reply } = request.operation else {
            unreachable!()
        };
        request.operation = ActorOperation::Method(ActorInvocation {
            actor: actor.clone(),
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        });
        let prepared = match self.runtime.prepare_activation(&actor).await {
            Ok(prepared) => prepared,
            Err(error) => return self.complete_activation(request, reply, Err(error)).await,
        };
        let activation = match prepared {
            PreparedActivation::Completed(activation) => {
                return self
                    .complete_activation(request, reply, Ok(activation))
                    .await;
            }
            PreparedActivation::Hydrate { activation } => activation,
        };

        let invocation = request.operation.invocation().into_owned();
        let request_id = invocation.request_id.clone();
        let (id, database) =
            self.track(invocation, activation.owner_epoch, CommitOrigin::default());
        self.hydrating = true;
        let executor = self.runtime.executor();
        self.running.push(
            async move {
                let result = executor.hydrate(actor, request_id, database).await;
                (
                    id,
                    request,
                    Execution::Activation {
                        activation,
                        reply,
                        result,
                    },
                )
            }
            .boxed(),
        );
        true
    }

    async fn executed(&mut self, id: u64, mut request: ActorRequest, result: Execution) -> bool {
        let committed = self.active.remove(&id).and_then(|active| active.committed);
        let result = match result {
            Execution::Activation {
                activation,
                reply,
                result,
            } => {
                self.hydrating = false;
                let result = self.runtime.finish_activation(activation, result);
                let valid = self.complete_activation(request, reply, result).await;
                self.release(id);
                return valid;
            }
            Execution::Invocation(result) => result,
        };
        if let Ok(ActorMethodOutcome::Interleaved(outcome)) = result {
            self.interleaved = true;
            if outcome.sequence < self.next_sequence || self.ready.contains_key(&outcome.sequence) {
                return self
                    .stop(request, anyhow::anyhow!("actor snapshot sequence repeated"))
                    .await;
            }
            self.ready.insert(
                outcome.sequence,
                ReadyCompletion {
                    id,
                    request,
                    outcome,
                    committed,
                },
            );
            return self.commit_ready().await;
        }
        if self.interleaved {
            match &result {
                Ok(ActorMethodOutcome::Failed(failure))
                    if matches!(
                        failure.code.as_str(),
                        "actor_method_failed"
                            | "actor_socket_failed"
                            | "method_not_found"
                            | "method_not_callable"
                    ) => {}
                _ => {
                    return self
                        .stop(
                            request,
                            anyhow::anyhow!("interleaved actor execution failed: {result:?}"),
                        )
                        .await;
                }
            }
            let Ok(ActorMethodOutcome::Failed(failure)) = result else {
                unreachable!()
            };
            self.finish(
                request,
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure {
                        code: "actor_error".into(),
                        message: failure.message,
                    },
                }),
            )
            .await;
        } else {
            let invocation = request.operation.invocation().into_owned();
            let result = self
                .runtime
                .finish_serial(
                    &invocation,
                    request.owner_epoch,
                    result,
                    committed,
                    request.operation.commit_origin(),
                    &mut request.tracker,
                )
                .await;
            self.finish(request, result).await;
        }
        self.release(id);
        true
    }

    async fn complete_activation(
        &mut self,
        request: ActorRequest,
        reply: oneshot::Sender<Result<ActorActivation>>,
        result: Result<ActorActivation>,
    ) -> bool {
        let valid = result.is_ok();
        let _ = reply.send(result);
        self.finish(
            request,
            Ok(ActorExecutionResult::Completed {
                result: serde_json::Value::Null,
                effects: vec![],
            }),
        )
        .await;
        valid
    }

    async fn commit_ready(&mut self) -> bool {
        while let Some(ready) = self.ready.remove(&self.next_sequence) {
            let ReadyCompletion {
                id,
                mut request,
                outcome,
                committed,
            } = ready;
            let invocation = request.operation.invocation().into_owned();
            let result = self
                .runtime
                .commit_interleaved(
                    &invocation,
                    request.owner_epoch,
                    outcome,
                    committed,
                    request.operation.commit_origin(),
                    &mut request.tracker,
                )
                .await;
            match result {
                Ok(result @ ActorExecutionResult::Completed { .. }) => {
                    self.next_sequence += 1;
                    self.finish(request, Ok(result)).await;
                    self.release(id);
                }
                Ok(_) => {
                    return self
                        .stop(request, anyhow::anyhow!("actor state publication failed"))
                        .await;
                }
                Err(error) => return self.stop(request, error).await,
            }
        }
        true
    }

    fn release(&mut self, id: u64) {
        if self.blocked_on == Some(id) {
            self.blocked_on = None;
        }
    }

    async fn stop(&mut self, request: ActorRequest, error: anyhow::Error) -> bool {
        tracing::warn!(%error, actor = %self.object, "actor admission stopped; publication outcome may be unknown");
        self.runtime.evict(request.operation.actor()).await;
        self.finish(
            request,
            Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            }),
        )
        .await;
        false
    }

    async fn finish(&mut self, mut request: ActorRequest, result: Result<ActorExecutionResult>) {
        request.tracker.state_version(self.runtime.state_version());
        request.tracker.complete(&result);
        ActorRuntime::log_invocation(
            self.runtime.endpoint(),
            &request.operation.invocation(),
            request.tracker.completion(),
            &result,
        );
        let _ = self
            .completed
            .send(ActorCompletion {
                object: self.object.clone(),
                reply: request.reply,
                result,
            })
            .await;
    }
}

async fn execute(
    executor: &dyn ActorExecutor,
    operation: &ActorOperation,
    database: Arc<dyn ActorDatabase>,
) -> InvocationExecution {
    match operation {
        ActorOperation::Method(invocation) => {
            executor
                .invoke(
                    ActorMethodInvocation {
                        request_id: invocation.request_id.clone(),
                        actor: invocation.actor.clone(),
                        method: invocation.method.clone(),
                        args: invocation.args.clone(),
                    },
                    database,
                )
                .await
        }
        ActorOperation::Socket(invocation) => Ok(
            match executor.handle_socket(invocation.clone(), database).await? {
                ActorSocketOutcome::Interleaved(outcome) => {
                    ActorMethodOutcome::Interleaved(outcome)
                }
                ActorSocketOutcome::Handled { effects } => ActorMethodOutcome::Completed {
                    result: serde_json::Value::Null,
                    effects,
                },
                ActorSocketOutcome::Failed(failure) => ActorMethodOutcome::Failed(failure),
            },
        ),
        ActorOperation::Activate { .. } => unreachable!(),
    }
}
