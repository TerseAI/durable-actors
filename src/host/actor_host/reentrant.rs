use std::collections::BTreeMap;

use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};

use super::*;
use crate::actor::{
    ActorInterleavedOutcome, ActorMethodInvocation, ActorMethodOutcome, ActorSocketOutcome,
};

type Execution = Result<std::result::Result<ActorInterleavedOutcome, ActorInvocationFailure>>;

pub(super) async fn run(
    object: ActorStorageKey,
    runtime: ActorRuntime,
    mut requests: mpsc::Receiver<ActorRequest>,
    completed: mpsc::Sender<ActorCompletion>,
    accepting: watch::Receiver<bool>,
) {
    let mut mailbox = Mailbox {
        object,
        runtime,
        completed,
        running: FuturesUnordered::new(),
        ready: BTreeMap::new(),
        next_sequence: 1,
    };
    let mut open = true;
    loop {
        tokio::select! {
            biased;
            Some((request, result)) = mailbox.running.next(), if !mailbox.running.is_empty() => {
                if !mailbox.executed(request, result).await { return; }
            }
            request = requests.recv(), if open => match request {
                Some(request) => {
                    let accepting = *accepting.borrow();
                    if !mailbox.admit(request, accepting).await { return; }
                }
                None => open = false,
            },
            else => return,
        }
    }
}

struct Mailbox {
    object: ActorStorageKey,
    runtime: ActorRuntime,
    completed: mpsc::Sender<ActorCompletion>,
    running: FuturesUnordered<BoxFuture<'static, (ActorRequest, Execution)>>,
    ready: BTreeMap<u64, (ActorRequest, ActorInterleavedOutcome)>,
    next_sequence: u64,
}

impl Mailbox {
    async fn admit(&mut self, mut request: ActorRequest, accepting: bool) -> bool {
        if !accepting && !request.operation.is_disconnect() {
            self.finish(request, Ok(ActorExecutionResult::HostUnavailable))
                .await;
            return true;
        }
        drop(request.waiting.take());
        if let Some(trace) = &mut request.trace {
            trace.admitted();
        }
        if matches!(request.operation, ActorOperation::Activate { .. }) {
            return self.activate(request).await;
        }
        let invocation = request.operation.invocation().into_owned();
        let state = match self
            .runtime
            .prepare_interleaved(&invocation, request.owner_epoch, &mut request.timings)
            .await
        {
            Ok(state) => state,
            Err(error) => return self.stop(request, error).await,
        };
        let executor = self.runtime.executor();
        self.running.push(
            async move {
                let result = execute(&*executor, &request.operation, state).await;
                (request, result)
            }
            .boxed(),
        );
        true
    }

    async fn activate(&mut self, mut request: ActorRequest) -> bool {
        let ActorOperation::Activate { actor, reply } = request.operation else {
            unreachable!()
        };
        let result = self.runtime.activate_actor(&actor).await;
        let valid = result.is_ok();
        let _ = reply.send(result);
        request.operation = ActorOperation::Method(ActorInvocation {
            actor,
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        });
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

    async fn executed(&mut self, request: ActorRequest, result: Execution) -> bool {
        match result {
            Ok(Ok(outcome)) => {
                if outcome.sequence < self.next_sequence
                    || self.ready.contains_key(&outcome.sequence)
                {
                    return self
                        .stop(request, anyhow::anyhow!("actor snapshot sequence repeated"))
                        .await;
                }
                self.ready.insert(outcome.sequence, (request, outcome));
                self.commit_ready().await
            }
            Ok(Err(failure))
                if matches!(
                    failure.code.as_str(),
                    "actor_method_failed"
                        | "actor_socket_failed"
                        | "method_not_found"
                        | "method_not_callable"
                ) =>
            {
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
                true
            }
            Ok(Err(failure)) => {
                self.stop(
                    request,
                    anyhow::anyhow!("{}: {}", failure.code, failure.message),
                )
                .await
            }
            Err(error) => self.stop(request, error).await,
        }
    }

    async fn commit_ready(&mut self) -> bool {
        while let Some((mut request, outcome)) = self.ready.remove(&self.next_sequence) {
            let invocation = request.operation.invocation().into_owned();
            let result = self
                .runtime
                .commit_interleaved(
                    &invocation,
                    request.owner_epoch,
                    outcome,
                    &mut request.timings,
                )
                .await;
            match result {
                Ok(result @ ActorExecutionResult::Completed { .. }) => {
                    self.next_sequence += 1;
                    self.finish(request, Ok(result)).await;
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

    async fn stop(&mut self, request: ActorRequest, error: anyhow::Error) -> bool {
        tracing::warn!(%error, actor = %self.object, "reentrant activation stopped; publication outcome may be unknown");
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
        ActorRuntime::log_invocation(
            self.runtime.endpoint(),
            &request.operation.invocation(),
            &request.timings,
            &result,
        );
        if let Some(trace) = &mut request.trace {
            trace.complete(&result);
        }
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
    state: Option<Arc<serde_json::Value>>,
) -> Execution {
    match operation {
        ActorOperation::Method(invocation) => match executor
            .invoke_shared(
                ActorMethodInvocation {
                    request_id: invocation.request_id.clone(),
                    actor: invocation.actor.clone(),
                    method: invocation.method.clone(),
                    args: invocation.args.clone(),
                },
                state,
            )
            .await
        {
            Ok(ActorMethodOutcome::Interleaved(outcome)) => Ok(Ok(outcome)),
            Ok(ActorMethodOutcome::Failed(failure)) => Ok(Err(failure)),
            Ok(_) => Err(anyhow::anyhow!(
                "reentrant actor returned an unordered snapshot"
            )),
            Err(error) => Err(error),
        },
        ActorOperation::Socket(invocation) => match executor
            .handle_socket_shared(invocation.clone(), state)
            .await
        {
            Ok(ActorSocketOutcome::Interleaved(outcome)) => Ok(Ok(outcome)),
            Ok(ActorSocketOutcome::Failed(failure)) => Ok(Err(failure)),
            Ok(_) => Err(anyhow::anyhow!(
                "reentrant actor returned an unordered snapshot"
            )),
            Err(error) => Err(error),
        },
        ActorOperation::Activate { .. } => unreachable!(),
    }
}
