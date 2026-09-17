use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde_json::Value;
use tracing::{info, warn};

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure,
        ActorMethodEviction, ActorMethodInvocation, ActorMethodOutcome, ActorSocketEffect,
        ActorSocketInvocation, ActorSocketOutcome, ActorSocketPublisher, ActorSocketSource,
        validate_socket_effects,
    },
    control_plane::ControlPlaneClient,
    state_log::StateSnapshot,
    state_transport::{StateTransport, StateWrite},
    storage_urls::StateWriteTicket,
};

use super::HostEndpoint;

const STATE_WRITE_TICKET_SAFETY: Duration = Duration::from_secs(5);

#[async_trait]
pub(crate) trait StateCommitAuthority: Send + Sync {
    async fn load_actor_state(
        &self,
        _actor: &crate::actor::ActorKey,
        _host: &super::HostId,
        _epoch: u64,
    ) -> Result<Option<(u64, String)>> {
        Ok(None)
    }
    fn ensure_authority(&self) -> Result<()> {
        Ok(())
    }
    async fn prepare_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<StateWriteTicket>;

    #[allow(clippy::too_many_arguments)]
    async fn commit_state(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
        state_object: &str,
        request_id: &str,
    ) -> Result<CommittedState>;
}

#[derive(Debug)]
pub(crate) struct CommittedState {
    pub(super) state_version: u64,
    pub(super) next_write: Option<StateWriteTicket>,
}

#[async_trait]
impl StateCommitAuthority for ControlPlaneClient {
    async fn load_actor_state(
        &self,
        actor: &crate::actor::ActorKey,
        host: &super::HostId,
        epoch: u64,
    ) -> Result<Option<(u64, String)>> {
        ControlPlaneClient::load_actor_state(self, actor, host, epoch)
            .await
            .map(Some)
    }
    fn ensure_authority(&self) -> Result<()> {
        self.ensure_live_lease()
    }
    async fn prepare_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<StateWriteTicket> {
        ControlPlaneClient::prepare_state_write(self, actor, host_id, owner_epoch, expected_version)
            .await
    }

    async fn commit_state(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
        state_object: &str,
        request_id: &str,
    ) -> Result<CommittedState> {
        let (state_version, next_write) = ControlPlaneClient::commit_state(
            self,
            actor,
            host_id,
            owner_epoch,
            expected_version,
            state_object,
            request_id,
        )
        .await?;
        Ok(CommittedState {
            state_version,
            next_write,
        })
    }
}

pub(super) struct ActorRuntime {
    endpoint: HostEndpoint,
    executor: Arc<dyn ActorExecutor>,
    commits: Arc<dyn StateCommitAuthority>,
    state: Arc<dyn StateTransport>,
    sockets: Arc<dyn ActorSocketSource>,
    publisher: Arc<dyn ActorSocketPublisher>,
    cached_state: Option<CachedActorState>,
}

impl ActorRuntime {
    pub(super) fn new(
        endpoint: HostEndpoint,
        executor: Arc<dyn ActorExecutor>,
        commits: Arc<dyn StateCommitAuthority>,
        state: Arc<dyn StateTransport>,
        sockets: Arc<dyn ActorSocketSource>,
        publisher: Arc<dyn ActorSocketPublisher>,
    ) -> Self {
        Self {
            endpoint,
            executor,
            commits,
            state,
            sockets,
            publisher,
            cached_state: None,
        }
    }

    pub(super) fn endpoint(&self) -> &HostEndpoint {
        &self.endpoint
    }

    pub(super) async fn invoke_actor(
        &mut self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: String,
        mut timings: InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        self.commits.ensure_authority()?;
        let outcome = self
            .invoke_actor_once(
                &invocation,
                owner_epoch,
                state_version,
                &state_read_url,
                &mut timings,
            )
            .await;
        Self::log_invocation(&self.endpoint, &invocation, &timings, &outcome);
        self.commits.ensure_authority()?;
        outcome
    }

    pub(super) async fn handle_socket_event(
        &mut self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: String,
        mut timings: InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        self.commits.ensure_authority()?;
        let persistence = ActorInvocation {
            request_id: invocation.request_id.clone(),
            actor: invocation.actor.clone(),
            method: socket_event_name(&invocation.event).into(),
            args: Vec::new(),
        };
        let outcome = self
            .handle_socket_event_once(
                invocation,
                owner_epoch,
                state_version,
                &state_read_url,
                &persistence,
                &mut timings,
            )
            .await;
        Self::log_invocation(&self.endpoint, &persistence, &timings, &outcome);
        self.commits.ensure_authority()?;
        outcome
    }

    async fn handle_socket_event_once(
        &mut self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: &str,
        persistence: &ActorInvocation,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        timings.queue_admitted_at_ms = Some(timings.elapsed_ms());
        let mut cached = self
            .take_or_load_state(
                &invocation.actor,
                owner_epoch,
                state_version,
                state_read_url,
                timings,
            )
            .await?;
        if self
            .finish_pending_commit(persistence, &mut cached)
            .await
            .is_err()
        {
            self.cached_state = Some(cached);
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        timings.pending_commit_resolved_at_ms = Some(timings.elapsed_ms());
        let outcome = self.execute_socket_event(invocation, cached.state()).await;
        timings.actor_execution_completed_at_ms = Some(timings.elapsed_ms());
        let (next_state, effects) = match outcome {
            Ok(outcome) => outcome,
            Err(result) => {
                self.cached_state = Some(cached);
                return Ok(result);
            }
        };
        if cached.state.as_deref() == Some(&next_state) {
            let version = cached.state_version;
            self.cached_state = Some(cached);
            return self
                .complete_with_state(&persistence.actor, version, Value::Null, effects)
                .await;
        }
        let published = self
            .publish_result(
                persistence,
                owner_epoch,
                &mut cached,
                Value::Null,
                next_state,
            )
            .await;
        timings.state_publication_completed_at_ms = Some(timings.elapsed_ms());
        if published.is_err() {
            self.evict(&persistence.actor).await;
        }
        let version = cached.state_version;
        self.cached_state = Some(cached);
        match published {
            Ok(ActorExecutionResult::Completed { .. }) => {
                self.complete_with_state(&persistence.actor, version, Value::Null, effects)
                    .await
            }
            Ok(result) => Ok(result),
            Err(_) => Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            }),
        }
    }

    async fn invoke_actor_once(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: &str,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        timings.queue_admitted_at_ms = Some(timings.elapsed_ms());
        let mut cached = self
            .take_or_load_state(
                &invocation.actor,
                owner_epoch,
                state_version,
                state_read_url,
                timings,
            )
            .await?;
        if let Err(error) = self.finish_pending_commit(invocation, &mut cached).await {
            self.cached_state = Some(cached);
            warn!(
                actor = %invocation.actor.storage_key(),
                error = %format!("{error:#}"),
                "pending actor state commit remains unresolved"
            );
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        timings.pending_commit_resolved_at_ms = Some(timings.elapsed_ms());
        if let Some(result) = cached.replay(&invocation.request_id) {
            self.cached_state = Some(cached);
            return Ok(ActorExecutionResult::Completed {
                result,
                effects: Vec::new(),
            });
        }

        let executed = self.execute_method(invocation, cached.state()).await;
        timings.actor_execution_completed_at_ms = Some(timings.elapsed_ms());
        let (result, next_state, effects) = match executed {
            Ok(outcome) => outcome,
            Err(failure) => {
                self.cached_state = Some(cached);
                return Ok(failure);
            }
        };
        if cached.state.as_deref() == Some(&next_state) {
            let version = cached.state_version;
            self.cached_state = Some(cached);
            return self
                .complete_with_state(&invocation.actor, version, result, effects)
                .await;
        }

        let published = self
            .publish_result(invocation, owner_epoch, &mut cached, result, next_state)
            .await;
        timings.state_publication_completed_at_ms = Some(timings.elapsed_ms());
        if published.is_err() {
            self.evict(&invocation.actor).await;
        }
        let version = cached.state_version;
        self.cached_state = Some(cached);
        match published {
            Ok(ActorExecutionResult::Completed { result, .. }) => {
                self.complete_with_state(&invocation.actor, version, result, effects)
                    .await
            }
            Ok(result) => Ok(result),
            Err(error) => {
                warn!(
                    actor = %invocation.actor.storage_key(),
                    error = %format!("{error:#}"),
                    "actor completed but state publication could not be confirmed"
                );
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                })
            }
        }
    }

    async fn complete_with_state(
        &self,
        actor: &crate::actor::ActorKey,
        state_version: u64,
        result: Value,
        effects: Vec<ActorSocketEffect>,
    ) -> Result<ActorExecutionResult> {
        self.commits.ensure_authority()?;
        let (mut automatic, effects): (Vec<_>, Vec<_>) = effects.into_iter().partition(|effect| {
            matches!(
                effect,
                ActorSocketEffect::StateSnapshot { .. } | ActorSocketEffect::StateUpdate { .. }
            )
        });
        for effect in &mut automatic {
            match effect {
                ActorSocketEffect::StateSnapshot { version, .. }
                | ActorSocketEffect::StateUpdate { version, .. } => *version = Some(state_version),
                _ => unreachable!(),
            }
        }
        if !automatic.is_empty() {
            if let Err(error) = self.publisher.publish(actor, automatic).await {
                warn!(actor = %actor.storage_key(), error = %error, "committed state notification failed");
                return Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                });
            }
        }
        Ok(ActorExecutionResult::Completed { result, effects })
    }

    async fn take_or_load_state(
        &mut self,
        actor: &crate::actor::ActorKey,
        owner_epoch: u64,
        state_version: u64,
        state_read_url: &str,
        timings: &mut InvocationTimings,
    ) -> Result<CachedActorState> {
        let cached = self.cached_state.take();
        timings.state_cache_checked_at_ms = Some(timings.elapsed_ms());
        if let Some(cached) = cached
            && cached.owner_epoch == owner_epoch
        {
            return Ok(cached);
        }
        let refreshed = self
            .commits
            .load_actor_state(actor, &self.endpoint.id, owner_epoch)
            .await?;
        let (state_version, state_read_url) = refreshed
            .as_ref()
            .map(|(version, url)| (*version, url.as_str()))
            .unwrap_or((state_version, state_read_url));
        if state_version == 0 {
            ensure!(
                state_read_url.is_empty(),
                "uninitialized actor has a state URL"
            );
            return Ok(CachedActorState::new(owner_epoch));
        }
        ensure!(
            !state_read_url.is_empty(),
            "initialized actor has no state URL"
        );
        let loaded = self
            .state
            .read(state_read_url)
            .await
            .context("load actor state")?;
        timings.state_downloaded_at_ms = Some(timings.elapsed_ms());
        let cached = CachedActorState::from_loaded(owner_epoch, state_version, &loaded)?;
        timings.state_decoded_at_ms = Some(timings.elapsed_ms());
        Ok(cached)
    }

    async fn execute_method(
        &self,
        invocation: &ActorInvocation,
        state: Option<Arc<Value>>,
    ) -> std::result::Result<(Value, Value, Vec<ActorSocketEffect>), ActorExecutionResult> {
        let connections = self
            .sockets
            .connections(&invocation.actor)
            .await
            .map_err(|error| {
                failed(
                    "socket_gateway_unavailable",
                    format!("load actor connections: {error:#}"),
                )
            })?;
        let outcome = self
            .executor
            .invoke_shared(
                ActorMethodInvocation {
                    request_id: invocation.request_id.clone(),
                    actor: invocation.actor.clone(),
                    method: invocation.method.clone(),
                    args: invocation.args.clone(),
                    connections,
                },
                state,
            )
            .await;
        match outcome {
            Ok(ActorMethodOutcome::Completed {
                result,
                state,
                effects,
            }) => match validate_socket_effects(&effects) {
                Ok(()) => Ok((result, state, effects)),
                Err(error) => {
                    self.evict(&invocation.actor).await;
                    Err(failed(
                        "actor_error",
                        format!("actor returned invalid socket effects: {error:#}"),
                    ))
                }
            },
            Ok(ActorMethodOutcome::Failed(failure)) => {
                self.evict(&invocation.actor).await;
                let code = match failure.code.as_str() {
                    "resource_exhausted" => "resource_exhausted",
                    _ => "actor_error",
                };
                Err(failed(code, failure.message))
            }
            Err(error) => {
                self.evict(&invocation.actor).await;
                Err(failed(
                    "actor_error",
                    format!("actor executor failed: {error:#}"),
                ))
            }
        }
    }

    async fn execute_socket_event(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<Arc<Value>>,
    ) -> std::result::Result<(Value, Vec<ActorSocketEffect>), ActorExecutionResult> {
        let actor = invocation.actor.clone();
        match self.executor.handle_socket_shared(invocation, state).await {
            Ok(ActorSocketOutcome::Handled { state, effects }) => {
                match validate_socket_effects(&effects) {
                    Ok(()) => Ok((state, effects)),
                    Err(error) => {
                        self.evict(&actor).await;
                        Err(failed(
                            "actor_error",
                            format!("actor returned invalid socket effects: {error:#}"),
                        ))
                    }
                }
            }
            Ok(ActorSocketOutcome::Failed(failure)) => {
                let code = match failure.code.as_str() {
                    "resource_exhausted" => "resource_exhausted",
                    _ => "actor_error",
                };
                Err(failed(code, failure.message))
            }
            Err(error) => Err(failed(
                "actor_error",
                format!("actor executor failed: {error:#}"),
            )),
        }
    }

    async fn publish_result(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut CachedActorState,
        result: Value,
        next_state: Value,
    ) -> Result<ActorExecutionResult> {
        let mut timings = StateWriteTimings::new();
        let next_version = cached.state_version.checked_add(1);
        let outcome = self
            .publish_result_once(
                invocation,
                owner_epoch,
                cached,
                result,
                next_state,
                &mut timings,
            )
            .await;
        self.log_state_write(invocation, owner_epoch, next_version, &timings, &outcome);
        outcome
    }

    async fn publish_result_once(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut CachedActorState,
        result: Value,
        next_state: Value,
        timings: &mut StateWriteTimings,
    ) -> Result<ActorExecutionResult> {
        let next_version = cached
            .state_version
            .checked_add(1)
            .context("actor state version overflow")?;
        let ticket = match cached.next_write.take() {
            Some(ticket)
                if ticket.state_version == next_version
                    && ticket.expires_at_ms
                        > unix_millis()?.saturating_add(i64::try_from(
                            STATE_WRITE_TICKET_SAFETY.as_millis(),
                        )?) =>
            {
                ticket
            }
            _ => {
                self.commits
                    .prepare_state_write(
                        &invocation.actor,
                        &self.endpoint.id,
                        owner_epoch,
                        cached.state_version,
                    )
                    .await?
            }
        };
        ensure!(
            ticket.state_version == next_version,
            "state write ticket has the wrong version"
        );
        ensure!(
            ticket
                .stream
                .as_ref()
                .is_none_or(|stream| stream.owner_epoch == owner_epoch),
            "write capability belongs to another owner epoch"
        );
        timings.write_ticket_ready_at_ms = Some(timings.elapsed_ms());
        let snapshot = StateSnapshot::new(
            next_version,
            owner_epoch,
            invocation.request_id.clone(),
            next_state,
            result.clone(),
        )?;
        timings.snapshot_created_at_ms = Some(timings.elapsed_ms());
        let bytes = snapshot.encode()?;
        timings.snapshot_encoded_at_ms = Some(timings.elapsed_ms());
        cached.pending = Some(PendingStateCommit {
            snapshot,
            ticket,
            durable: false,
        });
        let pending = cached
            .pending
            .as_ref()
            .expect("pending write was installed");
        let write = self.state.write_ticket(&pending.ticket, bytes).await?;
        cached
            .pending
            .as_mut()
            .expect("pending write was installed")
            .durable = true;
        timings.snapshot_persisted_at_ms = Some(timings.elapsed_ms());
        timings.durability_proof = Some(if write == StateWrite::Replicated {
            "replicas"
        } else {
            "object_storage"
        });
        if write != StateWrite::Replicated {
            timings.snapshot_uploaded_at_ms = timings.snapshot_persisted_at_ms;
        }
        self.finish_pending_commit(invocation, cached).await?;
        timings.state_finalized_at_ms = Some(timings.elapsed_ms());
        Ok(ActorExecutionResult::Completed {
            result,
            effects: Vec::new(),
        })
    }

    fn log_state_write(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_version: Option<u64>,
        timings: &StateWriteTimings,
        outcome: &Result<ActorExecutionResult>,
    ) {
        match outcome {
            Ok(_) => info!(
                event = "actor_state_write",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                owner_epoch,
                state_version,
                started_at_ms = 0,
                write_ticket_ready_at_ms = timings.write_ticket_ready_at_ms,
                snapshot_created_at_ms = timings.snapshot_created_at_ms,
                snapshot_encoded_at_ms = timings.snapshot_encoded_at_ms,
                snapshot_uploaded_at_ms = timings.snapshot_uploaded_at_ms,
                snapshot_persisted_at_ms = timings.snapshot_persisted_at_ms,
                durability_proof = timings.durability_proof,
                state_finalized_at_ms = timings.state_finalized_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome = "committed",
                "immutable actor state committed"
            ),
            Err(error) => warn!(
                event = "actor_state_write",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                owner_epoch,
                state_version,
                started_at_ms = 0,
                write_ticket_ready_at_ms = timings.write_ticket_ready_at_ms,
                snapshot_created_at_ms = timings.snapshot_created_at_ms,
                snapshot_encoded_at_ms = timings.snapshot_encoded_at_ms,
                snapshot_uploaded_at_ms = timings.snapshot_uploaded_at_ms,
                snapshot_persisted_at_ms = timings.snapshot_persisted_at_ms,
                durability_proof = timings.durability_proof,
                state_finalized_at_ms = timings.state_finalized_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome = "failed",
                error = %format!("{error:#}"),
                "immutable actor state commit failed"
            ),
        }
    }

    async fn finish_pending_commit(
        &self,
        invocation: &ActorInvocation,
        cached: &mut CachedActorState,
    ) -> Result<()> {
        let Some(pending) = &mut cached.pending else {
            return Ok(());
        };
        if !pending.durable {
            if pending.ticket.stream.is_some()
                && pending.ticket.expires_at_ms <= unix_millis()?.saturating_add(5000)
            {
                let renewed = self
                    .commits
                    .prepare_state_write(
                        &invocation.actor,
                        &self.endpoint.id,
                        cached.owner_epoch,
                        cached.state_version,
                    )
                    .await?;
                ensure!(
                    renewed.stream == pending.ticket.stream
                        && renewed.object_name == pending.ticket.object_name,
                    "pending writer has been fenced"
                );
                pending.ticket = renewed;
            }
            self.state
                .write_ticket(&pending.ticket, pending.snapshot.encode()?)
                .await?;
            pending.durable = true;
        }
        let committed = if let Some(stream) = &pending.ticket.stream {
            let mut next_write = pending.ticket.clone();
            next_write.state_version = pending
                .snapshot
                .state_version
                .checked_add(1)
                .context("state version overflow")?;
            next_write.object_name = stream.object(next_write.state_version);
            CommittedState {
                state_version: pending.snapshot.state_version,
                next_write: Some(next_write),
            }
        } else {
            self.commits
                .commit_state(
                    &invocation.actor,
                    &self.endpoint.id,
                    cached.owner_epoch,
                    cached.state_version,
                    &pending.ticket.object_name,
                    &pending.snapshot.request_id,
                )
                .await?
        };
        ensure!(
            committed.state_version == pending.snapshot.state_version,
            "control plane committed the wrong actor state version"
        );
        let pending = cached.pending.take().expect("pending commit checked above");
        cached.state_version = pending.snapshot.state_version;
        cached.state = Some(Arc::new(pending.snapshot.state));
        cached.last_request_id = Some(pending.snapshot.request_id);
        cached.last_result = Some(pending.snapshot.result);
        cached.next_write = committed.next_write;
        Ok(())
    }

    pub(super) fn log_invocation(
        endpoint: &HostEndpoint,
        invocation: &ActorInvocation,
        timings: &InvocationTimings,
        outcome: &Result<ActorExecutionResult>,
    ) {
        match outcome {
            Ok(result) => Self::log_invocation_result(
                endpoint,
                invocation,
                timings,
                actor_execution_outcome(result),
                actor_execution_failure_code(result).unwrap_or(""),
                None,
            ),
            Err(error) => Self::log_invocation_result(
                endpoint,
                invocation,
                timings,
                "host_error",
                "",
                Some(format!("{error:#}")),
            ),
        }
    }

    fn log_invocation_result(
        endpoint: &HostEndpoint,
        invocation: &ActorInvocation,
        timings: &InvocationTimings,
        outcome: &str,
        failure_code: &str,
        error: Option<String>,
    ) {
        info!(
                event = "actor_host_invocation",
                request_id = %invocation.request_id,
                namespace_id = %invocation.actor.namespace_id,
                actor_type = %invocation.actor.actor_type,
                actor_id = %invocation.actor.actor_id,
                method = %invocation.method,
                host_id = %endpoint.id,
                started_at_ms = 0,
                queue_admitted_at_ms = timings.queue_admitted_at_ms,
                state_cache_checked_at_ms = timings.state_cache_checked_at_ms,
                state_downloaded_at_ms = timings.state_downloaded_at_ms,
                state_decoded_at_ms = timings.state_decoded_at_ms,
                pending_commit_resolved_at_ms = timings.pending_commit_resolved_at_ms,
                actor_execution_completed_at_ms = timings.actor_execution_completed_at_ms,
                state_publication_completed_at_ms = timings.state_publication_completed_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome,
                failure_code,
                error,
                "actor host invocation completed"
        );
    }

    async fn evict(&self, actor: &crate::actor::ActorKey) {
        if let Err(error) = self
            .executor
            .evict(ActorMethodEviction {
                actor: actor.clone(),
            })
            .await
        {
            warn!(error = %format!("{error:#}"), "failed to evict actor after invocation failure");
        }
    }
}

pub(super) fn socket_event_name(event: &crate::actor::ActorSocketEvent) -> &'static str {
    match event {
        crate::actor::ActorSocketEvent::Connect { .. } => "onConnect",
        crate::actor::ActorSocketEvent::Message { .. } => "onMessage",
        crate::actor::ActorSocketEvent::Disconnect { .. } => "onDisconnect",
    }
}

struct CachedActorState {
    owner_epoch: u64,
    state_version: u64,
    state: Option<Arc<Value>>,
    last_request_id: Option<String>,
    last_result: Option<Value>,
    next_write: Option<StateWriteTicket>,
    pending: Option<PendingStateCommit>,
}

struct PendingStateCommit {
    snapshot: StateSnapshot,
    ticket: StateWriteTicket,
    durable: bool,
}

impl CachedActorState {
    fn new(owner_epoch: u64) -> Self {
        Self {
            owner_epoch,
            state_version: 0,
            state: None,
            last_request_id: None,
            last_result: None,
            next_write: None,
            pending: None,
        }
    }

    fn from_loaded(owner_epoch: u64, state_version: u64, loaded: &[u8]) -> Result<Self> {
        let snapshot = StateSnapshot::decode(loaded)?;
        ensure!(
            snapshot.state_version == state_version,
            "actor snapshot version does not match its state head"
        );
        ensure!(
            snapshot.owner_epoch <= owner_epoch,
            "actor snapshot belongs to a newer owner epoch"
        );
        Ok(Self {
            owner_epoch,
            state_version,
            state: Some(Arc::new(snapshot.state)),
            last_request_id: Some(snapshot.request_id),
            last_result: Some(snapshot.result),
            next_write: None,
            pending: None,
        })
    }

    fn state(&self) -> Option<Arc<Value>> {
        self.state.clone()
    }

    fn replay(&self, request_id: &str) -> Option<Value> {
        (self.last_request_id.as_deref() == Some(request_id))
            .then(|| self.last_result.clone())
            .flatten()
    }
}

pub(super) struct InvocationTimings {
    started_at: Instant,
    queue_admitted_at_ms: Option<f64>,
    state_cache_checked_at_ms: Option<f64>,
    state_downloaded_at_ms: Option<f64>,
    state_decoded_at_ms: Option<f64>,
    pending_commit_resolved_at_ms: Option<f64>,
    actor_execution_completed_at_ms: Option<f64>,
    state_publication_completed_at_ms: Option<f64>,
}

impl InvocationTimings {
    pub(super) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            queue_admitted_at_ms: None,
            state_cache_checked_at_ms: None,
            state_downloaded_at_ms: None,
            state_decoded_at_ms: None,
            pending_commit_resolved_at_ms: None,
            actor_execution_completed_at_ms: None,
            state_publication_completed_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

struct StateWriteTimings {
    started_at: Instant,
    write_ticket_ready_at_ms: Option<f64>,
    snapshot_created_at_ms: Option<f64>,
    snapshot_encoded_at_ms: Option<f64>,
    snapshot_uploaded_at_ms: Option<f64>,
    snapshot_persisted_at_ms: Option<f64>,
    durability_proof: Option<&'static str>,
    state_finalized_at_ms: Option<f64>,
}

impl StateWriteTimings {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            write_ticket_ready_at_ms: None,
            snapshot_created_at_ms: None,
            snapshot_encoded_at_ms: None,
            snapshot_uploaded_at_ms: None,
            snapshot_persisted_at_ms: None,
            durability_proof: None,
            state_finalized_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

fn actor_execution_outcome(result: &ActorExecutionResult) -> &'static str {
    match result {
        ActorExecutionResult::Completed { .. } => "completed",
        ActorExecutionResult::Failed { .. } => "failed",
        ActorExecutionResult::Reroute => "reroute",
        ActorExecutionResult::HostUnavailable => "host_unavailable",
    }
}

fn actor_execution_failure_code(result: &ActorExecutionResult) -> Option<&str> {
    match result {
        ActorExecutionResult::Failed { failure } => Some(&failure.code),
        _ => None,
    }
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn unix_millis() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis(),
    )
    .context("system clock exceeds supported state-write timestamp range")
}

fn failed(code: impl Into<String>, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}
