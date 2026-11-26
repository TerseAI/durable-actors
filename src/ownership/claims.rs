use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    ActorClaim, Ownership, OwnershipHint, OwnershipStore, SessionCheckpoint, Stored, Version,
};
use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    bucket::RuntimeStorageReader,
    host::HostId,
    host_leases::{HostLease, HostLeaseRequest, max_host_lease_duration_ms},
    placement::{ObjectPlacement, ObjectPlacementStore},
};

const CLAIM_ATTEMPTS: usize = 4;

pub(crate) enum Claim {
    Won(ActorClaim),
    /// Another host's lease is still live.
    Held,
}

/// The control plane's side of ownership: it claims each activation before assigning a host,
/// and its hosts renew and release their leases through it.
pub struct Ownerships {
    pub(super) store: Arc<dyn OwnershipStore>,
    pub(super) storage: Arc<RuntimeStorageReader>,
}

impl Ownerships {
    pub(crate) fn new(store: Arc<dyn OwnershipStore>, storage: Arc<RuntimeStorageReader>) -> Self {
        Self { store, storage }
    }

    /// Claims `actor` for a host the caller is about to assign, unless another host's lease is
    /// live. A session its host never sealed is recovered and sealed first.
    pub(crate) async fn claim(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        region: &str,
        known: Option<OwnershipHint>,
    ) -> Result<Claim> {
        actor.validate()?;
        crate::placement::validate_region(region)?;
        let started = Instant::now();
        // An unsealed hint can predate a renewal, and sealing it would cut off a live writer.
        let mut known = known
            .filter(|hint| hint.record.actor == *actor && hint.record.sealed)
            .map(|hint| Stored {
                version: hint.version,
                record: hint.record,
            });
        for attempt in 1..=CLAIM_ATTEMPTS {
            let current = match known.take() {
                Some(stored) => Some(stored),
                None => self.load(actor).await?,
            };
            if let Some(current) = &current {
                // Only an actor that never stored state may still change its home region.
                ensure!(
                    current.record.region == region
                        || (current.record.sealed && current.record.base.is_none()),
                    "ownership scope cannot change"
                );
                if current.record.lease.expires_at_ms > self.now()? {
                    self.log_claim(actor, "held", attempt, started);
                    return Ok(Claim::Held);
                }
                if !current.record.sealed {
                    self.seal(current.clone()).await?;
                    continue;
                }
            }
            let mut record = self.claimed(actor, host, session, region, current.as_ref())?;
            if self
                .replace(&mut record, current.as_ref().map(|stored| stored.version))
                .await?
            {
                self.log_claim(actor, "won", attempt, started);
                return Ok(Claim::Won(ActorClaim {
                    progress: current.and_then(|stored| stored.record.checkpoint_progress),
                    record,
                }));
            }
        }
        anyhow::bail!("actor ownership kept changing during its claim")
    }

    pub(crate) async fn renew(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
    ) -> Result<HostLease> {
        request.validate_duration()?;
        let current = self.load(actor).await?.context("actor ownership missing")?;
        let mut record = current.record;
        ensure!(
            record.lease.id == request.id && record.lease.session_id == request.session_id,
            "actor ownership changed"
        );
        let now = self.now()?;
        ensure!(
            record.lease.expires_at_ms > now,
            "expired activation cannot renew"
        );
        // A claim has no route until its host's first renewal names one.
        ensure!(
            record.lease.route.is_empty() || record.lease.route == request.route,
            "activation route cannot change"
        );
        record.lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: now
                .checked_add(request.duration_ms)
                .context("lease expiration overflow")?,
        };
        ensure!(
            self.replace(&mut record, Some(current.version)).await?,
            "actor activation changed concurrently"
        );
        Ok(record.lease)
    }

    /// Records the route of a claimed host once it is ready, so control planes can reach it before
    /// its first renewal. A host that renewed or released first already wrote the record.
    pub(crate) async fn record_route(&self, actor: &ActorKey, lease: &HostLease) -> Result<()> {
        let Some(current) = self.load(actor).await? else {
            return Ok(());
        };
        let mut record = current.record;
        if record.lease.id != lease.id
            || record.lease.session_id != lease.session_id
            || !record.lease.route.is_empty()
        {
            return Ok(());
        }
        record.lease.route = lease.route.clone();
        self.replace(&mut record, Some(current.version)).await?;
        Ok(())
    }

    /// Ends the session's lease. With a checkpoint, also seals the record at the state it names.
    pub(crate) async fn release(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        checkpoint: Option<SessionCheckpoint>,
    ) -> Result<()> {
        for _ in 0..3 {
            let Some(current) = self.load(actor).await? else {
                return Ok(());
            };
            let mut record = current.record;
            if record.lease.id != *host
                || record.lease.session_id != session
                || record.lease.expires_at_ms == 0
            {
                return Ok(());
            }
            record.lease.expires_at_ms = 0;
            if let Some(checkpoint) = &checkpoint {
                record.base = checkpoint.snapshot.clone();
                record.checkpoint_progress = checkpoint.progress;
                record.sealed = true;
            }
            if self.replace(&mut record, Some(current.version)).await? {
                return Ok(());
            }
        }
        anyhow::bail!("actor activation kept changing during release")
    }

    pub(super) async fn load(&self, actor: &ActorKey) -> Result<Option<Stored>> {
        let stored = self.store.load(actor).await?;
        if let Some(stored) = &stored {
            ensure!(
                stored.record.actor == *actor,
                "ownership record belongs to another actor"
            );
            ensure!(
                stored
                    .record
                    .persistence
                    .same_backend(&self.storage.persistence),
                "actor persistence backend changed; an explicit state migration is required"
            );
        }
        Ok(stored)
    }

    /// Seals a session whose host never sealed it, at the newest state the host wrote.
    pub(super) async fn seal(&self, stored: Stored) -> Result<bool> {
        let mut record = stored.record;
        let recovered = self.storage.recover_session(&record).await?;
        crate::bucket::advance(&mut record.base, recovered)?;
        record.sealed = true;
        self.replace(&mut record, Some(stored.version)).await
    }

    pub(super) async fn replace(
        &self,
        record: &mut Ownership,
        expected: Option<Version>,
    ) -> Result<bool> {
        record.mutation = uuid::Uuid::new_v4().to_string();
        self.store.replace(record, expected).await
    }

    pub(super) fn now(&self) -> Result<u64> {
        self.storage.clock.now_ms()
    }

    fn claimed(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        region: &str,
        current: Option<&Stored>,
    ) -> Result<Ownership> {
        let previous = current.map(|stored| &stored.record);
        Ok(Ownership {
            persistence: self.storage.persistence.clone(),
            sealed: false,
            actor: actor.clone(),
            epoch: previous
                .map_or(Some(1), |record| record.epoch.checked_add(1))
                .context("owner epoch overflow")?,
            region: region.into(),
            checkpoint_progress: None,
            base: previous.and_then(|record| record.base.clone()),
            lease: HostLease {
                id: host.clone(),
                session_id: session.into(),
                route: String::new(),
                expires_at_ms: self
                    .now()?
                    .checked_add(max_host_lease_duration_ms()?)
                    .context("lease expiration overflow")?,
            },
            mutation: String::new(),
        })
    }

    fn log_claim(&self, actor: &ActorKey, outcome: &str, attempts: usize, started: Instant) {
        tracing::info!(
            event = "actor_ownership_claim",
            project_id = %actor.project_id,
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            outcome,
            attempts,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
    }
}

#[async_trait]
impl ObjectPlacementStore for Ownerships {
    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(Option<ObjectPlacement>, Option<OwnershipHint>)> {
        let actor = crate::storage_paths::actor_from_key(object)?;
        Ok(match self.load(&actor).await? {
            Some(stored) => (Some(stored.record.placement()), Some(stored.into())),
            None => (None, None),
        })
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        let actor = crate::storage_paths::actor_from_key(object)?;
        match self.load(&actor).await? {
            Some(stored) => Ok(Some(self.storage.current_placement(&stored.record).await?)),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl crate::host_leases::AbandonedLeases for Ownerships {
    async fn release(&self, actor: &ActorKey, host: &HostId, session_id: &str) -> Result<()> {
        Ownerships::release(self, actor, host, session_id, None).await
    }
}
