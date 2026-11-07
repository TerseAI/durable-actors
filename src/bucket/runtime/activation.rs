use std::time::Instant;

use super::*;
use crate::host_leases::HostLeaseRequest;

#[derive(Default)]
struct ActivationRecovery {
    snapshot: Option<LoadedSnapshot>,
    checkpoint: Option<super::super::recovery::PrefetchedCheckpoint>,
    checkpoint_ms: Option<f64>,
    snapshot_ms: Option<f64>,
}

/// What a host already holds for the base its claimed epoch starts from.
#[derive(Default)]
pub(crate) struct CachedBase {
    /// The base's SQLite database: its checkpoint is left for SQLite recovery to fetch.
    pub database: bool,
    /// The base snapshot, used only when it matches the digest the claim names.
    pub snapshot: Option<Bytes>,
}

impl RuntimeStorage {
    /// Claims and adopts an activation in one process, which is the control plane's and the host's
    /// work together.
    pub async fn register_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<LoadedActor> {
        let claim = self
            .claim_activation(actor, request, region, new_actor, owner_hint)
            .await?;
        self.adopt_activation(actor, request, region, &claim, &CachedBase::default())
            .await
    }

    /// Starts the epoch the control plane claimed for this host. The claim already fenced every
    /// earlier owner, so only the base download and the stream remain.
    pub(crate) async fn adopt_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        claim: &OwnershipClaim,
        cached: &CachedBase,
    ) -> Result<LoadedActor> {
        let started = Instant::now();
        let record = claim.0.clone();
        ensure!(
            record.persistence.same_backend(&self.persistence)
                && record.actor == *actor
                && record.region == region
                && !record.sealed
                && record.lease.id == request.id
                && record.lease.session_id == request.session_id
                && record.lease.route == request.route,
            "control plane claimed a different activation"
        );
        let recovery = match self.load_activation_base(&record, cached).await {
            Ok(recovery) => recovery,
            Err(error) => {
                let checkpoint = SessionCheckpoint {
                    snapshot: record.base.clone(),
                };
                if let Err(release) = self
                    .release_with_checkpoint(
                        actor,
                        &request.id,
                        &request.session_id,
                        Some(checkpoint),
                    )
                    .await
                {
                    tracing::warn!(%release, "failed activation lease release deferred to recovery");
                }
                return Err(error);
            }
        };
        let stream_started = Instant::now();
        self.snapshots.start(&record.stream()?).await?;
        let stream_open_ms = stream_started.elapsed().as_secs_f64() * 1_000.0;
        tracing::info!(
            event = "actor_activation_storage",
            project_id = %actor.project_id,
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            host_id = %request.id,
            session_id = %request.session_id,
            owner_epoch = record.epoch,
            stream_open_ms,
            snapshot_load_ms = recovery.snapshot_ms,
            checkpoint_prefetch_ms = recovery.checkpoint_ms,
            duration_ms = started.elapsed().as_secs_f64() * 1_000.0,
        );
        let mut loaded = self.remember(record, recovery.snapshot);
        loaded.checkpoint = recovery.checkpoint;
        Ok(loaded)
    }

    pub async fn renew_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
    ) -> Result<HostLease> {
        let (generation, mut record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor ownership missing")?;
        ensure!(
            record.actor == *actor
                && record.lease.id == request.id
                && record.lease.session_id == request.session_id,
            "actor ownership changed"
        );
        let current = &record.lease;
        ensure!(
            current.expires_at_ms > self.clock.now_ms()?,
            "expired activation cannot renew"
        );
        ensure!(
            current.route == request.route,
            "activation route cannot change"
        );
        let lease = self.new_lease(request)?;
        record.lease = lease.clone();
        self.save_activation(&mut record, Some(generation)).await?;
        if let Some(owned) = self
            .owned
            .lock()
            .unwrap()
            .get_mut(actor.storage_key().as_str())
        {
            ensure!(owned.epoch == record.epoch, "local ownership epoch changed");
            owned.lease = lease.clone();
        }
        Ok(lease)
    }

    pub async fn release_activation(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<()> {
        self.release_with_checkpoint(actor, host, session, None)
            .await
    }

    pub(crate) async fn release_with_checkpoint(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        checkpoint: Option<SessionCheckpoint>,
    ) -> Result<()> {
        for _ in 0..3 {
            let Some((generation, mut record)) = self.load(&actor.storage_key()).await? else {
                return Ok(());
            };
            if record.lease.id != *host
                || record.lease.session_id != session
                || record.lease.expires_at_ms == 0
            {
                return Ok(());
            }
            record.lease.expires_at_ms = 0;
            if let Some(checkpoint) = &checkpoint {
                record.base = checkpoint.snapshot.clone();
                record.sealed = true;
            }
            if self
                .replace_activation(&mut record, Some(generation))
                .await?
            {
                return Ok(());
            }
        }
        anyhow::bail!("actor activation kept changing during release")
    }

    async fn load_activation_base(
        &self,
        record: &Ownership,
        cached: &CachedBase,
    ) -> Result<ActivationRecovery> {
        if record.base.is_none() {
            return Ok(ActivationRecovery::default());
        }
        if cached.database {
            return self
                .load_activation_snapshot(record, cached.snapshot.as_ref())
                .await;
        }
        let snapshot = self.load_activation_snapshot(record, cached.snapshot.as_ref());
        let checkpoint = self.prefetch_activation_checkpoint(record);
        tokio::pin!(snapshot, checkpoint);
        let (mut recovery, (checkpoint, checkpoint_ms)) = tokio::select! {
            result = &mut snapshot => {
                let recovery = result?;
                let needs_checkpoint = match &recovery.snapshot {
                    Some(snapshot) => crate::state_log::StateSnapshot::decode(&snapshot.bytes)?.sqlite.parent.is_some(),
                    None => false,
                };
                if !needs_checkpoint {
                    return Ok(recovery);
                }
                (recovery, checkpoint.await)
            }
            checkpoint = &mut checkpoint => (snapshot.await?, checkpoint),
        };
        recovery.checkpoint = checkpoint;
        recovery.checkpoint_ms = Some(checkpoint_ms);
        Ok(recovery)
    }

    async fn load_activation_snapshot(
        &self,
        record: &Ownership,
        cached: Option<&Bytes>,
    ) -> Result<ActivationRecovery> {
        let started = Instant::now();
        let cached = record
            .base
            .as_ref()
            .zip(cached)
            .filter(|(base, bytes)| base.verify(bytes).is_ok())
            .map(|(base, bytes)| LoadedSnapshot {
                reference: base.clone(),
                bytes: bytes.clone(),
            });
        let snapshot = self.load_latest(record, cached).await?;
        Ok(ActivationRecovery {
            snapshot,
            snapshot_ms: Some(started.elapsed().as_secs_f64() * 1_000.0),
            ..ActivationRecovery::default()
        })
    }

    async fn prefetch_activation_checkpoint(
        &self,
        record: &Ownership,
    ) -> (Option<super::super::recovery::PrefetchedCheckpoint>, f64) {
        let started = Instant::now();
        let checkpoint = match &record.base {
            Some(base) => match self.snapshots.prefetch_checkpoint(base).await {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    tracing::warn!(%error, "checkpoint prefetch deferred to SQLite recovery");
                    None
                }
            },
            None => None,
        };
        (checkpoint, started.elapsed().as_secs_f64() * 1_000.0)
    }

    async fn save_activation(&self, record: &mut Ownership, generation: Option<i64>) -> Result<()> {
        ensure!(
            self.replace_activation(record, generation).await?,
            "actor activation changed concurrently"
        );
        Ok(())
    }
}

impl OwnershipHint {
    /// Whether `request` may claim the hinted record without recovering the previous session.
    fn claimable(
        &self,
        actor: &ActorKey,
        region: &str,
        request: &HostLeaseRequest,
        now_ms: u64,
    ) -> bool {
        let record = &self.record;
        record.sealed
            && self.generation > 0
            && record.actor == *actor
            && record.region == region
            && record.lease.expires_at_ms <= now_ms
            && (record.lease.id != request.id || record.lease.session_id != request.session_id)
    }
}

impl RuntimeStorageReader {
    /// Claims the next epoch of `actor` for the host `request` describes. A live owner or a
    /// concurrent claim fails here, before the new host starts.
    pub(crate) async fn claim_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<OwnershipClaim> {
        actor.validate()?;
        crate::placement::validate_region(region)?;
        request.validate_duration()?;
        let started = Instant::now();
        let now = self.clock.now_ms()?;
        // Unsealed recovery can fence a live session if the hinted lease was renewed.
        let owner_hint = owner_hint.filter(|hint| hint.claimable(actor, region, request, now));
        for hint in [owner_hint, None] {
            let read_started = Instant::now();
            let current = if let Some(hint) = hint {
                Some((hint.generation, hint.record.clone()))
            } else if new_actor && owner_hint.is_none() {
                None
            } else {
                self.load(&actor.storage_key()).await?
            };
            let ownership_read_ms = read_started.elapsed().as_secs_f64() * 1_000.0;
            let previous = current.as_ref().map(|(_, record)| record);
            self.validate_activation(actor, request, region, previous)?;
            let mut record = self.activation_record(actor, request, region, previous)?;
            let crashed = previous.filter(|record| !record.sealed);
            let recovery_started = Instant::now();
            if let Some(crashed) = crashed {
                record.base = self.recover_session(crashed).await?;
            }
            let session_recovery_ms =
                crashed.map(|_| recovery_started.elapsed().as_secs_f64() * 1_000.0);
            let cas_started = Instant::now();
            let replaced = self
                .replace_activation(
                    &mut record,
                    current.as_ref().map(|(generation, _)| *generation),
                )
                .await?;
            let ownership_cas_ms = cas_started.elapsed().as_secs_f64() * 1_000.0;
            if !replaced {
                ensure!(hint.is_some(), "actor activation changed concurrently");
                continue;
            }
            tracing::info!(
                event = "actor_activation_claim",
                project_id = %actor.project_id,
                actor_name = %actor.actor_name,
                actor_id = %actor.actor_id,
                host_id = %request.id,
                session_id = %request.session_id,
                owner_epoch = record.epoch,
                new_actor,
                hinted = hint.is_some(),
                ownership_read_ms,
                session_recovery_ms,
                ownership_cas_ms,
                duration_ms = started.elapsed().as_secs_f64() * 1_000.0,
            );
            return Ok(OwnershipClaim(record));
        }
        anyhow::bail!("actor activation changed concurrently")
    }

    /// Seals a claim whose host may never have started. The seal keeps the claimed epoch, so a
    /// host that did start cannot share its stream with the next owner.
    pub(crate) async fn revoke_claim(&self, claim: &OwnershipClaim) -> Result<()> {
        let claimed = &claim.0;
        let Some((generation, mut record)) = self.load(&claimed.actor.storage_key()).await? else {
            return Ok(());
        };
        if record.mutation != claimed.mutation {
            return Ok(());
        }
        record.lease.expires_at_ms = 0;
        record.sealed = true;
        self.replace_activation(&mut record, Some(generation))
            .await?;
        Ok(())
    }

    /// The newest snapshot a crashed session made durable, persisted so the next epoch can name it
    /// as its base.
    async fn recover_session(&self, record: &Ownership) -> Result<Option<SnapshotRef>> {
        let recovered = self
            .snapshots
            .recover(&record.stream()?.prefix)
            .await?
            .map(|(key, bytes)| decode_snapshot(key, bytes))
            .transpose()?;
        let mut base = record.base.clone();
        advance(&mut base, recovered.map(|snapshot| snapshot.reference))?;
        Ok(base)
    }

    fn validate_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        current: Option<&Ownership>,
    ) -> Result<()> {
        let Some(record) = current else {
            return Ok(());
        };
        ensure!(
            record.persistence.same_backend(&self.persistence),
            "actor persistence configuration changed; an explicit state migration is required"
        );
        ensure!(
            record.actor == *actor && record.region == region,
            "ownership scope cannot change"
        );
        ensure!(
            record.lease.id != request.id || record.lease.session_id != request.session_id,
            "activation session cannot be reused"
        );
        ensure!(
            record.lease.expires_at_ms <= self.clock.now_ms()?,
            "previous owner lease is still active"
        );
        Ok(())
    }

    fn activation_record(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        current: Option<&Ownership>,
    ) -> Result<Ownership> {
        Ok(Ownership {
            persistence: self.persistence.clone(),
            sealed: false,
            actor: actor.clone(),
            epoch: current
                .map_or(Some(1), |record| record.epoch.checked_add(1))
                .context("owner epoch overflow")?,
            region: region.into(),
            base: current.and_then(|record| record.base.clone()),
            lease: self.new_lease(request)?,
            mutation: String::new(),
        })
    }

    fn new_lease(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        ensure!(
            !request.id.as_str().is_empty() && !request.session_id.is_empty(),
            "activation identity missing"
        );
        Ok(HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: self
                .clock
                .now_ms()?
                .checked_add(request.duration_ms)
                .context("lease expiration overflow")?,
        })
    }

    async fn replace_activation(
        &self,
        record: &mut Ownership,
        generation: Option<i64>,
    ) -> Result<bool> {
        record.mutation = uuid::Uuid::new_v4().to_string();
        self.ownership
            .replace(
                &ownership_key(&record.actor.storage_key())?,
                generation,
                crate::payload::encode(record)?,
            )
            .await
    }
}
