use std::time::Instant;

use super::*;
use crate::ownership::ActorClaim;

#[derive(Default)]
pub(crate) struct CachedBase {
    /// The base this node parked a closed database at. When the claim names exactly this base, the
    /// database file already holds its pages, so activation skips the snapshot.
    pub parked: Option<SnapshotRef>,
}

impl RuntimeStorage {
    /// Starts the session the control plane claimed for this host, from the state its sealed
    /// predecessor left.
    pub(crate) async fn adopt(
        &self,
        claim: &ActorClaim,
        cached: &CachedBase,
    ) -> Result<LoadedActor> {
        let record = claim.record.clone();
        let started = Instant::now();
        let (snapshot, head) = if cached.parked.is_some() && record.base == cached.parked {
            (None, record.base.clone())
        } else {
            (self.load_latest(&record, None).await?, None)
        };
        let snapshot_load_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let stream_started = Instant::now();
        self.snapshots
            .start(&record.stream()?, record.base.as_ref().zip(claim.progress))
            .await?;
        tracing::info!(
            event = "actor_activation_storage",
            project_id = %record.actor.project_id,
            actor_name = %record.actor.actor_name,
            actor_id = %record.actor.actor_id,
            host_id = %record.lease.id,
            session_id = %record.lease.session_id,
            new_actor = claim.is_new(),
            parked = head.is_some(),
            snapshot_load_ms,
            stream_open_ms = stream_started.elapsed().as_secs_f64() * 1_000.0,
            duration_ms = started.elapsed().as_secs_f64() * 1_000.0,
        );
        Ok(self.remember(record, snapshot, head))
    }

    /// Records a lease the control plane renewed.
    pub(crate) fn renewed(&self, actor: &ActorKey, lease: &HostLease) -> Result<()> {
        let mut owned = self.owned.lock().unwrap();
        let record = owned
            .get_mut(actor.storage_key().as_str())
            .context("actor is not locally activated")?;
        ensure!(
            record.lease.id == lease.id && record.lease.session_id == lease.session_id,
            "local ownership changed"
        );
        record.lease = lease.clone();
        Ok(())
    }
}
