use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use futures_util::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use super::Ownerships;

/// Long enough that an actor with traffic is reactivated, and recovered, by its next claim first.
const ABANDONED_AFTER_MS: u64 = 10 * 60 * 1000;
const RECOVERY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const CONCURRENT_RECORDS: usize = 16;

impl Ownerships {
    /// Recovers and seals activations whose hosts stopped without sealing them, which otherwise
    /// wait for the actor's next claim.
    pub(crate) fn start_abandoned_recovery(self: Arc<Self>, stop: CancellationToken) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = interval.tick() => {}
                }
                if let Err(error) = self.seal_abandoned().await {
                    tracing::warn!(%error, "abandoned activation recovery deferred");
                }
            }
        });
    }

    pub(crate) async fn seal_abandoned(&self) -> Result<()> {
        let started = std::time::Instant::now();
        let unsealed = self.store.unsealed().await?;
        let records = unsealed.len();
        let now = self.now()?;
        let abandoned = unsealed.into_iter().filter(|stored| {
            stored
                .record
                .lease
                .expires_at_ms
                .saturating_add(ABANDONED_AFTER_MS)
                <= now
                && stored
                    .record
                    .persistence
                    .same_backend(&self.storage.persistence)
        });
        let outcomes: Vec<_> = stream::iter(abandoned)
            .map(|stored| async move {
                let actor = stored.record.actor.clone();
                (actor, self.seal(stored).await)
            })
            .buffer_unordered(CONCURRENT_RECORDS)
            .collect()
            .await;
        let mut sealed = 0;
        for (actor, outcome) in outcomes {
            match outcome {
                Ok(true) => sealed += 1,
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    error = %format!("{error:#}"),
                    project_id = %actor.project_id,
                    actor_name = %actor.actor_name,
                    actor_id = %actor.actor_id,
                    "abandoned activation recovery deferred"
                ),
            }
        }
        tracing::info!(
            event = "abandoned_activations_sealed",
            records,
            sealed,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    /// Ends every live lease, which lets a new host take over at once. Only for a store that no
    /// live host can be using.
    pub(crate) async fn release_leases(&self) -> Result<()> {
        let now = self.now()?;
        for stored in self.store.unsealed().await? {
            if stored.record.lease.expires_at_ms <= now {
                continue;
            }
            let mut record = stored.record;
            record.lease.expires_at_ms = 0;
            ensure!(
                self.replace(&mut record, Some(stored.version)).await?,
                "actor ownership changed while its lease was released"
            );
        }
        Ok(())
    }
}
