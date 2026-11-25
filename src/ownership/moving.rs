use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use super::{
    BucketOwnership, Ownership, OwnershipStore, PostgresOwnership, Stored, Version, bucket::Object,
};
use crate::{actor::ActorKey, clock::Clock};

const BACKFILL_INTERVAL: Duration = Duration::from_secs(10 * 60);
const CONCURRENT_MOVES: usize = 16;

/// Ownership while a cluster moves from the bucket to PostgreSQL. Reads try PostgreSQL first. A
/// bucket record moves when it is next replaced: a marker replaces it at the generation read,
/// which fences every reader that predates the move, and then PostgreSQL takes the record.
pub(crate) struct MovingOwnership {
    postgres: PostgresOwnership,
    bucket: BucketOwnership,
    clock: Arc<dyn Clock>,
}

impl MovingOwnership {
    pub(crate) fn new(
        postgres: PostgresOwnership,
        bucket: BucketOwnership,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            postgres,
            bucket,
            clock,
        }
    }

    /// Moves records until none are left in the bucket. Live leases move once they end.
    pub(crate) fn start_backfill(self: Arc<Self>, stop: CancellationToken) {
        tokio::spawn(async move {
            loop {
                match self.backfill().await {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(error = %format!("{error:#}"), "ownership backfill deferred")
                    }
                }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(BACKFILL_INTERVAL) => {}
                }
            }
        });
    }

    /// Moves every bucket record whose lease has ended and returns how many remain.
    pub(crate) async fn backfill(&self) -> Result<usize> {
        let started = std::time::Instant::now();
        let now = self.clock.now_ms()?;
        let owned = self
            .bucket
            .scan(|generation, object| match object {
                Object::Owned(record) => Some((generation, record)),
                Object::Moved { .. } => None,
            })
            .await?;
        let (live, ended): (Vec<_>, Vec<_>) = owned
            .into_iter()
            .partition(|(_, record)| record.lease.expires_at_ms > now);
        let outcomes: Vec<_> = stream::iter(ended)
            .map(|(generation, record)| async move {
                self.replace(&record, Some(Version::Bucket(generation)))
                    .await
            })
            .buffer_unordered(CONCURRENT_MOVES)
            .collect()
            .await;
        let moved = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(true)))
            .count();
        let remaining = live.len() + outcomes.len() - moved;
        tracing::info!(
            event = "ownership_backfill",
            moved,
            live = live.len(),
            remaining,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(remaining)
    }
}

#[async_trait]
impl OwnershipStore for MovingOwnership {
    async fn load(&self, actor: &ActorKey) -> Result<Option<Stored>> {
        if let Some(stored) = self.postgres.load(actor).await? {
            return Ok(Some(stored));
        }
        Ok(self
            .bucket
            .read(actor)
            .await?
            .map(|(generation, object)| Stored {
                version: Version::Bucket(generation),
                record: object.into_record(),
            }))
    }

    async fn replace(&self, record: &Ownership, expected: Option<Version>) -> Result<bool> {
        if let Some(Version::Postgres(_)) = expected {
            return self.postgres.replace(record, expected).await;
        }
        Ok(self.bucket.mark_moved(record, expected).await?
            && self.postgres.replace(record, None).await?)
    }

    async fn unsealed(&self) -> Result<Vec<Stored>> {
        let mut unsealed = self.postgres.unsealed().await?;
        unsealed.extend(self.bucket.unsealed().await?);
        Ok(unsealed)
    }
}
