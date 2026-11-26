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
const BACKFILL_LOCK: &str = "ownership-backfill";
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

    /// Moves bucket records to PostgreSQL every interval for as long as this mode runs, since
    /// control planes that predate it can still write the bucket. One control plane at a time does
    /// this, holding a PostgreSQL lock, and gives up after a pass once that lock's session ends.
    pub(crate) fn start_backfill(self: Arc<Self>, stop: CancellationToken) {
        tokio::spawn(async move {
            loop {
                let lock = match self.postgres.try_lock(BACKFILL_LOCK).await {
                    Ok(lock) => lock,
                    Err(error) => {
                        tracing::warn!(error = %format!("{error:#}"), "ownership backfill lock deferred");
                        None
                    }
                };
                while lock.as_ref().is_some_and(|lock| !lock.is_lost()) {
                    if let Err(error) = self.backfill().await {
                        tracing::warn!(error = %format!("{error:#}"), "ownership backfill deferred");
                    }
                    if !wait(&stop).await {
                        return;
                    }
                }
                if lock.is_none() && !wait(&stop).await {
                    return;
                }
            }
        });
    }

    /// Moves every bucket record whose lease has ended and returns how many remain, counting
    /// live leases and records it could not read.
    pub(crate) async fn backfill(&self) -> Result<usize> {
        let started = std::time::Instant::now();
        let now = self.clock.now_ms()?;
        let (mut live, mut unreadable, mut candidates) = (0, 0, Vec::new());
        for entry in self
            .bucket
            .scan(|generation, object| Some((generation, object)))
            .await?
        {
            match entry {
                Err(error) => {
                    unreadable += 1;
                    tracing::warn!(error = %format!("{error:#}"), "ownership backfill could not read a record");
                }
                Ok((_, Object::Owned(record))) if record.lease.expires_at_ms > now => live += 1,
                Ok(entry) => candidates.push(entry),
            }
        }
        // A marker without a PostgreSQL row is a move that stopped between its two writes.
        let in_postgres = self.postgres.actors().await?;
        candidates.retain(|(_, object)| match object {
            Object::Moved { moved_to_postgres } => !in_postgres.contains(&moved_to_postgres.actor),
            Object::Owned(_) => true,
        });
        let outcomes: Vec<_> = stream::iter(candidates)
            .map(|(generation, object)| async move {
                self.replace(&object.into_record(), Some(Version::Bucket(generation)))
                    .await
            })
            .buffer_unordered(CONCURRENT_MOVES)
            .collect()
            .await;
        let moved = outcomes.iter().filter(|o| matches!(o, Ok(true))).count();
        let remaining = live + unreadable + outcomes.len() - moved;
        tracing::info!(
            event = "ownership_backfill",
            moved,
            live,
            unreadable,
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

/// Waits one backfill interval; `false` once the control plane stops.
async fn wait(stop: &CancellationToken) -> bool {
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(BACKFILL_INTERVAL) => true,
    }
}
