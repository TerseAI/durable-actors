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
        let outcomes: Vec<_> = stream::iter(candidates)
            .map(|(generation, object)| self.finish_move(generation, object))
            .buffer_unordered(CONCURRENT_MOVES)
            .collect()
            .await;
        let moved = outcomes
            .iter()
            .filter(|o| matches!(o, Ok(Move::Moved)))
            .count();
        let pending = outcomes
            .iter()
            .filter(|o| !matches!(o, Ok(Move::Moved | Move::Done)))
            .count();
        let remaining = live + unreadable + pending;
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

    /// Moves an ended bucket record, or finishes a move that marked the bucket but never reached
    /// PostgreSQL.
    async fn finish_move(&self, generation: i64, object: Object) -> Result<Move> {
        if let Object::Moved { moved_to_postgres } = &object
            && self
                .postgres
                .load(&moved_to_postgres.actor)
                .await?
                .is_some()
        {
            return Ok(Move::Done);
        }
        let moved = self
            .replace(&object.into_record(), Some(Version::Bucket(generation)))
            .await?;
        Ok(if moved { Move::Moved } else { Move::Pending })
    }
}

enum Move {
    Moved,
    /// PostgreSQL already holds the record.
    Done,
    /// The record changed since it was read.
    Pending,
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
