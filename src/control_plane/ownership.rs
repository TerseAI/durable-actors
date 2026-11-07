use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    bucket::{Bucket, BucketObject, OwnershipStore, confirm_replace},
    clock::Clock,
    postgres::PostgresDatabase,
};

// Created here rather than migrated so a control plane built before this table still starts.
const SCHEMA: &str = "
SELECT pg_advisory_xact_lock(hashtext('durable-actors'), hashtext('actor-ownership'));
CREATE TABLE IF NOT EXISTS actor_ownership (
    key TEXT PRIMARY KEY,
    generation BIGINT NOT NULL CHECK (generation > 0),
    record BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);";

/// Keeps ownership records in PostgreSQL, adopting each actor's bucket record on first read.
pub(super) struct PostgresOwnership {
    database: PostgresDatabase,
    legacy: Arc<dyn Bucket>,
    clock: Arc<dyn Clock>,
    schema: tokio::sync::OnceCell<()>,
}

impl PostgresOwnership {
    pub(super) fn new(
        database: PostgresDatabase,
        legacy: Arc<dyn Bucket>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            legacy,
            clock,
            schema: tokio::sync::OnceCell::new(),
        }
    }
}

#[async_trait]
impl OwnershipStore for PostgresOwnership {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.ensure_schema().await?;
        match self.select(key).await? {
            Some(record) => Ok(Some(record)),
            None => self.adopt(key).await,
        }
    }

    async fn replace(&self, key: &str, generation: Option<i64>, bytes: Bytes) -> Result<bool> {
        self.ensure_schema().await?;
        let replaced = match generation {
            Some(generation) => self.update(key, generation, &bytes).await,
            None => self.insert(key, 1, &bytes).await,
        };
        confirm_replace(self, key, &bytes, replaced).await
    }
}

/// Marks a bucket record as moved so binaries that still read the bucket fail instead of
/// reviving it.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Moved {
    moved_to_postgres: String,
}

#[derive(Deserialize)]
struct LeasedRecord {
    lease: LeaseExpiry,
}

#[derive(Deserialize)]
struct LeaseExpiry {
    expires_at_ms: u64,
}

impl PostgresOwnership {
    /// A record whose lease a bucket-backed host still renews stays in the bucket until released.
    /// Adopted generations exceed the bucket's, so a hint read before the move cannot match.
    async fn adopt(&self, key: &str) -> Result<Option<BucketObject>> {
        let Some(legacy) = self.legacy.get(key).await? else {
            return Ok(None);
        };
        let record = match serde_json::from_slice::<Moved>(&legacy.bytes) {
            Ok(moved) => Bytes::from(STANDARD.decode(moved.moved_to_postgres)?),
            Err(_) if self.leased(&legacy.bytes)? => return Ok(Some(legacy)),
            Err(_) => {
                if !self.mark_moved(key, &legacy).await? {
                    return self.get(key).await;
                }
                legacy.bytes.clone()
            }
        };
        let generation = legacy
            .generation
            .checked_add(1)
            .context("ownership generation overflow")?;
        self.insert(key, generation, &record).await?;
        self.select(key).await
    }

    fn leased(&self, bytes: &[u8]) -> Result<bool> {
        let record: LeasedRecord =
            serde_json::from_slice(bytes).context("parse bucket ownership record")?;
        Ok(record.lease.expires_at_ms > self.clock.now_ms()?)
    }

    async fn mark_moved(&self, key: &str, legacy: &BucketObject) -> Result<bool> {
        let moved = serde_json::to_vec(&Moved {
            moved_to_postgres: STANDARD.encode(&legacy.bytes),
        })?;
        crate::bucket::replace(
            self.legacy.as_ref(),
            key,
            Some(legacy.generation),
            moved.into(),
        )
        .await
    }

    async fn select(&self, key: &str) -> Result<Option<BucketObject>> {
        let row = self
            .database
            .query_opt(
                "SELECT generation, record FROM actor_ownership WHERE key = $1",
                &[&key],
            )
            .await?;
        Ok(row.map(|row| BucketObject {
            generation: row.get(0),
            bytes: Bytes::from(row.get::<_, Vec<u8>>(1)),
        }))
    }

    async fn insert(&self, key: &str, generation: i64, bytes: &[u8]) -> Result<bool> {
        let inserted = self
            .database
            .execute(
                "INSERT INTO actor_ownership (key, generation, record) VALUES ($1, $2, $3)
                 ON CONFLICT (key) DO NOTHING",
                &[&key, &generation, &bytes],
            )
            .await?;
        Ok(inserted == 1)
    }

    async fn update(&self, key: &str, generation: i64, bytes: &[u8]) -> Result<bool> {
        let updated = self
            .database
            .execute(
                "UPDATE actor_ownership SET generation = generation + 1, record = $3, updated_at = now()
                 WHERE key = $1 AND generation = $2",
                &[&key, &generation, &bytes],
            )
            .await?;
        Ok(updated == 1)
    }

    async fn ensure_schema(&self) -> Result<()> {
        self.schema
            .get_or_try_init(|| async {
                let mut client = self.database.connection().await?;
                let transaction = client.transaction().await?;
                transaction.batch_execute(SCHEMA).await?;
                transaction
                    .commit()
                    .await
                    .context("create actor ownership table")
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/ownership.rs"]
mod tests;
