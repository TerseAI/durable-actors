use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    bucket::{Bucket, BucketObject, OwnershipStore, confirm_replace},
    clock::Clock,
    host_leases::HostLease,
    payload::Payload,
    postgres::PostgresDatabase,
};

/// Keeps ownership records in PostgreSQL, moving each actor's bucket record here on the first
/// lookup after its lease ends.
pub(super) struct PostgresOwnership {
    database: PostgresDatabase,
    legacy: Arc<dyn Bucket>,
    clock: Arc<dyn Clock>,
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
        }
    }
}

#[async_trait]
impl OwnershipStore for PostgresOwnership {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        match self.select(key).await? {
            Some(record) => Ok(Some(record)),
            None => self.adopt(key).await,
        }
    }

    async fn replace(&self, key: &str, generation: Option<i64>, bytes: Payload) -> Result<bool> {
        let record = bytes.to_bytes()?;
        let replaced = match generation {
            Some(generation) => self.update(key, generation, &record).await,
            None => self.insert(key, 1, &record).await,
        };
        confirm_replace(self, key, &bytes, replaced).await
    }

    async fn keys(&self) -> Result<Vec<String>> {
        let prefix = crate::storage_paths::owners();
        let (rows, legacy) = tokio::try_join!(self.select_keys(), self.legacy.list(&prefix))?;
        let keys: BTreeSet<String> = rows.into_iter().chain(legacy).collect();
        Ok(keys.into_iter().collect())
    }

    async fn peek(&self, key: &str) -> Result<Option<BucketObject>> {
        if let Some(record) = self.select(key).await? {
            return Ok(Some(record));
        }
        Ok(self
            .legacy
            .get(key)
            .await?
            .filter(|object| serde_json::from_reader::<_, Moved>(object.bytes.reader()).is_err()))
    }
}

/// Marks a bucket record as moved so binaries that still read the bucket fail instead of
/// reviving it.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Moved {
    moved_to_postgres: String,
}

/// The parts of a bucket record the lease check and usage logs read.
#[derive(Deserialize)]
struct LegacyRecord {
    actor: ActorKey,
    lease: HostLease,
    epoch: u64,
}

impl PostgresOwnership {
    /// A record whose lease a bucket-backed host still renews stays in the bucket until released.
    /// Adopted generations exceed the bucket's, so a hint read before the move cannot match.
    async fn adopt(&self, key: &str) -> Result<Option<BucketObject>> {
        let Some(legacy) = self.legacy.get(key).await? else {
            log_bucket_read(key, "absent", None);
            return Ok(None);
        };
        let bytes = legacy.bytes.to_bytes()?;
        let record = match serde_json::from_slice::<Moved>(&bytes) {
            Ok(moved) => {
                let record = Bytes::from(STANDARD.decode(moved.moved_to_postgres)?);
                log_bucket_read(key, "moved", Some(&record));
                record
            }
            Err(_) if self.leased(key, &bytes)? => return Ok(Some(legacy)),
            Err(_) => {
                if !self.mark_moved(key, legacy.generation, &bytes).await? {
                    return self.get(key).await;
                }
                bytes
            }
        };
        let generation = legacy
            .generation
            .checked_add(1)
            .context("ownership generation overflow")?;
        self.insert(key, generation, &record).await?;
        self.select(key).await
    }

    fn leased(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        let record: LegacyRecord =
            serde_json::from_slice(bytes).context("parse bucket ownership record")?;
        let leased = record.lease.expires_at_ms > self.clock.now_ms()?;
        log_bucket_read(key, if leased { "leased" } else { "released" }, Some(bytes));
        Ok(leased)
    }

    /// A renewal or activation that lands after the read makes the marker lose, and the lookup
    /// starts over.
    async fn mark_moved(&self, key: &str, generation: i64, bytes: &[u8]) -> Result<bool> {
        let moved = serde_json::to_vec(&Moved {
            moved_to_postgres: STANDARD.encode(bytes),
        })?;
        let marked =
            crate::bucket::replace(self.legacy.as_ref(), key, Some(generation), moved.into()).await;
        let outcome = match &marked {
            Ok(true) => "moved",
            Ok(false) => "conflict",
            Err(_) => "failed",
        };
        log_bucket_access(
            "ownership_bucket_write",
            key,
            "migration",
            outcome,
            Some(bytes),
        );
        marked
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
            bytes: row.get::<_, Vec<u8>>(1).into(),
        }))
    }

    async fn select_keys(&self) -> Result<Vec<String>> {
        let rows = self
            .database
            .connection()
            .await?
            .query("SELECT key FROM actor_ownership", &[])
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
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
}

fn log_bucket_read(key: &str, outcome: &str, record: Option<&[u8]>) {
    log_bucket_access("ownership_bucket_read", key, "read", outcome, record);
}

/// Each remaining use of bucket ownership, so its decline can be watched in Cloud Logging.
fn log_bucket_access(
    event: &'static str,
    key: &str,
    operation: &str,
    outcome: &str,
    record: Option<&[u8]>,
) {
    let record = record.and_then(|bytes| serde_json::from_slice::<LegacyRecord>(bytes).ok());
    let record = record.as_ref();
    tracing::info!(
        event,
        key,
        operation,
        outcome,
        project_id = record.map(|record| record.actor.project_id.as_str()),
        actor_name = record.map(|record| record.actor.actor_name.as_str()),
        actor_id = record.map(|record| record.actor.actor_id.as_str()),
        host_id = record.map(|record| record.lease.id.as_str()),
        session_id = record.map(|record| record.lease.session_id.as_str()),
        owner_epoch = record.map(|record| record.epoch),
        lease_expires_at_ms = record.map(|record| record.lease.expires_at_ms),
    );
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/ownership.rs"]
mod tests;
