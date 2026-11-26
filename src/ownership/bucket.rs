use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use super::{Ownership, OwnershipStore, Stored, Version};
use crate::{actor::ActorKey, bucket::Bucket, storage_paths};

const CONCURRENT_READS: usize = 16;

/// Ownership records as objects in the authority bucket, replaced by object generation.
pub(crate) struct BucketOwnership(Arc<dyn Bucket>);

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum Object {
    /// PostgreSQL holds the actor's ownership. Readers that predate the move cannot parse this,
    /// so they can no longer claim or renew the actor.
    Moved {
        moved_to_postgres: Ownership,
    },
    Owned(Ownership),
}

impl Object {
    pub(super) fn into_record(self) -> Ownership {
        match self {
            Self::Moved { moved_to_postgres } => moved_to_postgres,
            Self::Owned(record) => record,
        }
    }
}

impl BucketOwnership {
    pub(crate) fn new(bucket: Arc<dyn Bucket>) -> Self {
        Self(bucket)
    }

    pub(super) async fn read(&self, actor: &ActorKey) -> Result<Option<(i64, Object)>> {
        self.read_key(&key(actor)?).await
    }

    /// Replaces the record with a marker holding `record`, the actor's record in PostgreSQL.
    pub(super) async fn mark_moved(
        &self,
        record: &Ownership,
        expected: Option<Version>,
    ) -> Result<bool> {
        let moved = Object::Moved {
            moved_to_postgres: record.clone(),
        };
        self.write(&record.actor, expected, &moved).await
    }

    /// Every record `keep` returns something for, and an error for each that could not be read.
    pub(super) async fn scan<T: Send>(
        &self,
        keep: impl Fn(i64, Object) -> Option<T> + Sync,
    ) -> Result<Vec<Result<T>>> {
        let keys = self.0.list(&storage_paths::owners()).await?;
        let keep = &keep;
        Ok(stream::iter(keys)
            .map(|key| async move {
                match self.read_key(&key).await {
                    Ok(object) => object
                        .and_then(|(generation, object)| keep(generation, object))
                        .map(Ok),
                    Err(error) => Some(Err(error.context(format!("read {key}")))),
                }
            })
            .buffer_unordered(CONCURRENT_READS)
            .filter_map(|kept| async move { kept })
            .collect()
            .await)
    }

    async fn read_key(&self, key: &str) -> Result<Option<(i64, Object)>> {
        let Some(object) = self.0.get(key).await? else {
            return Ok(None);
        };
        Ok(Some((
            object.generation,
            serde_json::from_reader(object.bytes.reader())?,
        )))
    }

    async fn write(
        &self,
        actor: &ActorKey,
        expected: Option<Version>,
        object: &Object,
    ) -> Result<bool> {
        let generation = match expected {
            None => None,
            Some(Version::Bucket(generation)) => Some(generation),
            Some(Version::Postgres(_)) => bail!("a bucket record cannot replace a PostgreSQL one"),
        };
        crate::bucket::replace(
            self.0.as_ref(),
            &key(actor)?,
            generation,
            crate::payload::encode(object)?,
        )
        .await
    }
}

#[async_trait]
impl OwnershipStore for BucketOwnership {
    async fn load(&self, actor: &ActorKey) -> Result<Option<Stored>> {
        match self.read(actor).await? {
            None => Ok(None),
            Some((generation, Object::Owned(record))) => Ok(Some(Stored {
                version: Version::Bucket(generation),
                record,
            })),
            Some((_, Object::Moved { .. })) => bail!("actor ownership moved to PostgreSQL"),
        }
    }

    async fn replace(&self, record: &Ownership, expected: Option<Version>) -> Result<bool> {
        self.write(&record.actor, expected, &Object::Owned(record.clone()))
            .await
    }

    async fn unsealed(&self) -> Result<Vec<Stored>> {
        let scanned = self
            .scan(|generation, object| match object {
                Object::Owned(record) if !record.sealed => Some(Stored {
                    version: Version::Bucket(generation),
                    record,
                }),
                _ => None,
            })
            .await?;
        Ok(scanned
            .into_iter()
            .filter_map(|stored| {
                stored
                    .inspect_err(|error| {
                        tracing::warn!(error = %format!("{error:#}"), "unreadable ownership record skipped")
                    })
                    .ok()
            })
            .collect())
    }
}

fn key(actor: &ActorKey) -> Result<String> {
    storage_paths::owner(&actor.storage_key())
}
