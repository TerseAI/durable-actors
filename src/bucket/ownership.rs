use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{Bucket, BucketObject};

/// Holds each actor's ownership record, which a writer replaces only at the generation it read.
#[async_trait]
pub(crate) trait OwnershipStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>>;
    /// `None` creates the record; a lost response is resolved by the record's unique bytes.
    async fn replace(&self, key: &str, generation: Option<i64>, bytes: Bytes) -> Result<bool>;
}

/// Where a host reads and replaces its actor's ownership record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OwnershipLocation {
    #[default]
    Bucket,
    ControlPlane,
}

pub(crate) struct BucketOwnership(pub Arc<dyn Bucket>);

#[async_trait]
impl OwnershipStore for BucketOwnership {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.0.get(key).await
    }

    async fn replace(&self, key: &str, generation: Option<i64>, bytes: Bytes) -> Result<bool> {
        super::replace(self.0.as_ref(), key, generation, bytes).await
    }
}

pub(crate) async fn confirm_replace(
    store: &dyn OwnershipStore,
    key: &str,
    bytes: &Bytes,
    replaced: Result<bool>,
) -> Result<bool> {
    let Err(error) = replaced else {
        return replaced;
    };
    match store.get(key).await {
        Ok(Some(current)) if current.bytes == *bytes => Ok(true),
        _ => Err(error),
    }
}
