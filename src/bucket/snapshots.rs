use super::{Bucket, replace};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PersistenceConfig {
    #[default]
    Local,
    Replicated {
        replicas: Vec<ReplicaPlacement>,
        #[serde(default)]
        durability: Durability,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaPlacement {
    pub id: String,
    pub address: String,
    pub zone: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    #[default]
    Zonal,
    Regional,
    MultiRegion,
}

impl PersistenceConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        use std::collections::HashSet;
        let Self::Replicated {
            replicas,
            durability,
        } = self
        else {
            return Ok(());
        };
        ensure!(!replicas.is_empty(), "at least one replica is required");
        let mut ids = HashSet::new();
        let mut addresses = HashSet::new();
        let mut zones = HashSet::new();
        let mut regions = HashSet::new();
        for replica in replicas {
            ensure!(
                !replica.id.is_empty()
                    && replica
                        .id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                "invalid replica ID"
            );
            ensure!(ids.insert(&replica.id), "duplicate replica ID");
            let address = reqwest::Url::parse(&replica.address)?;
            ensure!(
                matches!(address.scheme(), "http" | "https")
                    && address.host_str().is_some()
                    && address.username().is_empty()
                    && address.password().is_none()
                    && address.path() == "/"
                    && address.query().is_none()
                    && address.fragment().is_none(),
                "invalid replica address"
            );
            ensure!(
                addresses.insert(address.origin().ascii_serialization()),
                "duplicate replica address"
            );
            let (region, suffix) = replica.zone.rsplit_once('-').ok_or_else(|| {
                anyhow::anyhow!("replica placement must name a Google Cloud zone")
            })?;
            ensure!(
                suffix.len() == 1
                    && suffix.as_bytes()[0].is_ascii_lowercase()
                    && region.chars().last().is_some_and(|c| c.is_ascii_digit()),
                "invalid replica zone"
            );
            zones.insert(&replica.zone);
            regions.insert(region);
        }
        match durability {
            Durability::Zonal => ensure!(zones.len() == 1, "zonal durability requires one zone"),
            Durability::Regional => ensure!(
                zones.len() >= 2 && regions.len() == 1,
                "regional durability requires distinct zones in one region"
            ),
            Durability::MultiRegion => ensure!(
                regions.len() >= 2,
                "multi-region durability requires distinct regions"
            ),
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait SnapshotStore: Send + Sync {
    async fn get(&self, object: &str) -> Result<Option<Bytes>>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>>;
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()>;
    async fn prepare(&self, prefix: &str) -> Result<()>;
    async fn seal(&self, prefix: &str) -> Result<()>;
}

pub(crate) struct BucketSnapshots(pub Arc<dyn Bucket>);

#[async_trait]
impl SnapshotStore for BucketSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        Ok(self
            .0
            .get(object)
            .await?
            .map(|object| Bytes::from(object.bytes)))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.0.list(prefix).await
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let newest = self
            .list(prefix)
            .await?
            .into_iter()
            .filter_map(|key| version(&key).map(|v| (v, key)))
            .max_by_key(|(version, _)| *version)
            .map(|(_, key)| key);
        match newest {
            Some(key) => Ok(self.get(&key).await?.map(|bytes| (key, bytes))),
            None => Ok(None),
        }
    }

    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        ensure!(
            replace(self.0.as_ref(), object, None, bytes.to_vec()).await?,
            "conflicting immutable snapshot"
        );
        Ok(())
    }

    async fn prepare(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn seal(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/persistence.rs"]
mod tests;

pub(super) fn version(object: &str) -> Option<u64> {
    object
        .rsplit('/')
        .next()?
        .strip_suffix(".json")?
        .parse()
        .ok()
}
