use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use tokio::sync::Mutex;

use super::SnapshotStore;

pub(crate) struct RapidSet {
    copies: Vec<Arc<dyn SnapshotStore>>,
    epochs: Mutex<BTreeMap<String, Arc<Mutex<bool>>>>,
}

impl RapidSet {
    pub fn new(copies: Vec<Arc<dyn SnapshotStore>>) -> Result<Self> {
        ensure!(!copies.is_empty(), "Rapid requires a persistence bucket");
        Ok(Self {
            copies,
            epochs: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn from_config(
        config: &super::PersistenceConfig,
        clients: super::gcs::GcsClients,
    ) -> Result<Self> {
        config.validate()?;
        let super::PersistenceConfig::Rapid { buckets, .. } = config else {
            anyhow::bail!("Rapid persistence configuration required");
        };
        Self::new(
            buckets
                .iter()
                .map(|bucket| {
                    Ok(Arc::new(super::RapidSnapshots::with_clients(
                        &bucket.name,
                        clients.clone(),
                    )?) as Arc<dyn SnapshotStore>)
                })
                .collect::<Result<_>>()?,
        )
    }

    pub async fn verify_placements(
        config: &super::PersistenceConfig,
        clients: &super::gcs::GcsClients,
    ) -> Result<()> {
        config.validate()?;
        let super::PersistenceConfig::Rapid { buckets, .. } = config else {
            anyhow::bail!("production requires Rapid persistence");
        };
        for result in join_all(buckets.iter().map(|bucket| async {
            let actual = clients
                .control
                .get_bucket()
                .set_name(format!("projects/_/buckets/{}", bucket.name))
                .send()
                .await?;
            bucket.validate_placement(&actual)
        }))
        .await
        {
            result?;
        }
        Ok(())
    }

    async fn epoch(&self, prefix: &str) -> Arc<Mutex<bool>> {
        self.epochs
            .lock()
            .await
            .entry(prefix.into())
            .or_insert_with(|| Arc::new(Mutex::new(true)))
            .clone()
    }
}

#[async_trait]
impl SnapshotStore for RapidSet {
    async fn prepare(&self, prefix: &str) -> Result<()> {
        let epoch = self.epoch(prefix).await;
        let mut healthy = epoch.lock().await;
        ensure!(
            *healthy,
            "Rapid epoch has an ambiguous write; recovery is required"
        );
        *healthy = false;
        for result in join_all(self.copies.iter().map(|copy| copy.prepare(prefix))).await {
            result?;
        }
        *healthy = true;
        Ok(())
    }

    async fn seal(&self, prefix: &str) -> Result<()> {
        let epoch = self.epoch(prefix).await;
        let mut healthy = epoch.lock().await;
        *healthy = false;
        let results = join_all(self.copies.iter().map(|copy| copy.seal(prefix))).await;
        ensure!(
            results.iter().any(Result::is_ok),
            "no Rapid copy could be sealed for recovery"
        );
        Ok(())
    }

    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let (prefix, _) = object.rsplit_once('/').context("Rapid epoch missing")?;
        let epoch = self.epoch(&format!("{prefix}/")).await;
        let mut healthy = epoch.lock().await;
        ensure!(
            *healthy,
            "Rapid epoch has an ambiguous write; recovery is required"
        );
        // Cancellation must fence subsequent writes even when a remote copy persisted the record.
        *healthy = false;
        for result in join_all(
            self.copies
                .iter()
                .map(|copy| copy.put(object, bytes.clone())),
        )
        .await
        {
            result?;
        }
        *healthy = true;
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        let mut available = false;
        let mut selected = None;
        for result in join_all(self.copies.iter().map(|copy| copy.get(object))).await {
            let Ok(bytes) = result else {
                continue;
            };
            available = true;
            if let Some(bytes) = bytes {
                ensure!(
                    selected.as_ref().is_none_or(|previous| previous == &bytes),
                    "conflicting Rapid copies"
                );
                selected = Some(bytes);
            }
        }
        ensure!(available, "no Rapid copy is available for recovery");
        Ok(selected)
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let mut available = false;
        let mut selected: Option<(String, Bytes)> = None;
        for result in join_all(self.copies.iter().map(|copy| copy.latest(prefix))).await {
            let Ok(candidate) = result else {
                continue;
            };
            available = true;
            if let Some(candidate) = candidate {
                if let Some(previous) = &selected {
                    ensure!(
                        previous.0 != candidate.0 || previous.1 == candidate.1,
                        "conflicting Rapid copies"
                    );
                    if super::snapshots::version(&previous.0)
                        >= super::snapshots::version(&candidate.0)
                    {
                        continue;
                    }
                }
                selected = Some(candidate);
            }
        }
        ensure!(available, "no Rapid copy is available for recovery");
        Ok(selected)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut available = false;
        let mut keys = BTreeSet::new();
        for result in join_all(self.copies.iter().map(|copy| copy.list(prefix))).await {
            if let Ok(copy) = result {
                available = true;
                keys.extend(copy);
            }
        }
        ensure!(available, "no Rapid copy is available for recovery");
        Ok(keys.into_iter().collect())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/rapid_set.rs"]
mod tests;
