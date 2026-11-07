use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use moka::{future::Cache, notification::RemovalCause, policy::EvictionPolicy};

const STAGING: &str = "staging";

/// Files on local disk, evicted least-recently-used once their total size passes the capacity.
pub(super) struct DiskStore {
    root: PathBuf,
    entries: Cache<String, u64>,
}

impl DiskStore {
    pub async fn open(root: PathBuf, capacity_bytes: u64) -> Result<Self> {
        let staging = root.join(STAGING);
        if tokio::fs::try_exists(&staging).await? {
            tokio::fs::remove_dir_all(&staging).await?;
        }
        tokio::fs::create_dir_all(&staging).await?;
        let store = Self {
            entries: entries(root.clone(), capacity_bytes),
            root,
        };
        for (key, size) in store.existing().await? {
            store.entries.insert(key, size).await;
        }
        Ok(store)
    }

    pub async fn get(&self, key: &str) -> Option<(tokio::fs::File, u64)> {
        let size = self.entries.get(key).await?;
        match tokio::fs::File::open(self.root.join(key)).await {
            Ok(file) => Some((file, size)),
            Err(_) => {
                self.entries.invalidate(key).await;
                None
            }
        }
    }

    pub fn stage(&self) -> Result<tempfile::NamedTempFile> {
        Ok(tempfile::NamedTempFile::new_in(self.root.join(STAGING))?)
    }

    pub async fn insert(
        &self,
        key: &str,
        staged: tempfile::NamedTempFile,
        size: u64,
    ) -> Result<()> {
        let path = self.root.join(key);
        tokio::fs::create_dir_all(path.parent().context("cache entry has no parent")?).await?;
        staged.persist(&path)?;
        self.entries.insert(key.to_owned(), size).await;
        Ok(())
    }

    async fn existing(&self) -> Result<Vec<(String, u64)>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let mut found = Vec::new();
            for entry in walkdir::WalkDir::new(&root).min_depth(1) {
                let entry = entry?;
                let key = entry.path().strip_prefix(&root)?;
                if entry.file_type().is_file() && !key.starts_with(STAGING) {
                    let key = key.to_str().context("cache key is not UTF-8")?.to_owned();
                    found.push((key, entry.metadata()?.len()));
                }
            }
            anyhow::Ok(found)
        })
        .await?
    }
}

fn entries(root: PathBuf, capacity_bytes: u64) -> Cache<String, u64> {
    Cache::builder()
        .max_capacity(capacity_bytes)
        .weigher(|_, size: &u64| u32::try_from(*size).unwrap_or(u32::MAX))
        .eviction_policy(EvictionPolicy::lru())
        .eviction_listener(move |key: Arc<String>, _, cause| {
            // A replacement has already renamed its own file over this one.
            if cause != RemovalCause::Replaced {
                let _ = std::fs::remove_file(root.join(key.as_str()));
            }
        })
        .build()
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/store.rs"]
mod tests;
