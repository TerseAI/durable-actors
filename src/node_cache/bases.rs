use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

#[cfg(test)]
use anyhow::Result;
use tokio_util::task::AbortOnDropHandle;

use crate::{actor::ActorKey, bucket::Bucket, storage::SnapshotRef};

/// Uploads at once per node, so a wave of sleeping actors shares the node's network.
const CONCURRENT_UPLOADS: usize = 4;

/// Uploads parked databases as bases, after their hosts have exited and outside any sandbox.
pub(super) struct BaseUploads {
    bucket: Option<Arc<dyn Bucket>>,
    running: tokio::sync::Mutex<HashMap<String, AbortOnDropHandle<()>>>,
    permits: Arc<tokio::sync::Semaphore>,
}

impl BaseUploads {
    pub fn new(bucket: Option<Arc<dyn Bucket>>) -> Self {
        Self {
            bucket,
            running: tokio::sync::Mutex::new(HashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(CONCURRENT_UPLOADS)),
        }
    }

    /// Uploads the database parked at `path` as the actor's base at `snapshot`, unless a host
    /// takes the file back first.
    pub async fn start(&self, actor: &ActorKey, path: PathBuf, snapshot: SnapshotRef, txid: u64) {
        let Some(bucket) = self.bucket.clone() else {
            tracing::warn!(event = "sqlite_base_skipped", "no base bucket configured");
            return;
        };
        let permits = self.permits.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let _permit = permits.acquire_owned().await;
            let started = Instant::now();
            let (uploaded, resources) = crate::process_usage::measure(
                crate::bucket::upload_sqlite_base(bucket.as_ref(), &snapshot, txid, &path),
            )
            .await;
            let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
            match uploaded {
                Ok(bytes) => tracing::info!(
                    event = "sqlite_base_uploaded",
                    state_version = snapshot.state_version,
                    bytes,
                    cpu_ms = resources.cpu_ms,
                    peak_rss_bytes = resources.peak_rss_bytes,
                    duration_ms
                ),
                Err(error) => tracing::warn!(
                    event = "sqlite_base_upload_failed",
                    state_version = snapshot.state_version,
                    error = %format!("{error:#}"),
                    cpu_ms = resources.cpu_ms, peak_rss_bytes = resources.peak_rss_bytes,
                    duration_ms
                ),
            }
        }));
        let mut running = self.running.lock().await;
        running.retain(|_, task| !task.is_finished());
        running.insert(super::actor_digest(actor), task);
    }

    /// Stops any upload of the actor's parked database, so a host can take the file back.
    pub async fn cancel(&self, actor: &ActorKey) {
        let task = self
            .running
            .lock()
            .await
            .remove(&super::actor_digest(actor));
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/bases.rs"]
mod tests;
