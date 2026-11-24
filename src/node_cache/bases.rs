use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use tokio_util::task::AbortOnDropHandle;

use crate::{actor::ActorKey, bucket::Bucket, storage::SnapshotRef};

/// Uploads at once per node, so a wave of sleeping actors shares the node's network.
const CONCURRENT_UPLOADS: usize = 4;

/// Uploads parked databases as bases, after their hosts have exited and outside any sandbox.
/// Each upload reads its own copy, so a host can take its file back without stopping it: an
/// actor that wakes more often than its base takes to upload would otherwise never get one.
pub(super) struct BaseUploads {
    bucket: Option<Arc<dyn Bucket>>,
    staging: PathBuf,
    running: tokio::sync::Mutex<HashMap<String, Upload>>,
    permits: Arc<tokio::sync::Semaphore>,
}

struct Upload {
    /// Turns true once the upload's copy is made.
    copied: tokio::sync::watch::Receiver<bool>,
    _task: AbortOnDropHandle<()>,
}

impl BaseUploads {
    /// Uploads to `bucket`, copying parked databases into `staging` first.
    pub fn new(bucket: Option<Arc<dyn Bucket>>, staging: PathBuf) -> Self {
        Self {
            bucket,
            staging,
            running: tokio::sync::Mutex::new(HashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(CONCURRENT_UPLOADS)),
        }
    }

    /// Uploads the database parked at `path` as the actor's base at `snapshot`, replacing any
    /// older upload of the actor's.
    pub async fn start(&self, actor: &ActorKey, path: PathBuf, snapshot: SnapshotRef, txid: u64) {
        let Some(bucket) = self.bucket.clone() else {
            tracing::warn!(event = "sqlite_base_skipped", "no base bucket configured");
            return;
        };
        let (copied, waiting) = tokio::sync::watch::channel(false);
        let (staging, permits) = (self.staging.clone(), self.permits.clone());
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let copy = copy(&path, &staging).await;
            let _ = copied.send(true);
            let copy = match copy {
                Ok(copy) => copy,
                Err(error) => {
                    tracing::warn!(event = "sqlite_base_upload_failed", error = %format!("{error:#}"));
                    return;
                }
            };
            let _permit = permits.acquire_owned().await;
            upload(bucket.as_ref(), &copy, &snapshot, txid).await;
        }));
        let mut running = self.running.lock().await;
        running.retain(|_, upload| !upload._task.is_finished());
        running.insert(
            super::actor_digest(actor),
            Upload {
                copied: waiting,
                _task: task,
            },
        );
    }

    /// Waits until the actor's upload, if any, has its copy, so a host can take the file back.
    pub async fn copied(&self, actor: &ActorKey) {
        let waiting = self
            .running
            .lock()
            .await
            .get(&super::actor_digest(actor))
            .map(|upload| upload.copied.clone());
        if let Some(mut waiting) = waiting {
            let _ = waiting.wait_for(|copied| *copied).await;
        }
    }

    /// Stops the actor's upload, when a newer park supersedes it.
    pub async fn cancel(&self, actor: &ActorKey) {
        self.running
            .lock()
            .await
            .remove(&super::actor_digest(actor));
    }
}

/// A copy of the parked database in `staging`, deleted when dropped.
async fn copy(path: &Path, staging: &Path) -> Result<tempfile::TempPath> {
    let (path, staging) = (path.to_owned(), staging.to_owned());
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&staging)?;
        let copy = tempfile::NamedTempFile::new_in(&staging)?.into_temp_path();
        std::fs::copy(&path, &copy)?;
        Ok(copy)
    })
    .await?
}

async fn upload(bucket: &dyn Bucket, copy: &Path, snapshot: &SnapshotRef, txid: u64) {
    let started = Instant::now();
    let (uploaded, resources) = crate::process_usage::measure(crate::bucket::upload_sqlite_base(
        bucket, snapshot, txid, copy,
    ))
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
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/bases.rs"]
mod tests;
