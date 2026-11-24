pub(crate) mod access;
mod file;
mod gcs;
mod rapid;
pub(crate) use rapid::{RAPID_OPERATION_TIMEOUT, RapidSnapshots};
pub(crate) mod recovery;
mod snapshots;
#[cfg(test)]
pub(crate) use rapid::base::key as base_key;
pub(crate) use rapid::base::{SqliteBase, upload_file as upload_sqlite_base};
pub use rapid::sweep::{SweepConfig, serve as serve_history_sweeper};
pub(crate) use recovery::RestoredSqlite;
pub use snapshots::{ArchiveBatchConfig, RapidBucket, RapidPlacement, SnapshotStoreConfig};
pub(crate) use snapshots::{BucketSnapshots, SnapshotStore};
mod runtime;

use anyhow::Result;
use async_trait::async_trait;

pub use crate::payload::Payload;
pub use file::FileBucket;
pub use gcs::GcsBucket;
pub(crate) use gcs::WarmStorage;
pub(crate) use runtime::{ActorStateReader, CachedBase};
pub use runtime::{LoadedActor, OwnershipHint, RuntimeStorage, RuntimeStorageReader};

#[derive(Clone, Debug)]
pub struct BucketObject {
    pub generation: i64,
    pub bytes: Payload,
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>>;
    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>>;
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Payload,
    ) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Writes the object to a new file at `path` and returns its size, or `None` when missing.
    async fn download_to(&self, key: &str, path: &std::path::Path) -> Result<Option<u64>> {
        let Some(object) = self.get(key).await? else {
            return Ok(None);
        };
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::create_new(path)?;
            Ok(Some(std::io::copy(&mut object.bytes.reader(), &mut file)?))
        })
        .await?
    }
    async fn download_checked(
        &self,
        key: &str,
        path: &std::path::Path,
        expected: Option<u32>,
    ) -> Result<Option<u64>> {
        let bytes = self.download_to(key, path).await?;
        if let (Some(_), Some(expected)) = (bytes, expected) {
            let path = path.to_owned();
            let actual =
                tokio::task::spawn_blocking(move || file_crc32c(&std::fs::File::open(path)?))
                    .await??;
            anyhow::ensure!(actual == expected, "base CRC32C mismatch");
        }
        Ok(bytes)
    }
}

fn file_crc32c(file: &std::fs::File) -> Result<u32> {
    use std::os::unix::fs::FileExt;
    let mut buffer = vec![0; crate::payload::IO_BUFFER_BYTES];
    let (mut offset, mut crc) = (0, 0);
    loop {
        let size = file.read_at(&mut buffer, offset)?;
        if size == 0 {
            return Ok(crc);
        }
        crc = crc32c::crc32c_append(crc, &buffer[..size]);
        offset += size as u64;
    }
}

pub(crate) async fn replace(
    bucket: &dyn Bucket,
    key: &str,
    generation: Option<i64>,
    bytes: Payload,
) -> Result<bool> {
    match bucket
        .compare_and_swap(key, generation, bytes.clone())
        .await
    {
        Ok(true) => Ok(true),
        result => {
            // A retry can report a failed precondition after the first request succeeded.
            if bucket
                .get(key)
                .await?
                .is_some_and(|object| object.bytes == bytes)
            {
                return Ok(true);
            }
            result
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/bucket.rs"]
pub(crate) mod testing;
