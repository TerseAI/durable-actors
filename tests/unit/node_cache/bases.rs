use super::*;
use crate::bucket::{BucketObject, FileBucket};

const SNAPSHOT: &str = "durable-actors/v3/snapshots/63/cHJvamVjdA/Q291bnRlcg/aWQ/000000000000000000000000000000a1/42.json";

fn snapshot() -> SnapshotRef {
    SnapshotRef {
        object: SNAPSHOT.into(),
        state_version: 42,
        request_id: "request-42".into(),
        digest: "digest".into(),
    }
}

fn actor() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "id".into(),
    }
}

#[tokio::test]
async fn uploads_a_parked_database_under_its_base_name() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(FileBucket::new(directory.path().join("bucket"))?);
    let parked = directory.path().join("actor.sqlite");
    std::fs::write(&parked, b"pages of a parked database")?;
    let uploads = BaseUploads::new(Some(bucket.clone()));
    uploads.start(&actor(), parked, snapshot(), 7).await;
    let key = crate::bucket::base_key(&snapshot(), 7, 26)?;
    let stored = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(object) = bucket.get(&key).await? {
                return anyhow::Ok(object);
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await??;
    assert_eq!(&stored.bytes.to_bytes()?[..], b"pages of a parked database");
    Ok(())
}

/// A bucket whose uploads never finish.
struct Stuck(std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl Bucket for Stuck {
    async fn get(&self, _: &str) -> Result<Option<BucketObject>> {
        Ok(None)
    }
    async fn range(&self, _: &str, _: u64, _: u64) -> Result<Option<BucketObject>> {
        Ok(None)
    }
    async fn compare_and_swap(&self, _: &str, _: Option<i64>, _: Payload) -> Result<bool> {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        std::future::pending().await
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn a_host_taking_the_file_back_stops_its_upload() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let parked = directory.path().join("actor.sqlite");
    std::fs::write(&parked, b"pages")?;
    let bucket = Arc::new(Stuck(Default::default()));
    let uploads = BaseUploads::new(Some(bucket.clone()));
    uploads.start(&actor(), parked, snapshot(), 7).await;
    while !bucket.0.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(1), uploads.cancel(&actor())).await?;
    assert!(uploads.running.lock().await.is_empty());
    Ok(())
}
