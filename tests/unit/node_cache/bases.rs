use super::*;
use crate::bucket::{BucketObject, Payload};
use anyhow::Result;

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
async fn a_host_takes_its_file_back_once_copied_and_the_upload_goes_on() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let parked = directory.path().join("actor.sqlite");
    std::fs::write(&parked, b"pages")?;
    let bucket = Arc::new(Stuck(Default::default()));
    let uploads = BaseUploads::new(Some(bucket.clone()), directory.path().join("staging"));
    uploads.start(&actor(), parked.clone(), snapshot(), 7).await;
    if let Some(mut copying) = uploads.copying(&actor()).await {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            copying.wait_for(|copied| *copied),
        )
        .await??;
    }
    assert!(uploads.copying(&actor()).await.is_none());
    // The host rewrites its file; the upload reads its own copy.
    std::fs::write(&parked, b"other pages")?;
    while !bucket.0.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    let copies: Vec<_> = std::fs::read_dir(directory.path().join("staging"))?.collect();
    assert_eq!(copies.len(), 1);
    assert_eq!(std::fs::read(copies[0].as_ref().unwrap().path())?, b"pages");
    uploads.cancel(&actor()).await;
    Ok(())
}
