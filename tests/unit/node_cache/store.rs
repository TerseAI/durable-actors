use super::*;
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn least_recently_used_files_are_evicted_past_capacity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = DiskStore::open(root.path().into(), 8, u64::MAX).await?;
    for key in ["code/a", "code/b"] {
        insert(&store, key).await?;
    }
    store.entries.run_pending_tasks().await;
    assert!(store.get("code/a").await.is_some());

    insert(&store, "code/c").await?;
    store.entries.run_pending_tasks().await;

    assert!(store.get("code/a").await.is_some());
    assert!(store.get("code/b").await.is_none());
    assert!(!root.path().join("code/b").exists());
    assert_eq!(
        DiskStore::open(root.path().into(), 8, u64::MAX)
            .await?
            .existing()
            .await?
            .len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn small_files_are_evicted_past_the_entry_limit_with_bytes_to_spare() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = DiskStore::open(root.path().into(), 1 << 20, 2).await?;
    for key in ["sqlite/a", "sqlite/b", "sqlite/c"] {
        insert(&store, key).await?;
        store.entries.run_pending_tasks().await;
    }

    assert_eq!(store.entries.entry_count(), 2);
    assert!(store.get("sqlite/a").await.is_none());
    assert!(!root.path().join("sqlite/a").exists());
    assert!(store.get("sqlite/c").await.is_some());
    Ok(())
}

#[tokio::test]
async fn reopening_with_fewer_entries_keeps_the_newest_files() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = DiskStore::open(root.path().into(), 1 << 20, 3).await?;
    for key in ["code/a", "code/b", "code/c"] {
        insert(&store, key).await?;
    }
    drop(store);
    let now = SystemTime::now();
    for (key, age) in [("code/a", 1), ("code/b", 2), ("code/c", 3)] {
        std::fs::File::options()
            .write(true)
            .open(root.path().join(key))?
            .set_modified(now - Duration::from_secs(age * 60))?;
    }

    let store = DiskStore::open(root.path().into(), 1 << 20, 2).await?;
    store.entries.run_pending_tasks().await;

    assert!(store.get("code/c").await.is_none());
    assert!(!root.path().join("code/c").exists());
    assert!(store.get("code/a").await.is_some());
    assert!(store.get("code/b").await.is_some());
    Ok(())
}

async fn insert(store: &DiskStore, key: &str) -> Result<()> {
    let staged = store.stage()?;
    std::fs::write(staged.path(), b"four")?;
    store.insert(key, staged, 4).await
}
