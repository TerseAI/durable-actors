use super::*;
use crate::replicas::record::Record;
fn data(v: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"stateVersion":v,"ownerEpoch":1,"requestId":format!("r{v}"),"state":{"count":v},"result":v})).unwrap()
}
fn prefix() -> &'static str {
    "durable-actors/v3/snapshots/aa/cA/YQ/aQ/00000000000000000000000000000001/"
}
#[tokio::test]
async fn acknowledged_records_and_seals_survive_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    let first = Record::encode(1, &data(1), None)?;
    store.append(prefix(), first.clone(), 100).await?;
    store.append(prefix(), first, 100).await?;
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(1));
    store.seal(prefix()).await?;
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert!(store.prepare(prefix()).await.is_err());
    assert!(
        store
            .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
            .await
            .is_err()
    );
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(1));
    Ok(())
}
#[tokio::test]
async fn a_registered_disk_can_fence_an_epoch_that_never_finished_initializing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.seal(prefix()).await?;
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert!(store.latest(prefix()).await?.is_none());
    assert!(store.prepare(prefix()).await.is_err());
    Ok(())
}
#[tokio::test]
async fn batching_retains_records_until_archive_confirmation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    store
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    store
        .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
        .await?;
    assert!(
        store
            .due(10_099, 10_000, 16 * 1024 * 1024)
            .await?
            .is_empty()
    );
    assert_eq!(
        store.due(10_100, 10_000, 16 * 1024 * 1024).await?,
        vec![prefix().to_owned()]
    );
    assert_eq!(store.due(101, 10_000, 1).await?, vec![prefix().to_owned()]);
    let batch = store.batch(prefix(), 16 * 1024 * 1024).await?.unwrap();
    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.decode()?.last().unwrap().1, data(2));
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert!(store.batch(prefix(), 16 * 1024 * 1024).await?.is_some());
    store.archived(&batch.key()?, &batch).await?;
    assert!(store.batch(prefix(), 16 * 1024 * 1024).await?.is_none());
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(2));
    Ok(())
}
#[tokio::test]
async fn concurrent_appends_survive_archival_of_an_earlier_batch() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    store
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    let first = store.batch(prefix(), 1).await?.unwrap();
    store
        .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
        .await?;
    store.archived(&first.key()?, &first).await?;
    let second = store.batch(prefix(), 1).await?.unwrap();
    assert_eq!(second.decode()?, vec![(2, data(2))]);
    Ok(())
}
#[tokio::test]
async fn corrupt_records_cannot_replace_durable_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    store
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    let mut wrong = Record::encode(2, &data(2), Some(&data(1)))?;
    wrong.digest = "invalid".into();
    assert!(store.append(prefix(), wrong, 101).await.is_err());
    assert!(
        store
            .append(prefix(), Record::encode(1, &data(2), None)?, 101)
            .await
            .is_err()
    );
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(1));
    Ok(())
}

#[tokio::test]
async fn replacement_disk_is_seeded_and_old_writers_are_fenced_before_export() -> Result<()> {
    let source_dir = tempfile::tempdir()?;
    let target_dir = tempfile::tempdir()?;
    let source = ReplicaDisk::open(source_dir.path().join("source.sqlite")).await?;
    source.prepare(prefix()).await?;
    source
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    let backup = source.export_sealed().await?;
    assert!(
        source
            .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
            .await
            .is_err()
    );
    let target = ReplicaDisk::open(target_dir.path().join("target.sqlite")).await?;
    target
        .restore(backup, "original-disk-identity".into())
        .await?;
    assert_eq!(target.identity().await?, "original-disk-identity");
    assert_eq!(target.latest(prefix()).await?.unwrap().1, data(1));
    assert!(target.prepare(prefix()).await.is_err());
    assert!(target.batch(prefix(), 16 * 1024 * 1024).await?.is_some());
    assert!(
        target
            .restore(source.export_sealed().await?, "wrong".into())
            .await
            .is_err()
    );
    Ok(())
}
