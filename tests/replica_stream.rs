use anyhow::Result;
use little_actors::{
    replication::{FileReplicaStore, ReplicaStore, ReplicaStream},
    state_log::StateSnapshot,
};
use serde_json::json;

fn stream() -> ReplicaStream {
    ReplicaStream {
        prefix: "snapshots/epochs/actor/1/".into(),
        owner_epoch: 1,
        base_version: 0,
    }
}

fn snapshot(version: u64) -> Vec<u8> {
    StateSnapshot::new(
        version,
        1,
        format!("r{version}"),
        json!({"value": version}),
        json!(version),
    )
    .unwrap()
    .encode()
    .unwrap()
}

#[tokio::test]
async fn a_seal_survives_restart_and_rejects_delayed_writes_and_reinitialization() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    let stream = stream();
    store.initialize_stream(&stream).await?;
    store.append(&stream, "archive", &snapshot(1)).await?;
    let sealed = store.seal(&stream).await?;
    assert!(sealed.initialized && sealed.sealed);
    assert_eq!(sealed.latest.as_ref().unwrap().state_version, 1);
    drop(store);
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    assert!(
        store
            .append(&stream, "archive", &snapshot(1))
            .await
            .is_err()
    );
    assert!(
        store
            .append(&stream, "archive", &snapshot(2))
            .await
            .is_err()
    );
    assert!(store.initialize_stream(&stream).await.is_err());
    assert_eq!(store.seal(&stream).await?, sealed);
    Ok(())
}

#[tokio::test]
async fn missing_streams_are_not_recovery_witnesses_and_cannot_be_initialized_after_sealing()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    let stream = stream();
    assert!(
        store
            .append(&stream, "archive", &snapshot(1))
            .await
            .is_err()
    );
    assert!(!store.seal(&stream).await?.initialized);
    assert!(store.initialize_stream(&stream).await.is_err());
    Ok(())
}

#[tokio::test]
async fn archived_snapshots_keep_the_recovery_head_and_conflicting_retries_fail() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    let stream = stream();
    store.initialize_stream(&stream).await?;
    store.append(&stream, "archive", &snapshot(1)).await?;
    store.append(&stream, "archive", &snapshot(1)).await?;
    let conflicting =
        StateSnapshot::new(1, 1, "other".into(), json!({"value": 50}), json!(50))?.encode()?;
    assert!(
        store
            .append(&stream, "archive", &conflicting)
            .await
            .is_err()
    );
    let head = store.stream_head(&stream).await?;
    store
        .archived(&head.latest.as_ref().unwrap().object)
        .await?;
    drop(store);
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    assert_eq!(store.stream_head(&stream).await?, head);
    assert!(
        store
            .append(&stream, "archive", &conflicting)
            .await
            .is_err()
    );
    Ok(())
}
