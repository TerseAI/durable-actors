use anyhow::Result;
use little_actors::{
    replication::{FileReplicaStore, ReplicaStore, ReplicaStream},
    state_log::StateSnapshot,
};
use serde_json::json;

fn stream() -> ReplicaStream {
    ReplicaStream {
        prefix: "snapshots/epochs/actor/1/".into(),
        session: "snapshots/epochs/sessions/host/one/".into(),
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
    store.initialize_session(&stream.session).await?;
    store.append(&stream, "archive", &snapshot(1)).await?;
    let sealed = store.seal_session(&stream.session).await?;
    assert!(sealed.initialized && sealed.sealed);
    assert_eq!(sealed.streams[0].latest.as_ref().unwrap().state_version, 1);
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
    assert!(store.initialize_session(&stream.session).await.is_err());
    assert_eq!(store.seal_session(&stream.session).await?, sealed);
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
    assert!(store.stream_head(&stream).await.is_err());
    assert!(!store.seal_session(&stream.session).await?.initialized);
    assert!(store.initialize_session(&stream.session).await.is_err());
    Ok(())
}

#[tokio::test]
async fn archived_snapshots_keep_the_recovery_head_and_conflicting_retries_fail() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    let stream = stream();
    store.initialize_session(&stream.session).await?;
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

#[tokio::test]
async fn one_session_initialization_covers_multiple_actors_and_a_seal_fences_them_all() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().into(), 4096).await?;
    let first = stream();
    let second = ReplicaStream {
        prefix: "snapshots/epochs/other/1/".into(),
        ..first.clone()
    };
    store.initialize_session(&first.session).await?;
    store.append(&first, "archive", &snapshot(1)).await?;
    store.append(&second, "archive", &snapshot(2)).await?;
    let sealed = store.seal_session(&first.session).await?;
    assert_eq!(sealed.streams.len(), 2);
    assert!(
        store
            .append(&second, "archive", &snapshot(3))
            .await
            .is_err()
    );
    let later = ReplicaStream {
        prefix: "snapshots/epochs/later/1/".into(),
        ..first
    };
    assert!(store.append(&later, "archive", &snapshot(1)).await.is_err());
    Ok(())
}
