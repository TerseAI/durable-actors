use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use little_actors::{
    replication::{FileReplicaStore, ReplicaStore, ReplicatedStateTransport},
    state_transport::{SnapshotWriter, StateTransport, StateWrite},
    storage::WritePlan,
};
use tokio::sync::Semaphore;

struct Storage {
    bucket: Semaphore,
    second: Semaphore,
    second_finished: Semaphore,
}

#[async_trait]
impl StateTransport for Storage {
    async fn read(&self, _: &str) -> Result<Bytes> {
        anyhow::bail!("not uploaded")
    }

    async fn write(&self, url: &str, _: Vec<u8>) -> Result<StateWrite> {
        match url {
            "bucket" => {
                self.bucket.acquire().await?.forget();
            }
            "second" => {
                self.second.acquire().await?.forget();
                self.second_finished.add_permits(1);
            }
            _ => {}
        }
        Ok(StateWrite::Written)
    }
}

#[async_trait]
impl SnapshotWriter for Storage {
    async fn write_snapshot(&self, _: &WritePlan, bytes: Vec<u8>) -> Result<StateWrite> {
        self.write("bucket", bytes).await
    }
}

#[tokio::test]
async fn all_remote_replicas_can_commit_without_actor_disk_or_bucket() -> Result<()> {
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(0),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(0),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), storage.clone());
    let ticket = ticket();
    let mut write = Box::pin(transport.write_snapshot(&ticket, b"snapshot".to_vec()));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut write)
            .await
            .is_err()
    );
    storage.second.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), write).await??,
        StateWrite::Replicated
    );
    Ok(())
}

#[tokio::test]
async fn bucket_can_commit_while_a_replica_is_unavailable() -> Result<()> {
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(1),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(0),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), storage);
    assert_eq!(
        transport
            .write_snapshot(&ticket(), b"snapshot".to_vec())
            .await?,
        StateWrite::Written
    );
    Ok(())
}

#[tokio::test]
async fn failed_replica_is_reported_for_repair_even_when_gcs_commits_first() -> Result<()> {
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(1),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(0),
    });
    let (failures, mut reports) = tokio::sync::mpsc::unbounded_channel();
    let transport = ReplicatedStateTransport::new(storage.clone(), storage.clone())
        .with_failure_reports(failures);
    assert_eq!(
        transport
            .write_snapshot(&ticket(), b"snapshot".to_vec())
            .await?,
        StateWrite::Written
    );
    storage.second.close();
    let failed = tokio::time::timeout(Duration::from_millis(100), reports.recv()).await?;
    assert_eq!(
        failed.as_deref(),
        Some(ticket().replication.unwrap().replicas[1].host_id.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn replica_snapshot_survives_reopening_and_rejects_conflicting_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("snapshots");
    let store = FileReplicaStore::open(path.clone(), 1024).await?;
    store.put("snapshot", b"first").await?;
    store.put("snapshot", b"first").await?;
    assert!(store.put("snapshot", b"different").await.is_err());
    drop(store);
    let reopened = FileReplicaStore::open(path, 1024).await?;
    assert_eq!(reopened.read("snapshot").await?, Some(b"first".to_vec()));
    Ok(())
}

#[tokio::test]
async fn replica_blobs_use_a_directory_and_restore_capacity_after_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().to_owned(), 5).await?;
    assert!(
        FileReplicaStore::open(directory.path().to_owned(), 5)
            .await
            .is_err()
    );
    store.put("../snapshot", b"first").await?;
    assert_eq!(store.read("../snapshot").await?, Some(b"first".to_vec()));
    assert!(store.put("second", b"x").await.is_err());
    drop(store);

    let reopened = FileReplicaStore::open(directory.path().to_owned(), 5).await?;
    assert_eq!(reopened.read("../snapshot").await?, Some(b"first".to_vec()));
    assert!(reopened.put("second", b"x").await.is_err());
    Ok(())
}

#[tokio::test]
async fn concurrent_blob_writes_cannot_exceed_capacity_or_replace_a_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(FileReplicaStore::open(directory.path().to_owned(), 5).await?);
    let (first, second) =
        tokio::join!(store.put("first", b"first"), store.put("second", b"other"),);
    assert_ne!(first.is_ok(), second.is_ok());
    let (object, bytes) = if first.is_ok() {
        ("first", b"first")
    } else {
        ("second", b"other")
    };
    assert!(store.put(object, b"wrong").await.is_err());
    assert_eq!(store.read(object).await?, Some(bytes.to_vec()));
    Ok(())
}

#[tokio::test]
async fn replica_restart_ignores_unpublished_writes_and_rejects_truncated_blobs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().to_owned(), 1024).await?;
    let bytes = b"{\n  \"count\": 1\n}\n";
    store.put("snapshot", bytes).await?;
    drop(store);

    let blob = std::fs::read_dir(directory.path())?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "blob")
        })
        .expect("snapshot blob");
    let stored = std::fs::read(&blob)?;
    let header_end = stored.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    assert_eq!(&stored[header_end..], bytes);
    std::fs::write(directory.path().join(".pending-interrupted"), b"partial")?;

    let reopened = FileReplicaStore::open(directory.path().to_owned(), 1024).await?;
    assert_eq!(reopened.read("snapshot").await?, Some(bytes.to_vec()));
    assert!(!directory.path().join(".pending-interrupted").exists());
    drop(reopened);
    std::fs::write(blob, &stored[..stored.len() - 1])?;
    assert!(
        FileReplicaStore::open(directory.path().to_owned(), 1024)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn a_duplicate_host_cannot_satisfy_two_replica_acknowledgments() -> Result<()> {
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(0),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(1),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), storage);
    let mut invalid = ticket();
    invalid.replication.as_mut().unwrap().replicas[1].host_id = "one".into();
    assert!(
        transport
            .write_snapshot(&invalid, b"snapshot".to_vec())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn a_partial_replica_set_cannot_commit_when_the_bucket_fails() -> Result<()> {
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(0),
        second: Semaphore::new(0),
        second_finished: Semaphore::new(0),
    });
    storage.bucket.close();
    storage.second.close();
    let transport = ReplicatedStateTransport::new(storage.clone(), storage);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            transport.write_snapshot(&ticket(), b"snapshot".to_vec())
        )
        .await?
        .is_err()
    );
    Ok(())
}

fn ticket() -> WritePlan {
    WritePlan {
        stream: little_actors::replication::ReplicaStream {
            prefix: "snapshots/epoch/".into(),
            session: "session".into(),
            owner_epoch: 1,
            base_version: 0,
        },
        state_version: 1,
        object_name: "snapshot".into(),
        expires_at_ms: i64::MAX,
        replication: Some(
            serde_json::from_value(serde_json::json!({
                "replicas": [
                    { "hostId": "one", "url": "first" },
                    { "hostId": "two", "url": "second" }
                ]
            }))
            .unwrap(),
        ),
    }
}

#[tokio::test]
async fn replica_grpc_ack_is_readable_after_restart_and_bound_to_one_node() -> Result<()> {
    use little_actors::{
        clock::SystemClock,
        replication::{ReplicaAccess, ReplicaGrant, replica_routes},
    };
    let directory = tempfile::tempdir()?;
    let store = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 4096).await?);
    let access = ReplicaAccess::new("test-installation-key", Arc::new(SystemClock));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let scope = little_actors::replication::ReplicaScope {
        actor: little_actors::actor::ActorKey {
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        },
        host: little_actors::host::HostId::new("primary"),
        session: "session".into(),
        region: "us-east".into(),
    };
    let router = replica_routes(
        store.clone(),
        access.clone(),
        "node-a".into(),
        scope.clone(),
    );
    let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let object = little_actors::storage::snapshot_object_name(
        &scope.actor,
        1,
        "00000000000000000000000000000001",
    )?;
    store.initialize_session(&scope.identity()).await?;
    let stream = little_actors::replication::ReplicaStream {
        session: scope.identity(),
        prefix: object.strip_suffix("1.json").unwrap().into(),
        owner_epoch: 1,
        base_version: 0,
    };
    let grant = ReplicaGrant {
        stream: Some(stream.clone()),
        operation: "APPEND".into(),
        object: stream.prefix.clone(),
        region: "us-east".into(),
        host_id: "node-a".into(),
        expires_at_ms: u64::MAX,
    };
    let bytes = little_actors::state_log::StateSnapshot::new(
        1,
        1,
        "request".into(),
        serde_json::json!({"count":1}),
        serde_json::json!(1),
    )?
    .encode()?;
    let transport = little_actors::state_transport::GrpcStateTransport::new();
    let write = access.url(&origin, &grant)?;
    assert_eq!(
        transport.write(&write, bytes.clone()).await?,
        StateWrite::Written
    );
    let read = access.url(
        &origin,
        &ReplicaGrant {
            operation: "GET".into(),
            object: object.clone(),
            stream: None,
            ..grant.clone()
        },
    )?;
    assert_eq!(transport.read(&read).await?.as_ref(), bytes);
    let wrong_node = access.url(
        &origin,
        &ReplicaGrant {
            host_id: "node-b".into(),
            ..grant.clone()
        },
    )?;
    let wrong_activation = access.url(
        &origin,
        &ReplicaGrant {
            stream: Some(little_actors::replication::ReplicaStream {
                session: format!("{}other/", scope.identity()),
                ..stream
            }),
            ..grant.clone()
        },
    )?;
    let wrong_actor = access.url(
        &origin,
        &ReplicaGrant {
            operation: "GET".into(),
            stream: None,
            object: little_actors::storage::snapshot_object_name(
                &little_actors::actor::ActorKey {
                    actor_type: "Counter".into(),
                    actor_id: "other".into(),
                },
                1,
                "00000000000000000000000000000001",
            )?,
            ..grant.clone()
        },
    )?;
    let obsolete_put = access.url(
        &origin,
        &ReplicaGrant {
            operation: "PUT".into(),
            stream: None,
            object: object.clone(),
            ..grant
        },
    )?;
    for error in [
        transport
            .write(&wrong_activation, bytes.clone())
            .await
            .unwrap_err(),
        transport.read(&wrong_actor).await.unwrap_err(),
        transport
            .write(&obsolete_put, bytes.clone())
            .await
            .unwrap_err(),
        transport
            .write(&wrong_node, bytes.clone())
            .await
            .unwrap_err(),
        transport.read(&write).await.unwrap_err(),
    ] {
        assert_eq!(
            error.downcast_ref::<tonic::Status>().unwrap().code(),
            tonic::Code::PermissionDenied
        );
    }
    let _ = shutdown.send(());
    server.await??;
    drop(store);
    assert_eq!(
        FileReplicaStore::open(directory.path().join("snapshots"), 4096)
            .await?
            .read(&object)
            .await?,
        Some(bytes)
    );
    Ok(())
}

#[tokio::test]
async fn replica_uploads_continue_after_object_storage_wins() -> Result<()> {
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(1),
        second: Semaphore::new(0),
        second_finished: Semaphore::new(0),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), storage.clone());
    assert_eq!(
        transport
            .write_snapshot(&ticket(), b"snapshot".to_vec())
            .await?,
        StateWrite::Written
    );
    storage.second.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), storage.second_finished.acquire())
        .await??
        .forget();
    Ok(())
}
