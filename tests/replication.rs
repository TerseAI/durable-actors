use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use little_actors::{
    replication::{
        FileReplicaStore, ReplicaStore, ReplicaTarget, ReplicatedStateTransport, ReplicationTicket,
    },
    state_transport::{StateTransport, StateWrite},
    storage_urls::StateWriteTicket,
};
use tokio::sync::Semaphore;

#[test]
fn installation_policy_defaults_to_bucket_and_supports_only_explicit_zonal_replication()
-> Result<()> {
    use little_actors::replication::replica_count;
    assert_eq!(replica_count(&mut |_| None)?, 0);
    let config = |mode: &str, count: &str| {
        replica_count(&mut |name| match name {
            "DURABLE_OBJECT_DURABILITY" => Some(mode.into()),
            "DURABLE_OBJECT_REPLICA_COUNT" => Some(count.into()),
            _ => None,
        })
    };
    assert_eq!(config("zonal", "2")?, 2);
    assert_eq!(config("zonal", "3")?, 3);
    assert!(config("regional", "2").is_err());
    assert!(config("multi-region", "2").is_err());
    assert!(config("zonal", "0").is_err());
    assert!(config("zonal", "9").is_err());
    assert!(config("object_storage", "2").is_err());
    Ok(())
}

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

#[tokio::test]
async fn both_replicas_and_a_local_disk_copy_can_finish_before_the_bucket() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let local = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 1024).await?);
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(0),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(0),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), local.clone());
    let ticket = ticket();
    let mut write = Box::pin(transport.write_ticket(&ticket, b"snapshot".to_vec()));
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
    assert_eq!(local.read("snapshot").await?, Some(b"snapshot".to_vec()));
    Ok(())
}

#[tokio::test]
async fn bucket_can_commit_while_a_replica_is_unavailable() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let local = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 1024).await?);
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(1),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(0),
    });
    let transport = ReplicatedStateTransport::new(storage, local);
    assert_eq!(
        transport
            .write_ticket(&ticket(), b"snapshot".to_vec())
            .await?,
        StateWrite::Written
    );
    Ok(())
}

#[tokio::test]
async fn replica_snapshot_survives_reopening_and_rejects_conflicting_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("snapshots");
    let store = FileReplicaStore::open(path.clone(), 1024).await?;
    store.put("snapshot", "archive", b"first").await?;
    store.put("snapshot", "archive", b"first").await?;
    assert!(
        store
            .put("snapshot", "archive", b"different")
            .await
            .is_err()
    );
    drop(store);
    let reopened = FileReplicaStore::open(path, 1024).await?;
    assert_eq!(reopened.read("snapshot").await?, Some(b"first".to_vec()));
    assert_eq!(reopened.pending(10).await?.len(), 1);
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
    store.put("../snapshot", "archive", b"first").await?;
    assert_eq!(store.read("../snapshot").await?, Some(b"first".to_vec()));
    assert!(store.put("second", "archive", b"x").await.is_err());
    drop(store);

    let reopened = FileReplicaStore::open(directory.path().to_owned(), 5).await?;
    let pending = reopened.pending(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].object, "../snapshot");
    assert_eq!(pending[0].archive_url, "archive");
    assert_eq!(pending[0].bytes, b"first");
    assert!(reopened.put("second", "archive", b"x").await.is_err());
    reopened.archived("../snapshot").await?;
    reopened.put("second", "archive", b"other").await?;
    drop(reopened);

    let reopened = FileReplicaStore::open(directory.path().to_owned(), 5).await?;
    assert!(reopened.read("../snapshot").await?.is_none());
    assert_eq!(reopened.read("second").await?, Some(b"other".to_vec()));
    Ok(())
}

#[tokio::test]
async fn concurrent_blob_writes_cannot_exceed_capacity_or_replace_a_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(FileReplicaStore::open(directory.path().to_owned(), 5).await?);
    let (first, second) = tokio::join!(
        store.put("first", "archive", b"first"),
        store.put("second", "archive", b"other"),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let pending = store.pending(10).await?;
    assert_eq!(pending.len(), 1);
    assert!(
        store
            .put(&pending[0].object, "archive", b"wrong")
            .await
            .is_err()
    );
    assert_eq!(
        store.read(&pending[0].object).await?,
        Some(pending[0].bytes.clone())
    );
    Ok(())
}

#[tokio::test]
async fn replica_restart_ignores_unpublished_writes_and_rejects_truncated_blobs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().to_owned(), 1024).await?;
    let bytes = b"{\n  \"count\": 1\n}\n";
    store.put("snapshot", "archive", bytes).await?;
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
    assert_eq!(reopened.pending(10).await?.len(), 1);
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
async fn archive_retries_do_not_starve_unattempted_blobs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().to_owned(), 1024).await?;
    for index in 0..6 {
        store
            .put(&index.to_string(), "archive", b"snapshot")
            .await?;
    }
    let attempted = store.pending(4).await?;
    for snapshot in &attempted {
        store.attempted(&snapshot.object).await?;
    }
    let next = store.pending(2).await?;
    assert_eq!(next.len(), 2);
    assert!(next.iter().all(|snapshot| {
        attempted
            .iter()
            .all(|previous| previous.object != snapshot.object)
    }));
    Ok(())
}

#[tokio::test]
async fn a_duplicate_host_cannot_satisfy_two_replica_acknowledgments() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let local = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 1024).await?);
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(0),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(1),
    });
    let transport = ReplicatedStateTransport::new(storage, local);
    let mut invalid = ticket();
    invalid.replication.as_mut().unwrap().replicas[1].host_id = "one".into();
    assert!(
        transport
            .write_ticket(&invalid, b"snapshot".to_vec())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn a_full_local_spool_does_not_count_as_a_durable_primary() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let local = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 1).await?);
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(0),
        second_finished: Semaphore::new(0),
        second: Semaphore::new(1),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), local);
    let ticket = ticket();
    let mut write = Box::pin(transport.write_ticket(&ticket, b"snapshot".to_vec()));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut write)
            .await
            .is_err()
    );
    storage.bucket.add_permits(1);
    assert_eq!(write.await?, StateWrite::Written);
    Ok(())
}

fn ticket() -> StateWriteTicket {
    StateWriteTicket {
        stream: None,
        state_version: 1,
        object_name: "snapshot".into(),
        url: "bucket".into(),
        expires_at_ms: i64::MAX,
        replication: Some(ReplicationTicket {
            required_replicas: 2,
            archive_url: "archive".into(),
            replicas: vec![
                ReplicaTarget {
                    region: String::new(),
                    host_id: "one".into(),
                    url: "first".into(),
                },
                ReplicaTarget {
                    region: String::new(),
                    host_id: "two".into(),
                    url: "second".into(),
                },
            ],
        }),
    }
}

#[tokio::test]
async fn replica_http_ack_is_readable_after_restart_and_bound_to_one_node() -> Result<()> {
    use little_actors::{
        clock::SystemClock,
        replication::{ReplicaAccess, ReplicaGrant, replica_router},
    };
    let directory = tempfile::tempdir()?;
    let store = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 4096).await?);
    let access = ReplicaAccess::new("test-installation-key", Arc::new(SystemClock));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let router = replica_router(store.clone(), access.clone(), "node-a".into());
    let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let object = "snapshots/01/0123456789abcdef0123456789abcdef/project/Counter/one/1.json";
    let grant = ReplicaGrant {
        stream: None,
        operation: "PUT".into(),
        object: object.into(),
        region: "us-east".into(),
        host_id: "node-a".into(),
        archive_url: "https://archive.test".into(),
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
    let http = reqwest::Client::builder().http2_prior_knowledge().build()?;
    let write = access.url(&origin, "state", &grant)?;
    assert_eq!(
        http.put(&write).body(bytes.clone()).send().await?.status(),
        201
    );
    let read = access.url(
        &origin,
        "state",
        &ReplicaGrant {
            operation: "GET".into(),
            ..grant.clone()
        },
    )?;
    assert_eq!(http.get(read).send().await?.bytes().await?.as_ref(), bytes);
    let wrong_node = access.url(
        &origin,
        "state",
        &ReplicaGrant {
            host_id: "node-b".into(),
            ..grant
        },
    )?;
    assert_eq!(
        http.put(wrong_node)
            .body(bytes.clone())
            .send()
            .await?
            .status(),
        403
    );
    assert_eq!(http.get(&write).send().await?.status(), 403);
    let _ = shutdown.send(());
    server.await??;
    drop(store);
    assert_eq!(
        FileReplicaStore::open(directory.path().join("snapshots"), 4096)
            .await?
            .read(object)
            .await?,
        Some(bytes)
    );
    Ok(())
}

#[tokio::test]
async fn failed_archival_keeps_the_disk_copy_until_object_storage_is_confirmed() -> Result<()> {
    use axum::{Json, Router, http::StatusCode, routing::get};
    use little_actors::replication::{ArchiveTicket, archive_pending};
    use std::sync::atomic::{AtomicBool, Ordering};
    let directory = tempfile::tempdir()?;
    let store = FileReplicaStore::open(directory.path().join("snapshots"), 4096).await?;
    let available = Arc::new(AtomicBool::new(false));
    let ready = available.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let bucket = format!("{origin}/bucket");
    let router = Router::new()
        .route(
            "/archive",
            get(move || {
                let bucket = bucket.clone();
                async move {
                    Json(ArchiveTicket {
                        write_url: bucket.clone(),
                        read_url: bucket,
                    })
                }
            }),
        )
        .route(
            "/bucket",
            get(|| async { "snapshot" }).put(move || {
                let ready = ready.clone();
                async move {
                    if ready.load(Ordering::Relaxed) {
                        StatusCode::PRECONDITION_FAILED
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    store
        .put("snapshot", &format!("{origin}/archive"), b"snapshot")
        .await?;
    archive_pending(&store).await?;
    assert_eq!(store.read("snapshot").await?, Some(b"snapshot".to_vec()));
    available.store(true, Ordering::Relaxed);
    archive_pending(&store).await?;
    assert!(store.read("snapshot").await?.is_none());
    store
        .put("conflict", &format!("{origin}/archive"), b"different")
        .await?;
    archive_pending(&store).await?;
    assert_eq!(store.read("conflict").await?, Some(b"different".to_vec()));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn replica_uploads_continue_after_object_storage_wins() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let local = Arc::new(FileReplicaStore::open(directory.path().join("snapshots"), 1024).await?);
    let storage = Arc::new(Storage {
        bucket: Semaphore::new(1),
        second: Semaphore::new(0),
        second_finished: Semaphore::new(0),
    });
    let transport = ReplicatedStateTransport::new(storage.clone(), local);
    assert_eq!(
        transport
            .write_ticket(&ticket(), b"snapshot".to_vec())
            .await?,
        StateWrite::Written
    );
    storage.second.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), storage.second_finished.acquire())
        .await??
        .forget();
    Ok(())
}
