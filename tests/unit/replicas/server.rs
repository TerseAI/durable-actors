use super::*;
use crate::bucket::{
    Bucket, BucketObject, FileBucket, ReplicaPlacement, ReplicaSet, SnapshotStore,
};
use crate::replicas::record::Record;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct Time(AtomicU64);
impl Clock for Time {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
struct FailingBucket {
    inner: FileBucket,
    unavailable: AtomicBool,
}
#[async_trait]
impl Bucket for FailingBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        ensure!(!self.unavailable.load(Ordering::SeqCst), "GCS unavailable");
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}
fn prefix() -> &'static str {
    "durable-actors/v3/snapshots/aa/cA/YQ/aQ/00000000000000000000000000000001/"
}
fn data(version: u64) -> Vec<u8> {
    crate::state_log::StateSnapshot::new(
        version,
        1,
        format!("r{version}"),
        serde_json::json!({"count":version}),
        serde_json::json!(version),
    )
    .unwrap()
    .encode()
    .unwrap()
}
fn access() -> Access {
    Access::new("0123456789abcdef0123456789abcdef".into()).unwrap()
}

#[tokio::test]
async fn failed_archival_retains_state_and_a_follower_archives_after_primary_failure() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let bucket = Arc::new(FailingBucket {
        inner: FileBucket::new(dir.path().join("gcs"))?,
        unavailable: AtomicBool::new(true),
    });
    let time = Arc::new(Time(AtomicU64::new(100)));
    let server = ReplicaServer {
        id: "follower".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(bucket.clone()),
        access: access(),
        peers: vec![],
        primary: false,
        clock: time.clone(),
    };
    server.disk.prepare(prefix()).await?;
    server
        .disk
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    time.0.store(10_100, Ordering::SeqCst);
    server.upload_due().await?;
    assert!(
        bucket
            .list(&super::super::record::archive_prefix(prefix())?)
            .await?
            .is_empty()
    );
    time.0.store(30_100, Ordering::SeqCst);
    assert!(server.upload_due().await.is_err());
    assert!(server.disk.batch(prefix(), BATCH_BYTES).await?.is_some());
    bucket.unavailable.store(false, Ordering::SeqCst);
    server.upload_due().await?;
    assert!(server.disk.batch(prefix(), BATCH_BYTES).await?.is_none());
    assert_eq!(server.archive.get(prefix(), 1).await?, Some(data(1)));
    Ok(())
}

#[tokio::test]
async fn primary_archives_after_ten_seconds_and_repeated_uploads_are_idempotent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let time = Arc::new(Time(AtomicU64::new(100)));
    let server = ReplicaServer {
        id: "primary".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?)),
        access: access(),
        peers: vec![],
        primary: true,
        clock: time.clone(),
    };
    server.disk.prepare(prefix()).await?;
    server
        .disk
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    server.upload_due().await?;
    assert!(server.archive.list(prefix()).await?.is_empty());
    let batch = server.disk.batch(prefix(), BATCH_BYTES).await?.unwrap();
    let key = server.archive.write(&batch).await?;
    assert_eq!(key, server.archive.write(&batch).await?);
    time.0.store(10_100, Ordering::SeqCst);
    server.upload_due().await?;
    assert_eq!(
        server.archive.list(prefix()).await?,
        vec![format!("{}1.json", prefix())]
    );
    assert!(server.disk.batch(prefix(), BATCH_BYTES).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn network_replication_recovers_with_a_missing_node_and_fences_delayed_writes() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let archive = Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?));
    let mut servers = Vec::new();
    let mut tasks = Vec::new();
    let mut placements = Vec::new();
    for i in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let id = format!("replica-{i}");
        placements.push(ReplicaPlacement {
            id: id.clone(),
            address: format!("http://{}", listener.local_addr()?),
            zone: "us-west4-a".into(),
        });
        let server = Arc::new(ReplicaServer {
            id,
            disk: ReplicaDisk::open(dir.path().join(format!("{i}.sqlite"))).await?,
            archive: archive.clone(),
            access: access(),
            peers: vec![],
            primary: i == 0,
            clock: Arc::new(SystemClock),
        });
        let app = routes(server.clone());
        tasks.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        servers.push(server);
    }
    let config = PersistenceConfig::Replicated {
        replicas: placements.clone(),
        durability: crate::bucket::Durability::Zonal,
    };
    let store = ReplicaSet::from_config(&config, access().admin().into())?;
    store.prepare(prefix()).await?;
    store
        .put(&format!("{}1.json", prefix()), data(1).into())
        .await?;
    for server in &servers {
        assert_eq!(server.disk.latest(prefix()).await?.unwrap().1, data(1));
    }
    tasks[0].abort();
    let _ = (&mut tasks[0]).await;
    assert!(
        store
            .put(&format!("{}2.json", prefix()), data(2).into())
            .await
            .is_err()
    );
    let recovered = ReplicaSet::from_config(&config, access().admin().into())?;
    recovered.seal(prefix()).await?;
    assert!(recovered.latest(prefix()).await?.is_some());
    for server in &servers[1..] {
        assert!(
            server
                .disk
                .append(prefix(), Record::encode(3, &data(3), None)?, 200)
                .await
                .is_err()
        );
    }
    for task in tasks {
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn a_fresh_disk_cannot_impersonate_a_registered_replica() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let archive = Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?));
    let first = ReplicaDisk::open(dir.path().join("first.sqlite")).await?;
    let replacement = ReplicaDisk::open(dir.path().join("replacement.sqlite")).await?;
    register_disk(&archive, &first, "node").await?;
    register_disk(&archive, &first, "node").await?;
    assert!(register_disk(&archive, &replacement, "node").await.is_err());
    Ok(())
}

#[test]
fn capabilities_are_actor_scoped_and_cannot_administer_replicas() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let access = access();
    let token = access.scoped(&actor)?;
    let own = crate::storage_paths::snapshots(&actor)?;
    access.authorize(&token, Some(&own))?;
    assert!(access.authorize(&token, None).is_err());
    assert!(access.authorize(&token, Some(prefix())).is_err());
    assert!(access.authorize(&format!("{token}x"), Some(&own)).is_err());
    Ok(())
}

#[tokio::test]
async fn sealed_streams_release_local_payloads_only_after_verified_archival() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let server = ReplicaServer {
        id: "primary".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?)),
        access: access(),
        peers: vec![],
        primary: true,
        clock: Arc::new(Time(AtomicU64::new(20_000))),
    };
    server.disk.prepare(prefix()).await?;
    server
        .disk
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    server.disk.seal(prefix()).await?;
    assert_eq!(server.disk.latest(prefix()).await?.unwrap().1, data(1));
    server.upload_due().await?;
    assert!(server.disk.latest(prefix()).await?.is_none());
    let recovered = server
        .execute(Command::Latest {
            prefix: prefix().into(),
        })
        .await?;
    assert_eq!(STANDARD.decode(recovered.data.unwrap())?, data(1));
    let history = server
        .execute(Command::Get {
            object: format!("{}1.json", prefix()),
        })
        .await?;
    assert_eq!(STANDARD.decode(history.data.unwrap())?, data(1));
    Ok(())
}

#[tokio::test]
async fn one_failed_stream_does_not_starve_archival_of_other_actors() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let server = ReplicaServer {
        id: "primary".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?)),
        access: access(),
        peers: vec![],
        primary: true,
        clock: Arc::new(Time(AtomicU64::new(20_000))),
    };
    for stream in ["aaa-invalid/00000000000000000000000000000001/", prefix()] {
        server.disk.prepare(stream).await?;
        server
            .disk
            .append(stream, Record::encode(1, &data(1), None)?, 100)
            .await?;
    }
    assert!(server.upload_due().await.is_err());
    assert_eq!(server.archive.get(prefix(), 1).await?, Some(data(1)));
    Ok(())
}

#[tokio::test]
#[ignore = "local storage-path benchmark; not a GKE latency measurement"]
async fn benchmark_all_replica_acknowledgements() -> Result<()> {
    for count in [1, 3] {
        let dir = tempfile::tempdir()?;
        let mut tasks = Vec::new();
        let mut placements = Vec::new();
        for i in 0..count {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let id = format!("bench-{i}");
            placements.push(ReplicaPlacement {
                id: id.clone(),
                address: format!("http://{}", listener.local_addr()?),
                zone: "us-west4-a".into(),
            });
            let server = Arc::new(ReplicaServer {
                id,
                disk: ReplicaDisk::open(dir.path().join(format!("{i}.sqlite"))).await?,
                archive: Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?)),
                access: access(),
                peers: vec![],
                primary: i == 0,
                clock: Arc::new(SystemClock),
            });
            tasks.push(tokio::spawn(async move {
                axum::serve(listener, routes(server)).await.unwrap();
            }));
        }
        let config = PersistenceConfig::Replicated {
            replicas: placements,
            durability: crate::bucket::Durability::Zonal,
        };
        let store = ReplicaSet::from_config(&config, access().admin().into())?;
        for size in [1024, 1024 * 1024] {
            let actor = crate::actor::ActorKey {
                project_id: "benchmark".into(),
                actor_name: "Counter".into(),
                actor_id: size.to_string(),
            };
            let prefix = format!("{}{:032x}/", crate::storage_paths::snapshots(&actor)?, 1);
            store.prepare(&prefix).await?;
            let payload: String = (0..size)
                .map(|i| char::from(b'a' + (i % 26) as u8))
                .collect();
            let mut samples = Vec::new();
            for version in 1..=110 {
                let bytes = crate::state_log::StateSnapshot::new(
                    version,
                    1,
                    format!("r{version}"),
                    serde_json::json!({"count": version, "payload":payload}),
                    serde_json::json!(version),
                )?
                .encode()?;
                let started = std::time::Instant::now();
                store
                    .put(&format!("{prefix}{version}.json"), bytes.into())
                    .await?;
                if version > 10 {
                    samples.push(started.elapsed().as_secs_f64() * 1000.0);
                }
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "replica_benchmark {}",
                serde_json::json!({"replicas":count,"state_bytes":size,"samples":samples.len(),"p50_ms":samples[49],"p95_ms":samples[94],"p99_ms":samples[98]})
            );
        }
        for task in tasks {
            task.abort();
        }
    }
    Ok(())
}
