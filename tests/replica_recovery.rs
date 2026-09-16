use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use little_actors::{
    actor::ActorKey,
    clock::SystemClock,
    replication::{
        ReplicaAccess, ReplicaCatalog, ReplicaCoordinator, ReplicaManifest, ReplicaProvisioner,
        ReplicaTarget,
    },
    state_transport::{StateTransport, StateWrite},
    storage_urls::{StateWriteTicket, StorageUrlSigner},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Catalog(Mutex<HashMap<String, ReplicaManifest>>);
#[async_trait]
impl ReplicaCatalog for Catalog {
    async fn record(&self, object: &str, manifest: &ReplicaManifest) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .insert(object.into(), manifest.clone());
        Ok(())
    }
    async fn get(&self, object: &str) -> Result<Option<ReplicaManifest>> {
        Ok(self.0.lock().unwrap().get(object).cloned())
    }
}

struct Fleet;
#[async_trait]
impl ReplicaProvisioner for Fleet {
    async fn ensure(&self, _: &ActorKey, _: &str, _: usize) -> Result<Vec<ReplicaTarget>> {
        Ok(vec![
            ReplicaTarget {
                region: String::new(),
                host_id: "one".into(),
                url: "https://one".into(),
            },
            ReplicaTarget {
                region: String::new(),
                host_id: "two".into(),
                url: "https://two".into(),
            },
        ])
    }
}

struct StalledFleet;
#[async_trait]
impl ReplicaProvisioner for StalledFleet {
    async fn ensure(&self, _: &ActorKey, _: &str, _: usize) -> Result<Vec<ReplicaTarget>> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn cold_replica_provisioning_does_not_block_the_bucket_fallback() -> Result<()> {
    let coordinator = ReplicaCoordinator::new(
        Arc::new(Bucket),
        Arc::new(Catalog::default()),
        Arc::new(StalledFleet),
        Arc::new(SurvivingReplica),
        ReplicaAccess::new("installation", Arc::new(SystemClock)),
        "https://control".into(),
        2,
    )?;
    let actor = ActorKey {
        namespace_id: "project".into(),
        actor_type: "Counter".into(),
        actor_id: "one".into(),
    };
    let ticket = tokio::time::timeout(
        std::time::Duration::from_millis(1500),
        coordinator.write_ticket("us-east", &actor, 1),
    )
    .await??;
    assert!(ticket.replication.is_none());
    assert_eq!(ticket.url, "https://bucket/write");
    Ok(())
}

struct Bucket;
#[async_trait]
impl StorageUrlSigner for Bucket {
    async fn read_url(&self, _: &str, _: &str) -> Result<String> {
        Ok("https://bucket/read".into())
    }
    async fn write_ticket(
        &self,
        _: &str,
        _: &ActorKey,
        state_version: u64,
    ) -> Result<StateWriteTicket> {
        Ok(StateWriteTicket {
            state_version,
            object_name: "snapshots/one".into(),
            url: "https://bucket/write".into(),
            expires_at_ms: i64::MAX,
            replication: None,
        })
    }
    fn regions(&self) -> Vec<String> {
        vec!["us-east".into()]
    }
}

struct SurvivingReplica;
#[async_trait]
impl StateTransport for SurvivingReplica {
    async fn read(&self, url: &str) -> Result<Bytes> {
        if url.starts_with("https://two/") {
            Ok(Bytes::from_static(b"committed snapshot"))
        } else {
            anyhow::bail!("owner, first replica and pending bucket are unavailable")
        }
    }
    async fn write(&self, _: &str, _: Vec<u8>) -> Result<StateWrite> {
        anyhow::bail!("not needed")
    }
}

#[tokio::test]
async fn recovery_uses_the_recorded_replica_set_even_when_replication_is_disabled() -> Result<()> {
    let catalog = Arc::new(Catalog::default());
    let create = |count| {
        ReplicaCoordinator::new(
            Arc::new(Bucket),
            catalog.clone(),
            Arc::new(Fleet),
            Arc::new(SurvivingReplica),
            ReplicaAccess::new("installation", Arc::new(SystemClock)),
            "https://control".into(),
            count,
        )
    };
    let enabled = create(2)?;
    let actor = ActorKey {
        namespace_id: "project".into(),
        actor_type: "Counter".into(),
        actor_id: "one".into(),
    };
    let ticket = enabled.write_ticket("us-east", &actor, 1).await?;
    assert_eq!(ticket.replication.as_ref().unwrap().replicas.len(), 2);
    assert!(catalog.get(&ticket.object_name).await?.is_some());
    let restarted = create(0)?;
    assert!(
        restarted
            .read_url("us-east", &ticket.object_name)
            .await?
            .starts_with("https://control/_replica/read")
    );
    assert_eq!(
        restarted.recover("us-east", &ticket.object_name).await?,
        Bytes::from_static(b"committed snapshot")
    );
    Ok(())
}

#[tokio::test]
async fn a_surviving_http_replica_recovers_the_snapshot_before_any_bucket_upload() -> Result<()> {
    use little_actors::{
        replication::{
            ReplicaGrant, ReplicaStore, ReplicatedStateTransport, ReplicationTicket, replica_router,
        },
        state_log::StateSnapshot,
        state_transport::HttpStateTransport,
    };
    struct PeersOnly(HttpStateTransport);
    #[async_trait]
    impl StateTransport for PeersOnly {
        async fn read(&self, url: &str) -> Result<Bytes> {
            if url.starts_with("https://bucket") {
                anyhow::bail!("bucket is unavailable");
            }
            self.0.read(url).await
        }
        async fn write(&self, url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
            if url.starts_with("https://bucket") {
                anyhow::bail!("bucket is unavailable");
            }
            self.0.write(url, bytes).await
        }
    }
    let directory = tempfile::tempdir()?;
    let access = ReplicaAccess::new("installation", Arc::new(SystemClock));
    let object = "snapshots/one";
    let mut replicas = Vec::new();
    let mut servers = Vec::new();
    for index in 0..2 {
        let store = Arc::new(
            ReplicaStore::open(directory.path().join(format!("peer-{index}.db")), 4096).await?,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let host_id = format!("peer-{index}");
        replicas.push(ReplicaTarget {
            region: String::new(),
            host_id: host_id.clone(),
            url: format!("http://{}", listener.local_addr()?),
        });
        let router = replica_router(store, access.clone(), host_id);
        servers.push(tokio::spawn(
            async move { axum::serve(listener, router).await },
        ));
    }
    let catalog = Arc::new(Catalog::default());
    catalog
        .record(
            object,
            &ReplicaManifest {
                region: "us-east".into(),
                replicas: replicas.clone(),
            },
        )
        .await?;
    let ticket = StateWriteTicket {
        state_version: 1,
        object_name: object.into(),
        url: "https://bucket/write".into(),
        expires_at_ms: i64::MAX,
        replication: Some(ReplicationTicket {
            required_replicas: 2,
            archive_url: "https://control/archive".into(),
            replicas: replicas
                .iter()
                .map(|peer| {
                    Ok(ReplicaTarget {
                        region: String::new(),
                        host_id: peer.host_id.clone(),
                        url: access.url(
                            &peer.url,
                            "state",
                            &ReplicaGrant {
                                operation: "PUT".into(),
                                object: object.into(),
                                region: "us-east".into(),
                                host_id: peer.host_id.clone(),
                                archive_url: "https://control/archive".into(),
                                expires_at_ms: u64::MAX,
                            },
                        )?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        }),
    };
    let http = Arc::new(PeersOnly(HttpStateTransport::new()));
    let primary = Arc::new(ReplicaStore::open(directory.path().join("primary.db"), 4096).await?);
    let transport = ReplicatedStateTransport::new(http.clone(), primary.clone());
    let snapshot = StateSnapshot::new(
        1,
        1,
        "acknowledged".into(),
        serde_json::json!({"count":42}),
        serde_json::json!(42),
    )?
    .encode()?;
    assert_eq!(
        transport.write_ticket(&ticket, snapshot.clone()).await?,
        StateWrite::Replicated
    );
    assert_eq!(primary.read(object).await?, Some(snapshot.clone()));
    drop(transport);
    drop(primary);
    servers.remove(0).abort();
    let recovery = ReplicaCoordinator::new(
        Arc::new(Bucket),
        catalog,
        Arc::new(Fleet),
        http,
        access,
        "https://control".into(),
        0,
    )?;
    let recovered = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        recovery.recover("us-east", object),
    )
    .await??;
    assert_eq!(recovered.as_ref(), snapshot);
    for server in servers {
        server.abort();
    }
    Ok(())
}

struct RemoteFleet;
#[async_trait]
impl ReplicaProvisioner for RemoteFleet {
    fn replica_regions(&self) -> Vec<String> {
        vec!["north-america-central".into(), "north-america-west".into()]
    }
    async fn ensure(
        &self,
        actor: &ActorKey,
        region: &str,
        count: usize,
    ) -> Result<Vec<ReplicaTarget>> {
        let mut peers = Fleet.ensure(actor, region, count).await?;
        for (peer, region) in peers.iter_mut().zip(self.replica_regions()) {
            peer.region = region;
        }
        Ok(peers)
    }
}

#[tokio::test]
async fn cross_region_tickets_record_remote_locations_without_changing_the_bucket_home()
-> Result<()> {
    let catalog = Arc::new(Catalog::default());
    let coordinator = ReplicaCoordinator::new(
        Arc::new(Bucket),
        catalog.clone(),
        Arc::new(RemoteFleet),
        Arc::new(SurvivingReplica),
        ReplicaAccess::new("installation", Arc::new(SystemClock)),
        "https://control".into(),
        2,
    )?;
    assert_eq!(coordinator.durability().mode, "cross_region_preview");
    assert_eq!(
        coordinator.durability().replica_regions,
        RemoteFleet.replica_regions()
    );
    let actor = ActorKey {
        namespace_id: "project".into(),
        actor_type: "Counter".into(),
        actor_id: "one".into(),
    };
    let ticket = coordinator
        .write_ticket("north-america-east", &actor, 1)
        .await?;
    let manifest = catalog.get(&ticket.object_name).await?.unwrap();
    assert_eq!(manifest.region, "north-america-east");
    assert_eq!(
        ticket
            .replication
            .unwrap()
            .replicas
            .iter()
            .map(|peer| peer.region.clone())
            .collect::<Vec<_>>(),
        RemoteFleet.replica_regions()
    );
    let old: ReplicaManifest = serde_json::from_str(
        r#"{"region":"north-america-east","replicas":[{"hostId":"old","url":"https://old"}]}"#,
    )?;
    assert_eq!(old.replicas[0].region, "");
    Ok(())
}
