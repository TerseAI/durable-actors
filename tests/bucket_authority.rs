use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Result;
use async_trait::async_trait;
use little_actors::{
    bucket::{Bucket, BucketHostLeases, BucketObject},
    clock::Clock,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest, HostLeaseStore},
};

use little_actors::{
    actor::ActorKey,
    bucket::{ReplicaPeers, RuntimeStorage},
    placement::{ObjectPlacementStore, PlacementClaim},
    replication::{
        FileReplicaStore, ReplicaAccess, ReplicaProvisioner, ReplicaStore, ReplicaStream,
        ReplicaTarget, StreamHead,
    },
    state_log::StateSnapshot,
    storage_urls::StorageUrlSigner,
};

struct Fleet(Vec<ReplicaTarget>);
#[async_trait]
impl ReplicaProvisioner for Fleet {
    async fn ensure(&self, _: &ActorKey, _: &str, _: usize) -> Result<Vec<ReplicaTarget>> {
        Ok(self.0.clone())
    }
}

struct Peers {
    stores: BTreeMap<String, Arc<FileReplicaStore>>,
    unavailable: Mutex<Vec<String>>,
}
impl Peers {
    fn store(&self, peer: &ReplicaTarget) -> Result<&Arc<FileReplicaStore>> {
        anyhow::ensure!(
            !self.unavailable.lock().unwrap().contains(&peer.host_id),
            "peer unavailable"
        );
        Ok(&self.stores[&peer.host_id])
    }
}
#[async_trait]
impl ReplicaPeers for Peers {
    async fn initialize(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<()> {
        self.store(peer)?.initialize_stream(stream).await
    }
    async fn head(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        self.store(peer)?.stream_head(stream).await
    }
    async fn seal(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        self.store(peer)?.seal(stream).await
    }
    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        self.store(peer)?
            .read(object)
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing snapshot"))
    }
}

#[tokio::test]
async fn takeover_recovers_replica_only_writes_and_fences_the_old_epoch() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let leases = Arc::new(BucketHostLeases::new(bucket.clone(), clock.clone()));
    leases.register(&request("old")).await?;
    let targets: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|id| ReplicaTarget {
            host_id: id.into(),
            url: format!("http://{id}"),
            region: "us-east".into(),
        })
        .collect();
    let mut stores = BTreeMap::new();
    for peer in &targets {
        stores.insert(
            peer.host_id.clone(),
            Arc::new(FileReplicaStore::open(directory.path().join(&peer.host_id), 4096).await?),
        );
    }
    let peers = Arc::new(Peers {
        stores,
        unavailable: Mutex::new(vec![]),
    });
    let runtime = RuntimeStorage::new(
        bucket.clone(),
        std::collections::HashMap::from([("us-east".into(), bucket.clone() as Arc<dyn Bucket>)]),
        leases.clone(),
        Arc::new(Fleet(targets)),
        peers.clone(),
        ReplicaAccess::new("secret", clock.clone()),
        "http://control".into(),
        2,
    )?;
    let actor = ActorKey {
        namespace_id: "project".into(),
        actor_type: "Counter".into(),
        actor_id: "one".into(),
    };
    let PlacementClaim::Acquired(first) = runtime
        .claim_actor(&actor, None, &HostId::new("host"), "us-east")
        .await?
    else {
        panic!("claim lost")
    };
    let ticket = runtime.write_ticket("us-east", &actor, 1).await?;
    let stream = ticket.stream.as_ref().unwrap();
    let bytes = StateSnapshot::new(
        1,
        first.owner_epoch,
        "committed".into(),
        serde_json::json!({"count":1}),
        serde_json::json!(1),
    )?
    .encode()?;
    for store in peers.stores.values() {
        store.append(stream, "archive", &bytes).await?;
    }
    clock.0.store(20_000, Ordering::SeqCst);
    leases.register(&request("new")).await?;
    *peers.unavailable.lock().unwrap() = vec!["a".into(), "b".into()];
    assert!(
        runtime
            .claim_actor(&actor, Some(&first), &HostId::new("host"), "us-east")
            .await
            .is_err()
    );
    assert!(runtime.write_ticket("us-east", &actor, 2).await.is_err());
    let pending = runtime.get(&actor.storage_key()).await?.unwrap();
    *peers.unavailable.lock().unwrap() = vec!["a".into()];
    let PlacementClaim::Acquired(next) = runtime
        .claim_actor(&actor, Some(&pending), &HostId::new("host"), "us-east")
        .await?
    else {
        panic!("claim lost")
    };
    assert_eq!(next.state_version, 1);
    assert_eq!(next.last_request_id.as_deref(), Some("committed"));
    assert_eq!(
        bucket
            .get(next.state_object.as_ref().unwrap())
            .await?
            .unwrap()
            .bytes,
        bytes
    );
    assert!(
        peers.stores["b"]
            .append(stream, "archive", &bytes)
            .await
            .is_err()
    );
    Ok(())
}

#[derive(Default)]
struct MemoryBucket {
    objects: Mutex<BTreeMap<String, BucketObject>>,
    lose_reply: AtomicBool,
    reject_snapshots: AtomicBool,
}

#[async_trait]
impl Bucket for MemoryBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        anyhow::ensure!(
            !key.starts_with("snapshots/") || !self.reject_snapshots.load(Ordering::SeqCst),
            "bucket writes unavailable"
        );
        let mut objects = self.objects.lock().unwrap();
        let current = objects.get(key).map(|object| object.generation);
        if current != generation {
            return Ok(false);
        }
        objects.insert(
            key.into(),
            BucketObject {
                generation: current.unwrap_or(0) + 1,
                bytes,
            },
        );
        anyhow::ensure!(
            !self.lose_reply.swap(false, Ordering::SeqCst),
            "CAS response lost"
        );
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

fn request(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new("host"),
        session_id: session.into(),
        route: "http://host".into(),
        duration_ms: 10_000,
    }
}

#[tokio::test]
async fn only_one_session_can_acquire_a_host_and_expired_sessions_cannot_return() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let leases = Arc::new(BucketHostLeases::new(bucket.clone(), clock.clone()));
    let first = request("a");
    let second = request("b");
    let (a, b) = tokio::join!(leases.register(&first), leases.register(&second));
    assert_ne!(a.is_ok(), b.is_ok());
    let winner = if a.is_ok() { "a" } else { "b" };
    clock.0.store(20_000, Ordering::SeqCst);
    assert!(leases.register(&request(winner)).await.is_err());
    leases.register(&request("new")).await?;
    leases.unregister(&HostId::new("host"), winner).await?;
    let reopened = BucketHostLeases::new(bucket, clock);
    assert_eq!(
        reopened
            .lease_status(&HostId::new("host"))
            .await?
            .lease
            .unwrap()
            .session_id,
        "new"
    );
    Ok(())
}

#[tokio::test]
async fn releasing_a_lease_leaves_a_fence_against_delayed_renewal() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let leases = BucketHostLeases::new(bucket, clock);
    leases.register(&request("old")).await?;
    leases.unregister(&HostId::new("host"), "old").await?;
    assert!(!leases.lease_status(&HostId::new("host")).await?.is_active());
    assert!(leases.register(&request("old")).await.is_err());
    leases.register(&request("replacement")).await?;
    assert!(leases.register(&request("old")).await.is_err());
    Ok(())
}

#[tokio::test]
async fn a_lost_conditional_write_response_is_reconciled_by_identity() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let leases = BucketHostLeases::new(bucket.clone(), clock);
    bucket.lose_reply.store(true, Ordering::SeqCst);
    let lease = leases.register(&request("session")).await?;
    assert_eq!(
        lease,
        leases
            .lease_status(&HostId::new("host"))
            .await?
            .lease
            .unwrap()
    );
    Ok(())
}

#[tokio::test]
async fn simultaneous_claims_from_the_same_observed_generation_have_one_winner() -> Result<()> {
    struct RacingBucket {
        inner: Arc<MemoryBucket>,
        readers: AtomicU64,
        barrier: tokio::sync::Barrier,
    }
    #[async_trait]
    impl Bucket for RacingBucket {
        async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
            let observed = self.inner.get(key).await?;
            if key.starts_with("runtime/owners/") && self.readers.fetch_add(1, Ordering::SeqCst) < 2
            {
                self.barrier.wait().await;
            }
            Ok(observed)
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            generation: Option<i64>,
            bytes: Vec<u8>,
        ) -> Result<bool> {
            self.inner.compare_and_swap(key, generation, bytes).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }
    }
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let leases = Arc::new(BucketHostLeases::new(bucket.clone(), clock.clone()));
    let mut left = request("left");
    left.id = HostId::new("left");
    let mut right = request("right");
    right.id = HostId::new("right");
    leases.register(&left).await?;
    leases.register(&right).await?;
    let authority = Arc::new(RacingBucket {
        inner: bucket.clone(),
        readers: AtomicU64::new(0),
        barrier: tokio::sync::Barrier::new(2),
    });
    let runtime = RuntimeStorage::new(
        authority,
        std::collections::HashMap::from([("us-east".into(), bucket as Arc<dyn Bucket>)]),
        leases,
        Arc::new(Fleet(vec![])),
        Arc::new(Peers {
            stores: BTreeMap::new(),
            unavailable: Mutex::new(vec![]),
        }),
        ReplicaAccess::new("secret", clock),
        "http://control".into(),
        0,
    )?;
    let actor = ActorKey {
        namespace_id: "project".into(),
        actor_type: "Counter".into(),
        actor_id: "race".into(),
    };
    let (first, second) = tokio::join!(
        runtime.claim_actor(&actor, None, &left.id, "us-east"),
        runtime.claim_actor(&actor, None, &right.id, "us-east")
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let PlacementClaim::Acquired(winner) = first.or(second)? else {
        panic!("claim was not acquired")
    };
    assert_eq!(
        runtime.get_owner(&actor.storage_key()).await?.unwrap(),
        winner
    );
    Ok(())
}

#[tokio::test]
async fn http_replication_archival_and_takeover_run_without_postgres() -> Result<()> {
    use little_actors::{
        bucket::HttpReplicaPeers,
        clock::SystemClock,
        replication::{ReplicatedStateTransport, archive_pending, replica_router},
        state_transport::{HttpStateTransport, StateTransport, StateWrite},
    };
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let leases = Arc::new(BucketHostLeases::new(bucket.clone(), clock.clone()));
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let replica = Arc::new(FileReplicaStore::open(directory.path().join("replica"), 4096).await?);
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let peer_url = format!("http://{}", peer_listener.local_addr()?);
    let peer_stop = tokio_util::sync::CancellationToken::new();
    let peer_shutdown = peer_stop.clone();
    let routes = replica_router(replica.clone(), access.clone(), "peer".into());
    let peer_server = tokio::spawn(async move {
        axum::serve(peer_listener, routes)
            .with_graceful_shutdown(peer_shutdown.cancelled_owned())
            .await
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let runtime = Arc::new(RuntimeStorage::new(
        bucket.clone(),
        std::collections::HashMap::from([("us-east".into(), bucket.clone() as Arc<dyn Bucket>)]),
        leases.clone(),
        Arc::new(Fleet(vec![ReplicaTarget {
            host_id: "peer".into(),
            url: peer_url,
            region: "us-east".into(),
        }])),
        Arc::new(HttpReplicaPeers::new(access.clone())?),
        access,
        origin,
        1,
    )?);
    let stop = tokio_util::sync::CancellationToken::new();
    let shutdown = stop.clone();
    let routes = runtime.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    leases.register(&request("old")).await?;
    let actor = ActorKey {
        namespace_id: "project".into(),
        actor_type: "Counter".into(),
        actor_id: "http".into(),
    };
    let PlacementClaim::Acquired(first) = runtime
        .claim_actor(&actor, None, &HostId::new("host"), "us-east")
        .await?
    else {
        panic!("claim lost")
    };
    let ticket = runtime.write_ticket("us-east", &actor, 1).await?;
    let snapshot = StateSnapshot::new(
        1,
        first.owner_epoch,
        "first".into(),
        serde_json::json!({"count":1}),
        serde_json::json!(1),
    )?
    .encode()?;
    let http = Arc::new(HttpStateTransport::new());
    let transport = ReplicatedStateTransport::new(
        http.clone(),
        Arc::new(FileReplicaStore::open(directory.path().join("actor"), 4096).await?),
    );
    bucket.reject_snapshots.store(true, Ordering::SeqCst);
    assert_eq!(
        transport.write_ticket(&ticket, snapshot.clone()).await?,
        StateWrite::Replicated
    );
    assert!(bucket.get(&ticket.object_name).await?.is_none());
    assert_eq!(
        http.read(&runtime.read_url("us-east", &ticket.object_name).await?)
            .await?
            .as_ref(),
        snapshot
    );
    clock.0.store(20_000, Ordering::SeqCst);
    leases.register(&request("new")).await?;
    bucket.reject_snapshots.store(false, Ordering::SeqCst);
    let PlacementClaim::Acquired(next) = runtime
        .claim_actor(&actor, Some(&first), &HostId::new("host"), "us-east")
        .await?
    else {
        panic!("claim lost")
    };
    assert_eq!(next.state_version, 1);
    let late = StateSnapshot::new(
        2,
        first.owner_epoch,
        "late".into(),
        serde_json::json!({"count":99}),
        serde_json::json!(99),
    )?
    .encode()?;
    assert!(
        http.write(
            &ticket.replication.as_ref().unwrap().replicas[0].url,
            late.clone()
        )
        .await
        .is_err()
    );
    assert!(http.write(&ticket.url, late).await.is_err());
    assert_eq!(
        runtime
            .get(&actor.storage_key())
            .await?
            .unwrap()
            .state_version,
        1
    );
    archive_pending(replica.as_ref()).await?;
    assert!(replica.pending(10).await?.is_empty());
    assert_eq!(
        bucket.get(&ticket.object_name).await?.unwrap().bytes,
        snapshot
    );
    stop.cancel();
    peer_stop.cancel();
    server.await??;
    peer_server.await??;
    Ok(())
}
