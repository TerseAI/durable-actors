use super::*;
use crate::{
    bucket::{BucketObject, FileBucket},
    replication::{
        FileReplicaStore, ReplicaProvisioner, ReplicaScope, ReplicaStore, ReplicaStream,
        ReplicaTarget, SessionHead, StreamHead, replica_routes,
    },
    state_log::StateSnapshot,
    state_transport::{SnapshotWriter, StateWrite},
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

struct Gate {
    entered: Semaphore,
    release: Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}

impl Gate {
    async fn block(&self) -> Result<()> {
        self.entered.add_permits(1);
        self.release.acquire().await?.forget();
        Ok(())
    }
    async fn wait(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), self.entered.acquire())
            .await??
            .forget();
        Ok(())
    }
}

struct Fleet {
    targets: Vec<ReplicaTarget>,
    first: AtomicBool,
    provisioning: Gate,
}

#[async_trait]
impl ReplicaProvisioner for Fleet {
    fn replica_regions(&self) -> Vec<String> {
        vec!["us-east".into()]
    }
    async fn ensure(&self, _: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        if self.first.swap(false, Ordering::SeqCst) {
            self.provisioning.block().await?;
        }
        Ok(self.targets.clone())
    }
}

struct Authority {
    inner: FileBucket,
    first_membership: AtomicBool,
    publication: Gate,
    reject_snapshots: AtomicBool,
}

#[async_trait]
impl Bucket for Authority {
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
        ensure!(
            !key.contains("/snapshots/") || !self.reject_snapshots.load(Ordering::SeqCst),
            "GCS unavailable"
        );
        if key.contains("/hosts/") && self.first_membership.swap(false, Ordering::SeqCst) {
            self.publication.block().await?;
        }
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}

struct Replica {
    inner: FileReplicaStore,
    first_seed: AtomicBool,
    fail_writes: AtomicBool,
    seeding: Gate,
}

#[async_trait]
impl ReplicaStore for Replica {
    async fn initialize_session(&self, session: &str) -> Result<()> {
        self.inner.initialize_session(session).await
    }
    async fn append(&self, stream: &ReplicaStream, bytes: &[u8]) -> Result<()> {
        ensure!(
            !self.fail_writes.load(Ordering::SeqCst),
            "replica unavailable"
        );
        self.inner.append(stream, bytes).await?;
        if self.first_seed.swap(false, Ordering::SeqCst) {
            self.seeding.block().await?;
        }
        Ok(())
    }
    async fn stream_head(&self, stream: &ReplicaStream) -> Result<StreamHead> {
        self.inner.stream_head(stream).await
    }
    async fn seal_session(&self, session: &str) -> Result<SessionHead> {
        self.inner.seal_session(session).await
    }
    async fn read(&self, object: &str) -> Result<Option<Vec<u8>>> {
        self.inner.read(object).await
    }
    async fn put(&self, object: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put(object, bytes).await
    }
}

#[tokio::test]
async fn writes_continue_during_provisioning_seeding_and_membership_cas_then_replicas_catch_up()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let scope = ReplicaScope {
        actor: ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "catch-up".into(),
        },
        host: HostId::new("primary"),
        session: "activation".into(),
        region: "us-east".into(),
    };
    let replica = Arc::new(Replica {
        inner: FileReplicaStore::open(directory.path().join("replica"), 4096).await?,
        first_seed: AtomicBool::new(true),
        fail_writes: AtomicBool::new(false),
        seeding: Gate::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let target = ReplicaTarget {
        host_id: "replica".into(),
        region: "us-east".into(),
        url: format!("http://{}", listener.local_addr()?),
    };
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let routes = replica_routes(
        replica.clone(),
        access.clone(),
        target.host_id.clone(),
        scope.clone(),
    );
    let shutdown = stop.clone();
    tokio::spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    let fleet = Arc::new(Fleet {
        targets: vec![target],
        first: AtomicBool::new(true),
        provisioning: Gate::default(),
    });
    let authority = Arc::new(Authority {
        inner: FileBucket::new(directory.path().join("bucket"))?,
        first_membership: AtomicBool::new(true),
        publication: Gate::default(),
        reject_snapshots: AtomicBool::new(false),
    });
    let runtime = Arc::new(RuntimeStorage::new(
        authority.clone(),
        fleet.clone(),
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access.clone(),
        "http://control".into(),
        Arc::new(SystemClock),
    )?);
    let storage = Arc::new(HostStorage {
        durability: crate::replication::DurabilityPolicy::default(),
        observer: Arc::new(ControlPlaneClient::connect("http://127.0.0.1:1", "unavailable").await?),
        runtime: runtime.clone(),
        transport: Default::default(),
        stop: stop.clone(),
        host: scope.host.clone(),
        session: scope.session.clone(),
        region: scope.region.clone(),
        actor: Some(scope.actor.clone()),
        new_actor: true,
        activation: Mutex::new(None),
        fence: Mutex::new(LeaseFence::default()),
        lease: Mutex::new(None),
    });
    storage
        .register(&HostLeaseRequest {
            id: scope.host.clone(),
            session_id: scope.session.clone(),
            route: "http://primary".into(),
            duration_ms: 60_000,
        })
        .await?;
    let initial_source = Arc::new(crate::bucket::access::RuntimeAccess::new(
        crate::bucket::access::BucketLocation::File {
            directory: directory.path().join("bucket"),
        },
        Arc::new(crate::replication::ReplicaSet(vec![ReplicaTarget {
            host_id: "crashed-initial".into(),
            url: "http://127.0.0.1:1".into(),
            region: "us-east".into(),
        }])),
        access,
        runtime.clone(),
    )?);
    let initial = crate::host::replication::InitialReplication::start(
        initial_source,
        scope.clone(),
        true,
        stop.clone(),
        storage.transport.clone(),
    );
    let writer = crate::host::replication::ActorReplication::start(
        storage.clone(),
        scope.clone(),
        stop.clone(),
        initial,
    );
    let write = |version| {
        let (storage, writer, scope) = (storage.clone(), writer.clone(), scope.clone());
        async move {
            let plan = storage
                .prepare_state_write(&scope.actor, &scope.host, 1, version - 1)
                .await?;
            let bytes = StateSnapshot::new(
                version,
                1,
                format!("write-{version}"),
                serde_json::json!({"count": version}),
                serde_json::json!(version),
            )?
            .encode()?;
            let proof =
                tokio::time::timeout(Duration::from_secs(1), writer.write_snapshot(&plan, bytes))
                    .await??;
            anyhow::Ok((plan, proof))
        }
    };
    authority.publication.wait().await?;
    authority.publication.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.local_replica_members(&scope).is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    authority.first_membership.store(true, Ordering::SeqCst);
    assert_eq!(write(1).await?.1, StateWrite::Written);
    fleet.provisioning.wait().await?;
    fleet.provisioning.release.add_permits(1);
    replica.seeding.wait().await?;
    assert_eq!(
        write(2).await?.1,
        StateWrite::Written,
        "seeding must not lock writes"
    );
    replica.seeding.release.add_permits(1);
    authority.publication.wait().await?;
    let (third, proof) = write(3).await?;
    assert_eq!(
        proof,
        StateWrite::Written,
        "membership publication must not lock writes"
    );
    assert!(runtime.local_replica_members(&scope).is_empty());
    authority.publication.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.local_replica_members(&scope).is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(
        replica
            .stream_head(&third.stream)
            .await?
            .latest
            .unwrap()
            .state_version,
        3,
        "catch-up must include writes committed during seeding and publication"
    );
    authority.reject_snapshots.store(true, Ordering::SeqCst);
    let (fourth, proof) = write(4).await?;
    assert_eq!(proof, StateWrite::Replicated);
    assert!(authority.get(&fourth.object_name).await?.is_none());
    authority.reject_snapshots.store(false, Ordering::SeqCst);
    storage.unregister(&scope.host, &scope.session).await?;
    runtime.retire_replication(&scope).await?;
    assert!(
        authority.get(&fourth.object_name).await?.is_some(),
        "replica-only commit must survive recovery"
    );
    Ok(())
}

#[tokio::test]
async fn writes_require_repaired_replicas_through_provisioning_seeding_and_publication()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let scope = ReplicaScope {
        actor: ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "catch-up".into(),
        },
        host: HostId::new("primary"),
        session: "activation".into(),
        region: "us-east".into(),
    };
    let replica = Arc::new(Replica {
        inner: FileReplicaStore::open(directory.path().join("replica"), 4096).await?,
        first_seed: AtomicBool::new(false),
        fail_writes: AtomicBool::new(false),
        seeding: Gate::default(),
    });
    replica.initialize_session(&scope.identity()).await?;
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let mut targets = Vec::new();
    for host_id in ["replica", "replacement"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let target = ReplicaTarget {
            host_id: host_id.into(),
            region: "us-east".into(),
            url: format!("http://{}", listener.local_addr()?),
        };
        let routes = replica_routes(
            replica.clone(),
            access.clone(),
            target.host_id.clone(),
            scope.clone(),
        );
        let shutdown = stop.clone();
        tokio::spawn(async move {
            axum::serve(listener, routes)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        targets.push(target);
    }
    let initial_target = targets.remove(0);
    let fleet = Arc::new(Fleet {
        targets,
        first: AtomicBool::new(true),
        provisioning: Gate::default(),
    });
    let authority = Arc::new(Authority {
        inner: FileBucket::new(directory.path().join("bucket"))?,
        first_membership: AtomicBool::new(true),
        publication: Gate::default(),
        reject_snapshots: AtomicBool::new(false),
    });
    let runtime = Arc::new(RuntimeStorage::new(
        authority.clone(),
        fleet.clone(),
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access.clone(),
        "http://control".into(),
        Arc::new(SystemClock),
    )?);
    let storage = Arc::new(HostStorage {
        durability: crate::replication::DurabilityPolicy::AllReplicas,
        observer: Arc::new(ControlPlaneClient::connect("http://127.0.0.1:1", "unavailable").await?),
        runtime: runtime.clone(),
        transport: Default::default(),
        stop: stop.clone(),
        host: scope.host.clone(),
        session: scope.session.clone(),
        region: scope.region.clone(),
        actor: Some(scope.actor.clone()),
        new_actor: true,
        activation: Mutex::new(None),
        fence: Mutex::new(LeaseFence::default()),
        lease: Mutex::new(None),
    });
    storage
        .register(&HostLeaseRequest {
            id: scope.host.clone(),
            session_id: scope.session.clone(),
            route: "http://primary".into(),
            duration_ms: 60_000,
        })
        .await?;
    let initial_source = Arc::new(crate::bucket::access::RuntimeAccess::new(
        crate::bucket::access::BucketLocation::File {
            directory: directory.path().join("bucket"),
        },
        Arc::new(crate::replication::ReplicaSet(vec![initial_target])),
        access,
        runtime.clone(),
    )?);
    let initial = crate::host::replication::InitialReplication::start(
        initial_source,
        scope.clone(),
        true,
        stop.clone(),
        storage.transport.clone(),
    );
    let writer = crate::host::replication::ActorReplication::start(
        storage.clone(),
        scope.clone(),
        stop.clone(),
        initial,
    );
    let write = |version| {
        let (storage, writer, scope) = (storage.clone(), writer.clone(), scope.clone());
        async move {
            let plan = storage
                .prepare_state_write(&scope.actor, &scope.host, 1, version - 1)
                .await?;
            let bytes = StateSnapshot::new(
                version,
                1,
                format!("write-{version}"),
                serde_json::json!({"count": version}),
                serde_json::json!(version),
            )?
            .encode()?;
            let proof =
                tokio::time::timeout(Duration::from_secs(1), writer.write_snapshot(&plan, bytes))
                    .await??;
            anyhow::Ok((plan, proof))
        }
    };
    authority.publication.wait().await?;
    authority.publication.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.local_replica_members(&scope).is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(write(1).await?.1, StateWrite::Replicated);
    authority.first_membership.store(true, Ordering::SeqCst);
    replica.fail_writes.store(true, Ordering::SeqCst);
    assert!(write(2).await.is_err());
    fleet.provisioning.wait().await?;
    assert!(write(2).await.is_err());
    replica.fail_writes.store(false, Ordering::SeqCst);
    replica.first_seed.store(true, Ordering::SeqCst);
    fleet.provisioning.release.add_permits(1);
    replica.seeding.wait().await?;
    assert!(
        write(2).await.is_err(),
        "unseeded replicas cannot acknowledge writes"
    );
    replica.seeding.release.add_permits(1);
    authority.publication.wait().await?;
    assert!(
        write(2).await.is_err(),
        "unpublished membership cannot acknowledge writes"
    );
    assert!(runtime.local_replica_members(&scope).is_empty());
    authority.publication.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.local_replica_members(&scope).is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let (second, proof) = write(2).await?;
    assert_eq!(proof, StateWrite::Replicated);
    assert_eq!(
        replica
            .stream_head(&second.stream)
            .await?
            .latest
            .unwrap()
            .state_version,
        2
    );
    authority.reject_snapshots.store(true, Ordering::SeqCst);
    let (third, proof) = write(3).await?;
    assert_eq!(proof, StateWrite::Replicated);
    assert!(authority.get(&third.object_name).await?.is_none());
    authority.reject_snapshots.store(false, Ordering::SeqCst);
    storage.unregister(&scope.host, &scope.session).await?;
    runtime.retire_replication(&scope).await?;
    assert!(
        authority.get(&third.object_name).await?.is_some(),
        "replica-only commit must survive recovery"
    );
    Ok(())
}
