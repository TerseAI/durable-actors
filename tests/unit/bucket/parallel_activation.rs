use super::*;
use crate::{replication::ReplicaStore, state_log::StateSnapshot};
use tokio::sync::Semaphore;

#[tokio::test]
async fn recovery_seals_replicas_before_the_claim_write_finishes() -> Result<()> {
    let (mut f, peers, _, bytes) = replicated().await?;
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), release.clone()));
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    entered.acquire().await?.forget();
    let overlapped = tokio::time::timeout(Duration::from_secs(1), peers.sealed.acquire()).await;
    release.add_permits(1);
    let result = task.await??;
    overlapped
        .context("sealing waited for the claim write")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    Ok(())
}

#[tokio::test]
async fn a_lost_claim_retries_the_repaired_membership_and_discards_old_seal_errors() -> Result<()> {
    let (mut f, peers, scope, _) = replicated().await?;
    let replacement = ReplicaTarget {
        host_id: "replacement".into(),
        url: "http://replacement".into(),
        region: "us-east".into(),
    };
    let store = Arc::new(
        crate::replication::FileReplicaStore::open(f._directory.path().join("replacement"), 4096)
            .await?,
    );
    store.initialize_session(&scope.identity()).await?;
    let stream = f
        .runtime
        .load(&f.actor.storage_key())
        .await?
        .unwrap()
        .1
        .stream()?;
    let bytes = snapshot(2)?;
    store.append(&stream, &bytes).await?;
    peers
        .stores
        .lock()
        .unwrap()
        .insert(replacement.host_id.clone(), store.clone());
    peers.fail_old_seal.store(true, Ordering::SeqCst);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), release.clone()));
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    entered.acquire().await?.forget();
    let overlapped = tokio::time::timeout(Duration::from_secs(1), peers.sealed.acquire()).await;
    let key = format!(
        "{}.json",
        crate::storage_paths::session(&scope.host, &scope.session)
    );
    let object = f.bucket.inner.get(&key).await?.unwrap();
    let mut session: session::Session = serde_json::from_slice(&object.bytes)?;
    session.replicas = vec![replacement];
    assert!(
        f.bucket
            .inner
            .compare_and_swap(&key, Some(object.generation), serde_json::to_vec(&session)?)
            .await?
    );
    release.add_permits(1);
    let result = task.await??;
    overlapped
        .context("sealing did not overlap the disputed claim")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    assert!(store.append(&stream, &snapshot(3)?).await.is_err());
    Ok(())
}

#[tokio::test]
async fn snapshot_listing_overlaps_replica_restore_without_losing_the_newer_state() -> Result<()> {
    let (mut f, peers, _, _) = replicated().await?;
    let bytes = snapshot(2)?;
    let owner = f.runtime.load(&f.actor.storage_key()).await?.unwrap().1;
    f.bucket
        .inner
        .compare_and_swap(&owner.stream()?.object(1), None, snapshot(1)?)
        .await?;
    let store = peers.stores.lock().unwrap()["old"].clone();
    store.append(&owner.stream()?, &bytes).await?;
    let bucket = Arc::new(ObservedBucket {
        inner: f.bucket.clone(),
        listed: Semaphore::new(0),
        snapshot_read: None,
    });
    let release = Arc::new(Semaphore::new(0));
    peers
        .read_release
        .lock()
        .unwrap()
        .insert("old".into(), release.clone());
    f.runtime.authority = bucket.clone();
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    peers.read_started.acquire().await?.forget();
    let overlapped = tokio::time::timeout(Duration::from_secs(1), bucket.listed.acquire()).await;
    release.add_permits(8);
    let result = task.await??;
    overlapped
        .context("snapshot listing waited for replica restoration")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    Ok(())
}

#[tokio::test]
async fn replica_reads_overlap_the_bucket_lookup() -> Result<()> {
    let (mut f, peers, _, bytes) = replicated().await?;
    let read_started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    f.runtime.authority = Arc::new(ObservedBucket {
        inner: f.bucket.clone(),
        listed: Semaphore::new(0),
        snapshot_read: Some((read_started.clone(), release.clone())),
    });
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    read_started.acquire().await?.forget();
    let overlapped =
        tokio::time::timeout(Duration::from_secs(1), peers.read_started.acquire()).await;
    release.add_permits(8);
    let result = task.await??;
    overlapped
        .context("replica reads waited for the bucket lookup")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    Ok(())
}

#[tokio::test]
async fn a_fast_valid_replica_bypasses_a_stalled_replica() -> Result<()> {
    let (mut f, peers, scope, bytes) = replicated().await?;
    let owner = f.runtime.load(&f.actor.storage_key()).await?.unwrap().1;
    let stream = owner.stream()?;
    let reference = stream.snapshot(&bytes)?;
    let targets = add_replica(&mut f, &peers, &scope, &stream, &bytes).await?;
    peers
        .read_release
        .lock()
        .unwrap()
        .insert("old".into(), Arc::new(Semaphore::new(0)));
    f.runtime.peers = peers;
    let recovered = tokio::time::timeout(
        Duration::from_secs(1),
        f.runtime.recover_snapshot(&targets, &reference),
    )
    .await??;
    assert_eq!(recovered, bytes);
    assert_eq!(f.bucket.get(&reference.object).await?.unwrap().bytes, bytes);
    Ok(())
}

#[tokio::test]
async fn a_corrupt_replica_cannot_win_the_snapshot_race() -> Result<()> {
    let (mut f, peers, scope, bytes) = replicated().await?;
    let stream = f
        .runtime
        .load(&f.actor.storage_key())
        .await?
        .unwrap()
        .1
        .stream()?;
    let reference = stream.snapshot(&bytes)?;
    let targets = add_replica(&mut f, &peers, &scope, &stream, &bytes).await?;
    peers
        .read_override
        .lock()
        .unwrap()
        .insert("old".into(), b"corrupt".to_vec());
    f.runtime.peers = peers;
    assert_eq!(
        f.runtime.recover_snapshot(&targets, &reference).await?,
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn a_bucket_hit_does_not_wait_for_stalled_replicas() -> Result<()> {
    let (mut f, peers, scope, bytes) = replicated().await?;
    let stream = f
        .runtime
        .load(&f.actor.storage_key())
        .await?
        .unwrap()
        .1
        .stream()?;
    let reference = stream.snapshot(&bytes)?;
    f.bucket
        .inner
        .compare_and_swap(&reference.object, None, bytes.clone())
        .await?;
    let targets = f.runtime.replica_members(&scope).await?;
    peers
        .read_release
        .lock()
        .unwrap()
        .insert("old".into(), Arc::new(Semaphore::new(0)));
    f.runtime.peers = peers;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            f.runtime.recover_snapshot(&targets, &reference)
        )
        .await??,
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn recovery_cannot_close_the_session_or_publish_ownership_before_restoration() -> Result<()> {
    let (mut f, peers, scope, bytes) = replicated().await?;
    let release = Arc::new(Semaphore::new(0));
    peers
        .read_release
        .lock()
        .unwrap()
        .insert("old".into(), release.clone());
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    peers.read_started.acquire().await?.forget();
    let key = format!(
        "{}.json",
        crate::storage_paths::session(&scope.host, &scope.session)
    );
    let object = f.bucket.get(&key).await?.unwrap();
    let session: session::Session = serde_json::from_slice(&object.bytes)?;
    assert!(session.state == session::RecoveryState::Recovering);
    let owner = f
        .bucket
        .get(&ownership_key(&f.actor.storage_key())?)
        .await?
        .unwrap();
    let owner: Ownership = serde_json::from_slice(&owner.bytes)?;
    assert_eq!(owner.lease.id, scope.host);
    release.add_permits(1);
    assert_eq!(task.await??.state.unwrap().as_ref(), bytes);
    let session: session::Session =
        serde_json::from_slice(&f.bucket.get(&key).await?.unwrap().bytes)?;
    assert!(session.state == session::RecoveryState::Sealed);
    Ok(())
}

#[tokio::test]
async fn a_successful_claim_with_no_sealed_witness_cannot_activate() -> Result<()> {
    let (mut f, peers, scope, _) = replicated().await?;
    peers.fail_old_seal.store(true, Ordering::SeqCst);
    f.runtime.peers = peers;
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    assert!(task.await?.is_err());
    let owner = f
        .bucket
        .get(&ownership_key(&f.actor.storage_key())?)
        .await?
        .unwrap();
    let owner: Ownership = serde_json::from_slice(&owner.bytes)?;
    assert_eq!(owner.lease.id, scope.host);
    Ok(())
}

#[tokio::test]
async fn cancelled_recovery_can_resume_without_losing_replica_only_state() -> Result<()> {
    let (mut f, peers, _, bytes) = replicated().await?;
    let release = Arc::new(Semaphore::new(0));
    peers
        .read_release
        .lock()
        .unwrap()
        .insert("old".into(), release.clone());
    f.runtime.peers = peers.clone();
    f.clock.0.store(11_000, Ordering::SeqCst);
    let runtime = Arc::new(f.runtime);
    let actor = f.actor.clone();
    let first = runtime.clone();
    let task = tokio::spawn(async move {
        first
            .register_activation(&actor, &request("cancelled"), "us-east", false)
            .await
    });
    peers.read_started.acquire().await?.forget();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    release.add_permits(8);
    let result = runtime
        .register_activation(&f.actor, &request("retry"), "us-east", false)
        .await?;
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    Ok(())
}

#[tokio::test]
async fn concurrent_recoveries_publish_only_one_new_owner() -> Result<()> {
    let (mut f, peers, _, bytes) = replicated().await?;
    f.runtime.peers = peers;
    f.clock.0.store(11_000, Ordering::SeqCst);
    let first = request("contender-one");
    let second = request("contender-two");
    let results = tokio::join!(
        f.runtime
            .register_activation(&f.actor, &first, "us-east", false),
        f.runtime
            .register_activation(&f.actor, &second, "us-east", false),
    );
    let winners: Vec<_> = [results.0, results.1]
        .into_iter()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].state.as_ref().unwrap().as_ref(), bytes);
    assert_eq!(
        f.runtime
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap()
            .lease,
        winners[0].placement.lease
    );
    Ok(())
}

async fn add_replica(
    f: &mut Fixture,
    peers: &ObservedPeers,
    scope: &ReplicaScope,
    stream: &ReplicaStream,
    bytes: &[u8],
) -> Result<Vec<ReplicaTarget>> {
    let target = ReplicaTarget {
        host_id: "second".into(),
        url: "http://second".into(),
        region: "us-central".into(),
    };
    let store = Arc::new(
        crate::replication::FileReplicaStore::open(f._directory.path().join("second"), 4096)
            .await?,
    );
    store.initialize_session(&scope.identity()).await?;
    store.append(stream, bytes).await?;
    peers
        .stores
        .lock()
        .unwrap()
        .insert(target.host_id.clone(), store);
    let mut targets = f.runtime.replica_members(scope).await?;
    targets.push(target);
    Ok(targets)
}

fn activate(
    runtime: RuntimeStorage,
    actor: ActorKey,
    clock: Arc<TestClock>,
) -> tokio::task::JoinHandle<Result<LoadedActor>> {
    clock.0.store(11_000, Ordering::SeqCst);
    tokio::spawn(async move {
        runtime
            .register_activation(&actor, &request("next"), "us-east", false)
            .await
    })
}

async fn replicated() -> Result<(Fixture, Arc<ObservedPeers>, ReplicaScope, Vec<u8>)> {
    let mut f = Fixture::new()?;
    let lease = f
        .runtime
        .register_activation(&f.actor, &request("first"), "us-east", true)
        .await?
        .placement
        .lease;
    let scope = ReplicaScope {
        actor: f.actor.clone(),
        host: lease.id,
        session: lease.session_id,
        region: "us-east".into(),
    };
    let target = ReplicaTarget {
        host_id: "old".into(),
        url: "http://old".into(),
        region: "us-east".into(),
    };
    let store = Arc::new(
        crate::replication::FileReplicaStore::open(f._directory.path().join("replica"), 4096)
            .await?,
    );
    store.initialize_session(&scope.identity()).await?;
    f.runtime.fleet = Arc::new(ReplicaSet(vec![target.clone()]));
    f.runtime
        .register_initial_replicas(&scope, vec![target.clone()])
        .await?;
    let owner = f.runtime.load(&f.actor.storage_key()).await?.unwrap().1;
    let bytes = snapshot(1)?;
    store.append(&owner.stream()?, &bytes).await?;
    let peers = Arc::new(ObservedPeers {
        stores: Mutex::new(HashMap::from([(target.host_id, store)])),
        sealed: Semaphore::new(0),
        read_started: Semaphore::new(0),
        read_release: Mutex::new(HashMap::new()),
        read_override: Mutex::new(HashMap::new()),
        fail_old_seal: AtomicBool::new(false),
    });
    Ok((f, peers, scope, bytes))
}

fn snapshot(version: u64) -> Result<Vec<u8>> {
    StateSnapshot::new(
        version,
        1,
        format!("request-{version}"),
        serde_json::json!({"count":version}),
        serde_json::json!(version),
    )?
    .encode()
}

struct ObservedBucket {
    inner: Arc<CountedBucket>,
    listed: Semaphore,
    snapshot_read: Option<(Arc<Semaphore>, Arc<Semaphore>)>,
}

#[async_trait]
impl Bucket for ObservedBucket {
    async fn get(&self, key: &str) -> Result<Option<crate::bucket::BucketObject>> {
        if key.contains("/snapshots/")
            && let Some((entered, release)) = &self.snapshot_read
        {
            entered.add_permits(1);
            release.acquire().await?.forget();
        }
        self.inner.get(key).await
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
        self.listed.add_permits(1);
        self.inner.list(prefix).await
    }
}

struct ObservedPeers {
    stores: Mutex<HashMap<String, Arc<crate::replication::FileReplicaStore>>>,
    sealed: Semaphore,
    read_started: Semaphore,
    read_release: Mutex<HashMap<String, Arc<Semaphore>>>,
    read_override: Mutex<HashMap<String, Vec<u8>>>,
    fail_old_seal: AtomicBool,
}

impl ObservedPeers {
    fn store(&self, peer: &ReplicaTarget) -> Arc<crate::replication::FileReplicaStore> {
        self.stores.lock().unwrap()[&peer.host_id].clone()
    }
}

#[async_trait]
impl ReplicaPeers for ObservedPeers {
    async fn initialize(&self, peer: &ReplicaTarget, session: &str) -> Result<()> {
        self.store(peer).initialize_session(session).await
    }
    async fn head(
        &self,
        peer: &ReplicaTarget,
        stream: &ReplicaStream,
    ) -> Result<crate::replication::StreamHead> {
        self.store(peer).stream_head(stream).await
    }
    async fn seal(
        &self,
        peer: &ReplicaTarget,
        session: &str,
    ) -> Result<crate::replication::SessionHead> {
        self.sealed.add_permits(1);
        ensure!(
            peer.host_id != "old" || !self.fail_old_seal.load(Ordering::SeqCst),
            "old replica unavailable"
        );
        self.store(peer).seal_session(session).await
    }
    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        self.read_started.add_permits(1);
        let release = self
            .read_release
            .lock()
            .unwrap()
            .get(&peer.host_id)
            .cloned();
        if let Some(release) = release {
            release.acquire().await?.forget();
        }
        if let Some(bytes) = self.read_override.lock().unwrap().get(&peer.host_id) {
            return Ok(bytes.clone());
        }
        self.store(peer)
            .read(object)
            .await?
            .context("snapshot missing")
    }
}
