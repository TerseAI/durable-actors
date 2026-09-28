use super::*;
use crate::{replication::ReplicaStore, state_log::StateSnapshot};
use tokio::sync::Semaphore;

#[tokio::test]
async fn a_lost_claim_discards_speculative_work_and_retries_changed_membership() -> Result<()> {
    for fail_seal in [false, true] {
        lost_claim(fail_seal).await?;
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_download_overlaps_sealing_and_waits_for_the_newer_witness() -> Result<()> {
    let newer = snapshot(2)?;
    let (mut f, peers, scope, release) = delayed_seal(&newer).await?;
    let stream = f.stream().await?;
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    let overlapped =
        tokio::time::timeout(Duration::from_secs(1), peers.read_started.acquire()).await;
    assert!(!task.is_finished());
    let key = session_key(&scope);
    let session: session::Session =
        serde_json::from_slice(&f.bucket.get(&key).await?.unwrap().bytes)?;
    assert!(session.state != session::RecoveryState::Sealed);
    let owner: Ownership = serde_json::from_slice(
        &f.bucket
            .get(&ownership_key(&f.actor.storage_key())?)
            .await?
            .unwrap()
            .bytes,
    )?;
    assert_eq!(owner.lease.id, scope.host);
    assert!(f.bucket.get(&stream.object(1)).await?.is_none());
    release.add_permits(1);
    let result = task.await??;
    overlapped
        .context("snapshot download waited for the remaining seal")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), newer);
    assert!(f.bucket.get(&stream.object(1)).await?.is_none());
    assert_eq!(f.bucket.get(&stream.object(2)).await?.unwrap().bytes, newer);
    Ok(())
}

#[tokio::test]
async fn a_conflicting_late_seal_discards_the_prefetched_snapshot() -> Result<()> {
    let conflicting = StateSnapshot::new(
        1,
        1,
        "conflicting".into(),
        serde_json::json!({"count":99}),
        serde_json::json!(99),
    )?
    .encode()?;
    let (mut f, peers, scope, release) = delayed_seal(&conflicting).await?;
    let stream = f.stream().await?;
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    let overlapped =
        tokio::time::timeout(Duration::from_secs(1), peers.read_started.acquire()).await;
    release.add_permits(1);
    assert!(task.await?.is_err());
    overlapped
        .context("snapshot download waited for the conflicting seal")??
        .forget();
    assert!(f.bucket.get(&stream.object(1)).await?.is_none());
    let key = session_key(&scope);
    let session: session::Session =
        serde_json::from_slice(&f.bucket.get(&key).await?.unwrap().bytes)?;
    assert!(session.state == session::RecoveryState::Recovering);
    let owner: Ownership = serde_json::from_slice(
        &f.bucket
            .get(&ownership_key(&f.actor.storage_key())?)
            .await?
            .unwrap()
            .bytes,
    )?;
    assert_eq!(owner.lease.id, scope.host);
    Ok(())
}

#[tokio::test]
async fn independent_snapshots_restore_concurrently_before_recovery_can_close() -> Result<()> {
    let (mut f, peers, scope, bytes) = replicated().await?;
    let mut owner = f.runtime.load(&f.actor.storage_key()).await?.unwrap().1;
    let store = peers.stores.lock().unwrap()["old"].clone();
    let mut objects = vec![owner.stream()?.object(1)];
    for index in 1..12 {
        owner.actor.actor_id = format!("parallel-{index}");
        let stream = owner.stream()?;
        store.append(&stream, &bytes).await?;
        objects.push(stream.object(1));
    }
    let read_release = Arc::new(Semaphore::new(0));
    peers
        .read_release
        .lock()
        .unwrap()
        .insert("old".into(), read_release.clone());
    let write_started = Arc::new(Semaphore::new(0));
    let write_release = Arc::new(Semaphore::new(0));
    f.runtime.authority = Arc::new(ObservedBucket {
        inner: f.bucket.clone(),
        listed: Semaphore::new(0),
        snapshot_read: None,
        snapshot_write: Some((write_started.clone(), write_release.clone())),
    });
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    let reads =
        tokio::time::timeout(Duration::from_secs(1), peers.read_started.acquire_many(12)).await;
    read_release.add_permits(12);
    let writes = tokio::time::timeout(Duration::from_secs(1), write_started.acquire_many(12)).await;
    let key = session_key(&scope);
    let session: session::Session =
        serde_json::from_slice(&f.bucket.get(&key).await?.unwrap().bytes)?;
    assert!(session.state == session::RecoveryState::Recovering);
    let owner: Ownership = serde_json::from_slice(
        &f.bucket
            .get(&ownership_key(&f.actor.storage_key())?)
            .await?
            .unwrap()
            .bytes,
    )?;
    assert_eq!(owner.lease.id, scope.host);
    assert!(!task.is_finished());
    for object in &objects {
        assert!(f.bucket.get(object).await?.is_none());
    }
    write_release.add_permits(12);
    let result = tokio::time::timeout(Duration::from_secs(5), task).await???;
    reads
        .context("independent downloads waited for earlier downloads")??
        .forget();
    writes
        .context("independent snapshot writes waited for earlier writes")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    let session: session::Session =
        serde_json::from_slice(&f.bucket.get(&key).await?.unwrap().bytes)?;
    assert!(session.state == session::RecoveryState::Sealed);
    for object in objects {
        assert_eq!(f.bucket.get(&object).await?.unwrap().bytes, bytes);
    }
    Ok(())
}

#[tokio::test]
async fn bucket_and_replica_reads_overlap_and_select_the_newer_snapshot() -> Result<()> {
    let (mut f, peers, _, _) = replicated().await?;
    let bytes = snapshot(2)?;
    let stream = f.stream().await?;
    f.bucket
        .inner
        .compare_and_swap(&stream.object(1), None, snapshot(1)?)
        .await?;
    let store = peers.stores.lock().unwrap()["old"].clone();
    store.append(&stream, &bytes).await?;
    let read_started = Arc::new(Semaphore::new(0));
    let read_release = Arc::new(Semaphore::new(0));
    let bucket = Arc::new(ObservedBucket {
        inner: f.bucket.clone(),
        listed: Semaphore::new(0),
        snapshot_read: Some((read_started.clone(), read_release.clone())),
        snapshot_write: None,
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
    let bucket_read = tokio::time::timeout(Duration::from_secs(1), read_started.acquire()).await;
    let replica_read =
        tokio::time::timeout(Duration::from_secs(1), peers.read_started.acquire()).await;
    let listing = tokio::time::timeout(Duration::from_secs(1), bucket.listed.acquire()).await;
    release.add_permits(1);
    read_release.add_permits(2);
    let result = task.await??;
    bucket_read
        .context("bucket lookup waited for replica restoration")??
        .forget();
    replica_read
        .context("replica reads waited for the bucket lookup")??
        .forget();
    listing
        .context("snapshot listing waited for replica restoration")??
        .forget();
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    Ok(())
}

#[tokio::test]
async fn snapshot_reads_use_a_verified_source_without_waiting_for_stalled_replicas() -> Result<()> {
    #[derive(Debug)]
    enum Source {
        HealthyReplica,
        AfterCorruptReplica,
        Bucket,
    }
    for source in [
        Source::HealthyReplica,
        Source::AfterCorruptReplica,
        Source::Bucket,
    ] {
        let (mut f, peers, scope, bytes) = replicated().await?;
        let stream = f.stream().await?;
        let reference = stream.snapshot(&bytes)?;
        let targets = match source {
            Source::Bucket => {
                f.bucket
                    .inner
                    .compare_and_swap(&reference.object, None, bytes.clone())
                    .await?;
                f.runtime.replica_members(&scope).await?
            }
            _ => add_replica(&f, &peers, &scope, &stream, &bytes).await?,
        };
        match source {
            Source::AfterCorruptReplica => {
                peers
                    .read_override
                    .lock()
                    .unwrap()
                    .insert("old".into(), b"corrupt".to_vec());
            }
            _ => {
                peers
                    .read_release
                    .lock()
                    .unwrap()
                    .insert("old".into(), Arc::new(Semaphore::new(0)));
            }
        }
        f.runtime.peers = peers;
        let recovered = tokio::time::timeout(
            Duration::from_secs(1),
            f.runtime.recover_snapshot(&targets, &reference),
        )
        .await
        .with_context(|| format!("snapshot recovery timed out using {source:?}"))??;
        assert_eq!(recovered, bytes, "{source:?}");
        assert_eq!(f.bucket.get(&reference.object).await?.unwrap().bytes, bytes);
    }
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

async fn lost_claim(fail_seal: bool) -> Result<()> {
    let (mut f, peers, scope, _) = replicated().await?;
    let stream = f.stream().await?;
    let bytes = snapshot(2)?;
    let replacement = add_replica(&f, &peers, &scope, &stream, &bytes)
        .await?
        .pop()
        .unwrap();
    let store = peers.store(&replacement);
    peers.fail_old_seal.store(fail_seal, Ordering::SeqCst);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), release.clone()));
    f.runtime.peers = peers.clone();
    let task = activate(f.runtime, f.actor.clone(), f.clock.clone());
    entered.acquire().await?.forget();
    let overlapped = tokio::time::timeout(Duration::from_secs(1), peers.sealed.acquire()).await;
    let key = session_key(&scope);
    let object = f.bucket.inner.get(&key).await?.unwrap();
    let mut session: session::Session = serde_json::from_slice(&object.bytes)?;
    let downloaded = if fail_seal {
        None
    } else {
        Some(tokio::time::timeout(Duration::from_secs(1), peers.read_started.acquire()).await)
    };
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
    if let Some(downloaded) = downloaded {
        downloaded
            .context("snapshot download waited for the claim")??
            .forget();
    }
    assert_eq!(result.state.unwrap().as_ref(), bytes);
    assert!(f.bucket.get(&stream.object(1)).await?.is_none());
    assert!(store.append(&stream, &snapshot(3)?).await.is_err());
    Ok(())
}

async fn delayed_seal(
    bytes: &[u8],
) -> Result<(Fixture, Arc<ObservedPeers>, ReplicaScope, Arc<Semaphore>)> {
    let (f, peers, scope, _) = replicated().await?;
    let stream = f.stream().await?;
    let targets = add_replica(&f, &peers, &scope, &stream, bytes).await?;
    let key = session_key(&scope);
    let object = f.bucket.get(&key).await?.unwrap();
    let mut session: session::Session = serde_json::from_slice(&object.bytes)?;
    session.replicas = targets;
    assert!(
        f.bucket
            .compare_and_swap(&key, Some(object.generation), serde_json::to_vec(&session)?)
            .await?
    );
    let release = Arc::new(Semaphore::new(0));
    peers
        .seal_release
        .lock()
        .unwrap()
        .insert("second".into(), release.clone());
    Ok((f, peers, scope, release))
}

async fn add_replica(
    f: &Fixture,
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

pub(super) async fn replicated() -> Result<(Fixture, Arc<ObservedPeers>, ReplicaScope, Vec<u8>)> {
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
    let bytes = snapshot(1)?;
    store.append(&f.stream().await?, &bytes).await?;
    let peers = Arc::new(ObservedPeers {
        stores: Mutex::new(HashMap::from([(target.host_id, store)])),
        sealed: Semaphore::new(0),
        seal_release: Mutex::new(HashMap::new()),
        read_started: Semaphore::new(0),
        read_release: Mutex::new(HashMap::new()),
        read_override: Mutex::new(HashMap::new()),
        fail_old_seal: AtomicBool::new(false),
    });
    Ok((f, peers, scope, bytes))
}

fn session_key(scope: &ReplicaScope) -> String {
    format!(
        "{}.json",
        crate::storage_paths::session(&scope.host, &scope.session)
    )
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
    snapshot_write: Option<(Arc<Semaphore>, Arc<Semaphore>)>,
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
        if key.contains("/snapshots/")
            && let Some((entered, release)) = &self.snapshot_write
        {
            entered.add_permits(1);
            release.acquire().await?.forget();
        }
        self.inner.compare_and_swap(key, generation, bytes).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = self.inner.list(prefix).await?;
        self.listed.add_permits(1);
        Ok(keys)
    }
}

pub(super) struct ObservedPeers {
    stores: Mutex<HashMap<String, Arc<crate::replication::FileReplicaStore>>>,
    sealed: Semaphore,
    seal_release: Mutex<HashMap<String, Arc<Semaphore>>>,
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
        let release = self
            .seal_release
            .lock()
            .unwrap()
            .get(&peer.host_id)
            .cloned();
        if let Some(release) = release {
            release.acquire().await?.forget();
        }
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
