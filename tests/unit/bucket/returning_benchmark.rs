use super::*;
use crate::{replication::ReplicaStore, state_log::StateSnapshot, state_transport::SnapshotWriter};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Operations {
    gets: u64,
    lists: u64,
    writes: u64,
    seals: u64,
    replica_reads: u64,
}

struct DelayedAuthority {
    inner: Arc<CountedBucket>,
    delay_ms: AtomicU64,
    operations: Arc<Mutex<Operations>>,
}

#[async_trait]
impl Bucket for DelayedAuthority {
    async fn get(&self, key: &str) -> Result<Option<crate::bucket::BucketObject>> {
        self.operations.lock().unwrap().gets += 1;
        delay(self.delay_ms.load(Ordering::SeqCst)).await;
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.operations.lock().unwrap().lists += 1;
        delay(self.delay_ms.load(Ordering::SeqCst)).await;
        self.inner.list(prefix).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        self.operations.lock().unwrap().writes += 1;
        delay(self.delay_ms.load(Ordering::SeqCst)).await;
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}

struct DelayedPeers {
    inner: DiskPeers,
    delay_ms: AtomicU64,
    operations: Arc<Mutex<Operations>>,
}

#[async_trait]
impl ReplicaPeers for DelayedPeers {
    async fn initialize(&self, target: &ReplicaTarget, session: &str) -> Result<()> {
        self.inner.initialize(target, session).await
    }
    async fn head(
        &self,
        target: &ReplicaTarget,
        stream: &ReplicaStream,
    ) -> Result<crate::replication::StreamHead> {
        delay(self.delay_ms.load(Ordering::SeqCst)).await;
        self.inner.head(target, stream).await
    }
    async fn seal(
        &self,
        target: &ReplicaTarget,
        session: &str,
    ) -> Result<crate::replication::SessionHead> {
        self.operations.lock().unwrap().seals += 1;
        delay(self.delay_ms.load(Ordering::SeqCst)).await;
        self.inner.seal(target, session).await
    }
    async fn read(&self, target: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        self.operations.lock().unwrap().replica_reads += 1;
        delay(self.delay_ms.load(Ordering::SeqCst)).await;
        self.inner.read(target, object).await
    }
}

#[tokio::test]
#[ignore = "controlled returning-actor latency benchmark; run with --ignored --nocapture"]
async fn benchmark_returning_actor_activation() -> Result<()> {
    let label = std::env::var("ACTIVATION_BENCH_LABEL").unwrap_or_else(|_| "current".into());
    let samples: usize = std::env::var("ACTIVATION_BENCH_SAMPLES")
        .unwrap_or_else(|_| "30".into())
        .parse()?;
    ensure!(samples >= 2, "benchmark requires at least two samples");
    println!(
        "version,scenario,storage_ms,replica_ms,n,p50_ms,p95_ms,shutdown_p50_ms,gets,lists,writes,seals,replica_reads"
    );
    for storage_ms in [10, 25, 50] {
        for clean in [true, false] {
            let mut activations = Vec::new();
            let mut shutdowns = Vec::new();
            let mut expected = None;
            for sample in 0..samples + 3 {
                let (activation, shutdown, operations) = measure(storage_ms, clean).await?;
                if let Some(expected) = expected {
                    assert_eq!(operations, expected);
                }
                expected = Some(operations);
                if sample >= 3 {
                    activations.push(activation);
                    shutdowns.push(shutdown);
                }
            }
            activations.sort_by(f64::total_cmp);
            shutdowns.sort_by(f64::total_cmp);
            let operations = expected.unwrap();
            let scenario = if clean {
                "clean_shutdown"
            } else {
                "replica_only_crash"
            };
            println!(
                "{label},{scenario},{storage_ms},5,{samples},{:.2},{:.2},{:.2},{},{},{},{},{}",
                percentile(&activations, 50),
                percentile(&activations, 95),
                percentile(&shutdowns, 50),
                operations.gets,
                operations.lists,
                operations.writes,
                operations.seals,
                operations.replica_reads
            );
        }
    }
    Ok(())
}

async fn measure(storage_ms: u64, clean: bool) -> Result<(f64, f64, Operations)> {
    let mut f = Fixture::new()?;
    let operations = Arc::new(Mutex::new(Operations::default()));
    let authority = Arc::new(DelayedAuthority {
        inner: f.bucket.clone(),
        delay_ms: AtomicU64::new(0),
        operations: operations.clone(),
    });
    let mut stores = HashMap::new();
    let mut targets = Vec::new();
    for region in ["us-east", "us-central"] {
        stores.insert(
            region.into(),
            Arc::new(
                crate::replication::FileReplicaStore::open(f._directory.path().join(region), 4096)
                    .await?,
            ),
        );
        targets.push(ReplicaTarget {
            host_id: region.into(),
            region: region.into(),
            url: format!("http://{region}"),
        });
    }
    let peers = Arc::new(DelayedPeers {
        inner: DiskPeers {
            stores,
            available: AtomicBool::new(true),
        },
        delay_ms: AtomicU64::new(0),
        operations: operations.clone(),
    });
    f.runtime.authority = authority.clone();
    f.runtime.peers = peers.clone();
    f.runtime.fleet = Arc::new(ReplicaSet(targets.clone()));
    let first = request("first");
    let loaded = f
        .runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    let scope = ReplicaScope {
        actor: f.actor.clone(),
        host: first.id.clone(),
        session: first.session_id.clone(),
        region: "us-east".into(),
    };
    let membership = f
        .runtime
        .replace_replicas(
            &scope,
            targets,
            None,
            &crate::state_transport::GrpcStateTransport::new(),
        )
        .await?;
    f.runtime.enable_replication(membership)?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &loaded.placement.lease, 1, 1)
        .await?;
    let bytes = StateSnapshot::new(
        1,
        1,
        "committed".into(),
        serde_json::json!({"count": 42}),
        serde_json::json!(42),
    )?
    .encode()?;
    for store in peers.inner.stores.values() {
        store.append(&plan.stream, &bytes).await?;
    }
    if clean {
        f.runtime.write_snapshot(&plan, bytes.clone()).await?;
    }
    authority.delay_ms.store(storage_ms, Ordering::SeqCst);
    peers.delay_ms.store(5, Ordering::SeqCst);
    let shutdown = if clean {
        let started = Instant::now();
        f.runtime
            .finish_activation(&f.actor, &first.id, &first.session_id)
            .await?;
        started.elapsed().as_secs_f64() * 1000.0
    } else {
        f.clock.0.store(11_000, Ordering::SeqCst);
        0.0
    };
    *operations.lock().unwrap() = Operations::default();
    let started = Instant::now();
    let (_, hint) = f
        .runtime
        .get_owner_with_hint(&f.actor.storage_key())
        .await?;
    let loaded = f
        .runtime
        .register_activation(&f.actor, &request("next"), "us-east", false, hint.as_ref())
        .await?;
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(loaded.placement.owner_epoch, 2);
    assert_eq!(loaded.state.unwrap().as_ref(), bytes);
    let counts = *operations.lock().unwrap();
    Ok((elapsed, shutdown, counts))
}

async fn delay(ms: u64) {
    if ms > 0 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

fn percentile(values: &[f64], percentile: usize) -> f64 {
    values[(values.len() * percentile).div_ceil(100) - 1]
}
