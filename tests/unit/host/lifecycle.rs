use super::*;
use crate::{host::process::HostReadiness, replicas::ReplicaCluster};
use serde::Serialize;
use std::time::Instant;
use tokio::task::JoinHandle;

const WARMUPS: usize = 2;
const SAMPLES: usize = 20;
const HOT_SAMPLES: usize = 5;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local lifecycle benchmark; requires Bun and a built SDK"]
async fn benchmark_prewarmed_actor_lifecycle() -> Result<()> {
    let bench = Benchmark::new().await?;
    let mut samples = Vec::new();
    let pending = measure_new_and_hot(&bench, &mut samples).await?;
    let returning = expire_hosts(&bench, pending).await?;
    let archive_wait_ms = wait_for_archives(&bench, &returning).await?;
    measure_resumed(&bench, returning, &mut samples).await?;
    save_report(&bench, &samples, archive_wait_ms).await
}

type Pending = (ActorKey, Host, i64, usize);
type Returning = (ActorKey, HostReadiness, i64, usize);

async fn measure_new_and_hot(bench: &Benchmark, samples: &mut Vec<Sample>) -> Result<Vec<Pending>> {
    let mut pending = Vec::new();
    for iteration in 0..WARMUPS + SAMPLES {
        for method in ["increment", "read"] {
            let actor = ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: format!("{method}-{iteration}"),
            };
            let first_value = i64::from(method == "increment");
            let (host, first) = bench.activate(&actor, true, method, first_value).await?;
            record(samples, iteration, first);
            measure_hot(&host, first_value, iteration, samples).await?;
            pending.push((actor, host, first_value + HOT_SAMPLES as i64, iteration));
        }
    }
    Ok(pending)
}

async fn measure_hot(
    host: &Host,
    value: i64,
    iteration: usize,
    samples: &mut Vec<Sample>,
) -> Result<()> {
    for n in 1..=HOT_SAMPLES {
        for (method, case) in [("increment", "hot_write"), ("read", "hot_read")] {
            let start = Instant::now();
            host.invoke(method, value + n as i64).await?;
            record(samples, iteration, Sample::hot(case, start));
        }
    }
    Ok(())
}

async fn expire_hosts(bench: &Benchmark, pending: Vec<Pending>) -> Result<Vec<Returning>> {
    let mut returning = Vec::new();
    for (actor, host, value, iteration) in pending {
        let previous = host.expire(&bench.data).await?;
        returning.push((actor, previous, value, iteration));
    }
    Ok(returning)
}

async fn wait_for_archives(bench: &Benchmark, returning: &[Returning]) -> Result<f64> {
    let prefixes = returning
        .iter()
        .map(|(actor, previous, _, _)| {
            Ok(format!(
                "{}{:032x}/",
                crate::storage_paths::snapshots(actor)?,
                previous.owner_epoch
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let start = Instant::now();
    bench.replicas.wait_archived(&prefixes).await?;
    Ok(elapsed_ms(start))
}

async fn measure_resumed(
    bench: &Benchmark,
    returning: Vec<Returning>,
    samples: &mut Vec<Sample>,
) -> Result<()> {
    let mut resumed = Vec::new();
    for (actor, previous, value, iteration) in returning {
        let method = if actor.actor_id.starts_with("increment") {
            "increment"
        } else {
            "read"
        };
        let expected = value + i64::from(method == "increment");
        let (host, sample) = bench.activate(&actor, false, method, expected).await?;
        ensure!(
            host.ready.host_id != previous.host_id,
            "resume reused the original host"
        );
        ensure!(
            host.ready.owner_epoch > previous.owner_epoch,
            "resume did not advance ownership"
        );
        record(samples, iteration, sample);
        resumed.push(host);
    }
    for host in resumed {
        host.expire(&bench.data).await?;
    }
    Ok(())
}

async fn save_report(bench: &Benchmark, samples: &[Sample], archive_wait_ms: f64) -> Result<()> {
    let report = serde_json::json!({
        "scope": "local component benchmark: Rust host, prewarmed Bun, three HTTP SQLite FULL-sync replicas; filesystem authority and archive; no GKE, GCS network, control-plane routing or public gateway",
        "profile": if cfg!(debug_assertions) { "unoptimized test" } else { "optimized" },
        "code_bytes": bench.artifact.len(), "replicas": 3,
        "warmups_per_activation_case": WARMUPS, "samples_per_activation_case": SAMPLES,
        "idle_timeout_ms": 250, "archive_batch_age_ms": 10_000,
        "archive_wait_ms": archive_wait_ms,
        "summary": summaries(samples), "samples": samples,
    });
    let json = serde_json::to_string_pretty(&report)?;
    if let Ok(path) = std::env::var("TERSE_LIFECYCLE_OUTPUT") {
        tokio::fs::write(path, &json).await?;
    }
    println!("LIFECYCLE_BENCHMARK {json}");
    Ok(())
}

struct Benchmark {
    sdk: PathBuf,
    project: tempfile::TempDir,
    data: tempfile::TempDir,
    artifact: Vec<u8>,
    issuer: ActorJwtIssuer,
    replicas: ReplicaCluster,
}

impl Benchmark {
    async fn new() -> Result<Self> {
        let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk");
        let project = tempfile::tempdir_in(&sdk)?;
        let artifact = compile_counter(&sdk, project.path()).await?;
        Ok(Self {
            sdk,
            project,
            data: tempfile::tempdir()?,
            artifact,
            issuer: issuer()?,
            replicas: ReplicaCluster::start(3).await?,
        })
    }

    async fn activate(
        &self,
        actor: &ActorKey,
        new: bool,
        method: &str,
        expected: i64,
    ) -> Result<(Host, Sample)> {
        let prepared = self.prewarm(actor, new).await?;
        let start = Instant::now();
        tokio::fs::write(&prepared.warm.entrypoint, &self.artifact).await?;
        let task = tokio::spawn(serve_assigned_host(
            prepared.config,
            Some(prepared.warm),
            std::future::pending(),
        ));
        let ready = tokio::time::timeout(Duration::from_secs(10), prepared.ready).await??;
        let ready_ms = elapsed_ms(start);
        let host = Host {
            actor: actor.clone(),
            token: prepared.token,
            ready,
            task,
            client: reqwest::Client::new(),
            _socket: prepared.socket,
        };
        let invoke_start = Instant::now();
        host.invoke(method, expected).await?;
        let sample = Sample {
            case: format!(
                "{}_{}",
                if new { "cold" } else { "resume" },
                if method == "read" { "read" } else { "write" }
            ),
            total_ms: elapsed_ms(start),
            ready_ms: Some(ready_ms),
            invoke_ms: Some(elapsed_ms(invoke_start)),
        };
        Ok((host, sample))
    }

    async fn prewarm(&self, actor: &ActorKey, new: bool) -> Result<Prepared> {
        let socket = tempfile::tempdir_in("/tmp")?;
        let ipc = ActorExecutorListener::bind(socket.path().join("executor.sock")).await?;
        let javascript = Command::new("bun")
            .args([
                "--eval",
                "await import(process.env.DURABLE_ACTORS_SDK_HOST).then(m => m.runGenericHost())",
            ])
            .env("DURABLE_ACTORS_SDK_HOST", self.sdk.join("dist/host.js"))
            .env(
                "DURABLE_ACTORS_EXECUTOR_SOCKET",
                socket.path().join("executor.sock"),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let executor = tokio::time::timeout(Duration::from_secs(10), ipc.accept_warm()).await??;
        let host_id = HostId::new(format!("host.v3.benchmark.{}", uuid::Uuid::new_v4()));
        let session = uuid::Uuid::new_v4().to_string();
        let token = self
            .issuer
            .issue_host(&host_id, &session, "test", "north-america-east", actor)?
            .token;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let environment: HashMap<String, String> = serde_json::from_value(serde_json::json!({
            "DURABLE_ACTORS_CONTROL_PLANE_URL": "http://127.0.0.1:1",
            "DURABLE_ACTORS_HOST_TOKEN": token,
            "DURABLE_ACTORS_JWT_PUBLIC_KEYS": self.issuer.verifier_keys_json()?,
            "DURABLE_ACTORS_HOST_ID": host_id.as_str(), "DURABLE_ACTORS_SESSION_ID": session,
            "DURABLE_ACTORS_HOST_ROUTE": format!("http://{}", listener.local_addr()?),
            "DURABLE_ACTORS_JWT_ISSUER": "issuer", "DURABLE_ACTORS_INVOKE_JWT_AUDIENCE": "invocation",
            "DURABLE_ACTORS_SOCKET_JWT_AUDIENCE": "authority:websocket",
            "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS": "250",
            "DURABLE_ACTORS_ACTOR": serde_json::to_string(actor)?,
            "DURABLE_ACTORS_ACTOR_IS_NEW": new.to_string(),
            "DURABLE_ACTORS_RUNTIME_CONFIG": serde_json::json!({
                "bucket": {"type": "file", "directory": self.data.path()}, "region": "north-america-east",
                "persistence": self.replicas.config, "replicaToken": self.replicas.access.scoped(actor)?, "token": null
            }).to_string()
        }))?;
        let (send, ready) = tokio::sync::oneshot::channel();
        let entrypoint = self.project.path().join(format!("{host_id}.mjs"));
        ensure!(
            !entrypoint.exists(),
            "customer code existed before assignment"
        );
        Ok(Prepared {
            config: ActorHostConfig::from_lookup(|key| environment.get(key).cloned())?,
            token,
            ready,
            socket,
            warm: WarmHost {
                readiness: Some(send),
                listener,
                executor,
                javascript,
                entrypoint: entrypoint.to_str().unwrap().into(),
                storage: WarmGcs::new().await?,
                control_plane: None,
            },
        })
    }
}

struct Prepared {
    config: ActorHostConfig,
    warm: WarmHost,
    ready: tokio::sync::oneshot::Receiver<HostReadiness>,
    token: String,
    socket: tempfile::TempDir,
}

struct Host {
    actor: ActorKey,
    token: String,
    ready: HostReadiness,
    task: JoinHandle<Result<()>>,
    client: reqwest::Client,
    _socket: tempfile::TempDir,
}

impl Host {
    async fn invoke(&self, method: &str, expected: i64) -> Result<()> {
        let url = format!(
            "{}/v1/projects/{}/actors/{}/{}/invoke",
            self.ready.route, self.actor.project_id, self.actor.actor_name, self.actor.actor_id
        );
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({
                "requestId": uuid::Uuid::new_v4().to_string(), "ownerEpoch": self.ready.owner_epoch,
                "method": method, "args": []
            }))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;
        let status = response.status();
        let body: serde_json::Value = response.json().await?;
        ensure!(status.is_success(), "{method} failed ({status}): {body}");
        ensure!(
            body == serde_json::json!({"type": "completed", "result": expected}),
            "{method}: expected {expected}, got {body}"
        );
        Ok(())
    }

    async fn expire(self, data: &tempfile::TempDir) -> Result<HostReadiness> {
        tokio::time::timeout(Duration::from_secs(10), self.task).await???;
        let bucket = FileBucket::new(data.path().to_path_buf())?;
        let object = bucket
            .get(&crate::storage_paths::owner(&self.actor.storage_key())?)
            .await?
            .context("missing ownership record")?;
        let record: serde_json::Value = serde_json::from_slice(&object.bytes)?;
        ensure!(
            record["lease"]["expires_at_ms"] == 0 && record["sealed"] == true,
            "idle host did not release and seal ownership: {record}"
        );
        Ok(self.ready)
    }
}

#[derive(Serialize)]
struct Sample {
    case: String,
    total_ms: f64,
    ready_ms: Option<f64>,
    invoke_ms: Option<f64>,
}
impl Sample {
    fn hot(case: &str, start: Instant) -> Self {
        Self {
            case: case.into(),
            total_ms: elapsed_ms(start),
            ready_ms: None,
            invoke_ms: None,
        }
    }
}
fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
fn record(samples: &mut Vec<Sample>, iteration: usize, sample: Sample) {
    if iteration >= WARMUPS {
        samples.push(sample);
    }
}
fn summaries(samples: &[Sample]) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for case in [
        "cold_write",
        "cold_read",
        "hot_write",
        "hot_read",
        "resume_write",
        "resume_read",
    ] {
        let mut values: Vec<_> = samples
            .iter()
            .filter(|s| s.case == case)
            .map(|s| s.total_ms)
            .collect();
        values.sort_by(f64::total_cmp);
        let n = values.len();
        result.insert(case.into(), serde_json::json!({
            "n": n, "p50_ms": (values[(n - 1) / 2] + values[n / 2]) / 2.0,
            "p95_ms": values[(n as f64 * 0.95).ceil() as usize - 1], "min_ms": values[0], "max_ms": values[n - 1]
        }));
    }
    result.into()
}
