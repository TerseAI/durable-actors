//! Per-actor CPU, memory and kill events, measured outside the sandbox so an actor that runs out
//! of memory or hangs can't lose its own record. On Kubernetes an OpenTelemetry Collector on each
//! sandbox node samples pods; the control plane records which actor each pod ran and which pods
//! the platform killed, and joins them when it reads.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{actor::ActorKey, actor_lifecycle::LifecycleStore};

/// How often usage is sampled, by the node collector on Kubernetes and by the local runtime.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
/// Stored series are bucketed so a response holds at most this many points.
pub(crate) const MAX_POINTS: u64 = 500;
const MEMORY_SAMPLES_PER_ACTOR: usize = 17_280;
const MEMORY_KILLS_PER_ACTOR: usize = 100;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceSample {
    pub actor: ActorKey,
    pub sampled_at_ms: u64,
    pub cpu_millicores: u64,
    pub memory_bytes: u64,
    pub memory_limit_bytes: Option<u64>,
}

/// The sandbox an actor activation runs in, recorded when the control plane assigns it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SandboxAssignment {
    pub actor: ActorKey,
    pub sandbox_id: String,
    pub assigned_at_ms: u64,
    pub memory_limit_bytes: Option<u64>,
}

/// A sandbox the platform stopped. Assignments name its actor when it is read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SandboxKill {
    pub sandbox_id: String,
    pub killed_at_ms: u64,
    pub reason: KillReason,
    pub message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum KillReason {
    OutOfMemory,
    Evicted,
    Killed,
}

impl KillReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::OutOfMemory => "out_of_memory",
            Self::Evicted => "evicted",
            Self::Killed => "killed",
        }
    }

    pub(crate) fn parse(name: &str) -> Result<Self> {
        match name {
            "out_of_memory" => Ok(Self::OutOfMemory),
            "evicted" => Ok(Self::Evicted),
            "killed" => Ok(Self::Killed),
            other => Err(anyhow!("unknown kill reason {other}")),
        }
    }
}

/// One actor's resources over a time range, without pods, hosts or nodes.
#[derive(Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActorResources {
    pub samples: Vec<SamplePoint>,
    pub database: Vec<DatabasePoint>,
    pub kills: Vec<KillPoint>,
}

#[derive(Debug, Default, PartialEq)]
pub(crate) struct ResourceHistory {
    pub samples: Vec<SamplePoint>,
    pub kills: Vec<KillPoint>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SamplePoint {
    pub at_ms: u64,
    /// Unknown for a sandbox's first sample, which has no earlier CPU time to compare with.
    pub cpu_millicores: Option<u64>,
    pub memory_bytes: u64,
    pub memory_limit_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatabasePoint {
    pub at_ms: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KillPoint {
    pub at_ms: u64,
    pub reason: KillReason,
    pub message: Option<String>,
    pub memory_bytes: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
}

#[async_trait]
pub(crate) trait ResourcePersistence: Send + Sync {
    async fn record_assignment(&self, assignment: SandboxAssignment) -> Result<()>;
    async fn record_kill(&self, kill: SandboxKill) -> Result<()>;
    async fn history(&self, actor: &ActorKey, from_ms: u64, to_ms: u64) -> Result<ResourceHistory>;
}

/// Samples the runtime takes itself, where no node collector runs.
#[async_trait]
pub(crate) trait ResourceSampler: Send + Sync {
    async fn sample(&self) -> Result<Vec<ResourceSample>>;
}

#[async_trait]
pub(crate) trait SampleSink: Send + Sync {
    async fn record_samples(&self, samples: Vec<ResourceSample>) -> Result<()>;
}

/// Serves one actor's resources from the stores only, never from the cluster or GCS.
#[derive(Clone)]
pub(crate) struct ResourceReader {
    resources: Arc<dyn ResourcePersistence>,
    lifecycle: LifecycleStore,
}

impl Default for ResourceReader {
    fn default() -> Self {
        Self::new(
            Arc::new(MemoryResourcePersistence::default()),
            LifecycleStore::default(),
        )
    }
}

impl ResourceReader {
    pub(crate) fn new(resources: Arc<dyn ResourcePersistence>, lifecycle: LifecycleStore) -> Self {
        Self {
            resources,
            lifecycle,
        }
    }

    pub(crate) async fn actor(
        &self,
        actor: &ActorKey,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<ActorResources> {
        let (history, database) = tokio::try_join!(
            self.resources.history(actor, from_ms, to_ms),
            self.lifecycle.database_sizes(actor, from_ms, to_ms),
        )?;
        let kills = history
            .kills
            .into_iter()
            .map(|kill| {
                let last = history
                    .samples
                    .iter()
                    .rev()
                    .find(|sample| sample.at_ms <= kill.at_ms);
                attribute(kill, last)
            })
            .collect();
        Ok(ActorResources {
            samples: history.samples,
            database,
            kills,
        })
    }
}

/// Adds the last memory sample. gVisor can kill a sandbox at its memory limit without the kernel
/// reporting `OOMKilled`, so a plain SIGKILL at 90 % of the limit or more counts as out of memory.
pub(crate) fn attribute(mut kill: KillPoint, last: Option<&SamplePoint>) -> KillPoint {
    kill.memory_bytes = kill.memory_bytes.or(last.map(|sample| sample.memory_bytes));
    kill.memory_limit_bytes = kill
        .memory_limit_bytes
        .or(last.and_then(|sample| sample.memory_limit_bytes));
    let near_limit = matches!(
        (kill.memory_bytes, kill.memory_limit_bytes),
        (Some(used), Some(limit)) if used.saturating_mul(10) >= limit.saturating_mul(9)
    );
    if kill.reason == KillReason::Killed && near_limit {
        kill.reason = KillReason::OutOfMemory;
    }
    kill
}

/// Records the kills the platform reports until `stop`.
pub(crate) async fn record_kills(
    mut kills: broadcast::Receiver<SandboxKill>,
    persistence: Arc<dyn ResourcePersistence>,
    stop: CancellationToken,
) {
    loop {
        let kill = tokio::select! {
            _ = stop.cancelled() => return,
            kill = kills.recv() => kill,
        };
        match kill {
            Ok(kill) => {
                if let Err(error) = persistence.record_kill(kill).await {
                    tracing::warn!(error = %format!("{error:#}"), "sandbox kill was dropped");
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(skipped, "missed sandbox kill events");
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Samples the runtime's own processes on an interval, for deployments without a node collector.
pub(crate) struct ResourceMonitor {
    sampler: Arc<dyn ResourceSampler>,
    sink: Arc<dyn SampleSink>,
}

impl ResourceMonitor {
    pub(crate) fn new(sampler: Arc<dyn ResourceSampler>, sink: Arc<dyn SampleSink>) -> Self {
        Self { sampler, sink }
    }

    pub(crate) async fn run(self, stop: CancellationToken) {
        let mut ticks = tokio::time::interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticks.tick() => self.sample().await,
            }
        }
    }

    async fn sample(&self) {
        let recorded = match self.sampler.sample().await {
            Ok(samples) => self.sink.record_samples(samples).await,
            Err(error) => Err(error),
        };
        if let Err(error) = recorded {
            tracing::warn!(error = %format!("{error:#}"), "actor resource sampling failed");
        }
    }
}

/// Keeps recent resources in memory, for local development and deployments without analytics.
pub(crate) struct MemoryResourcePersistence {
    actors: Cache<ActorKey, Arc<Mutex<MemoryRecords>>>,
    sandboxes: Cache<String, SandboxAssignment>,
}

#[derive(Default)]
struct MemoryRecords {
    samples: VecDeque<ResourceSample>,
    kills: VecDeque<KillPoint>,
}

impl Default for MemoryResourcePersistence {
    fn default() -> Self {
        let idle = Duration::from_secs(86_400);
        Self {
            actors: Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(idle)
                .build(),
            sandboxes: Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(idle)
                .build(),
        }
    }
}

impl MemoryResourcePersistence {
    async fn records(&self, actor: &ActorKey) -> Arc<Mutex<MemoryRecords>> {
        self.actors
            .get_with(actor.clone(), async { Arc::default() })
            .await
    }
}

#[async_trait]
impl SampleSink for MemoryResourcePersistence {
    async fn record_samples(&self, samples: Vec<ResourceSample>) -> Result<()> {
        for sample in samples {
            let records = self.records(&sample.actor).await;
            let mut records = records.lock().map_err(|_| anyhow!("resources poisoned"))?;
            records.samples.push_back(sample);
            if records.samples.len() > MEMORY_SAMPLES_PER_ACTOR {
                records.samples.pop_front();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ResourcePersistence for MemoryResourcePersistence {
    async fn record_assignment(&self, assignment: SandboxAssignment) -> Result<()> {
        self.sandboxes
            .insert(assignment.sandbox_id.clone(), assignment)
            .await;
        Ok(())
    }

    async fn record_kill(&self, kill: SandboxKill) -> Result<()> {
        let Some(assignment) = self.sandboxes.remove(&kill.sandbox_id).await else {
            return Ok(());
        };
        let records = self.records(&assignment.actor).await;
        let mut records = records.lock().map_err(|_| anyhow!("resources poisoned"))?;
        records.kills.push_back(KillPoint {
            at_ms: kill.killed_at_ms,
            reason: kill.reason,
            message: kill.message,
            memory_bytes: None,
            memory_limit_bytes: assignment.memory_limit_bytes,
        });
        if records.kills.len() > MEMORY_KILLS_PER_ACTOR {
            records.kills.pop_front();
        }
        Ok(())
    }

    async fn history(&self, actor: &ActorKey, from_ms: u64, to_ms: u64) -> Result<ResourceHistory> {
        let Some(records) = self.actors.get(actor).await else {
            return Ok(ResourceHistory::default());
        };
        let records = records.lock().map_err(|_| anyhow!("resources poisoned"))?;
        let within = |at: u64| (from_ms..=to_ms).contains(&at);
        Ok(ResourceHistory {
            samples: records
                .samples
                .iter()
                .filter(|sample| within(sample.sampled_at_ms))
                .map(|sample| SamplePoint {
                    at_ms: sample.sampled_at_ms,
                    cpu_millicores: Some(sample.cpu_millicores),
                    memory_bytes: sample.memory_bytes,
                    memory_limit_bytes: sample.memory_limit_bytes,
                })
                .collect(),
            kills: records
                .kills
                .iter()
                .filter(|kill| within(kill.at_ms))
                .cloned()
                .collect(),
        })
    }
}

/// The bucket width that keeps a range at or under `MAX_POINTS`, never finer than one sample.
pub(crate) fn bucket_ms(from_ms: u64, to_ms: u64) -> u64 {
    (to_ms.saturating_sub(from_ms) / MAX_POINTS).max(SAMPLE_INTERVAL.as_millis() as u64)
}

#[cfg(test)]
#[path = "../tests/unit/actor_resources.rs"]
mod tests;
