//! Per-actor CPU, memory and kill events, measured outside the sandbox so an actor that runs out
//! of memory or hangs can't lose its own record. On Kubernetes an OpenTelemetry Collector on each
//! sandbox node samples pods; the control plane records which actor each pod ran and which pods
//! the platform killed, and joins them when it reads.

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{actor::ActorKey, actor_lifecycle::LifecycleStore};

/// How often the node collector samples pods.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
/// Stored series are bucketed so a response holds at most this many points.
pub(crate) const MAX_POINTS: u64 = 500;

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
}

#[async_trait]
pub(crate) trait ResourcePersistence: Send + Sync {
    async fn record_assignment(&self, assignment: SandboxAssignment) -> Result<()>;
    async fn record_kill(&self, kill: SandboxKill) -> Result<()>;
    async fn history(&self, actor: &ActorKey, from_ms: u64, to_ms: u64) -> Result<ResourceHistory>;
}

/// Without a node collector (local development, or no analytics backend) there is no usage to
/// keep, only the database sizes in lifecycle reports.
pub(crate) struct NoResources;

#[async_trait]
impl ResourcePersistence for NoResources {
    async fn record_assignment(&self, _: SandboxAssignment) -> Result<()> {
        Ok(())
    }

    async fn record_kill(&self, _: SandboxKill) -> Result<()> {
        Ok(())
    }

    async fn history(&self, _: &ActorKey, _: u64, _: u64) -> Result<ResourceHistory> {
        Ok(ResourceHistory::default())
    }
}

/// Serves one actor's resources from the stores only, never from the cluster or GCS.
#[derive(Clone)]
pub(crate) struct ResourceReader {
    resources: Arc<dyn ResourcePersistence>,
    lifecycle: LifecycleStore,
}

impl Default for ResourceReader {
    fn default() -> Self {
        Self::new(Arc::new(NoResources), LifecycleStore::default())
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
        Ok(ActorResources {
            samples: history.samples,
            database,
            kills: history.kills,
        })
    }
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

/// The bucket width that keeps a range at or under `MAX_POINTS`, never finer than one sample.
pub(crate) fn bucket_ms(from_ms: u64, to_ms: u64) -> u64 {
    (to_ms.saturating_sub(from_ms) / MAX_POINTS).max(SAMPLE_INTERVAL.as_millis() as u64)
}
