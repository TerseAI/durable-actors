use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use moka::{future::Cache, ops::compute::Op};
use serde::{Deserialize, Serialize};

use crate::{actor::ActorKey, host_leases::WaitingOperation, sandbox::ResourceLimits};

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleKind {
    Started,
    Heartbeat,
    Draining,
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleReport {
    pub kind: LifecycleKind,
    pub owner_epoch: u64,
    pub sequence: u64,
    pub started_at_ms: u64,
    pub occurred_at_ms: u64,
    pub resources: ResourceLimits,
    pub resident: Option<bool>,
    pub waiting: Vec<WaitingOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleEvent {
    pub event_id: String,
    pub actor: ActorKey,
    pub host_id: String,
    pub session_id: String,
    pub region: String,
    pub received_at_ms: u64,
    #[serde(flatten)]
    pub report: LifecycleReport,
}

#[async_trait]
pub(crate) trait LifecyclePersistence: Send + Sync {
    async fn record(&self, event: &LifecycleEvent) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct LifecycleStore(Arc<dyn LifecyclePersistence>);

impl Default for LifecycleStore {
    fn default() -> Self {
        Self::new(Arc::new(MemoryLifecyclePersistence::default()))
    }
}

impl LifecycleStore {
    pub(crate) fn new(persistence: Arc<dyn LifecyclePersistence>) -> Self {
        Self(persistence)
    }

    pub(crate) async fn record(&self, event: LifecycleEvent) -> Result<()> {
        ensure!(event.report.sequence > 0, "invalid lifecycle sequence");
        ensure!(event.report.owner_epoch > 0, "invalid ownership epoch");
        ensure!(
            event.report.occurred_at_ms >= event.report.started_at_ms,
            "invalid lifecycle time range"
        );
        self.0.record(&event).await
    }
}

struct MemoryLifecyclePersistence(Cache<ActorKey, LifecycleEvent>);

impl Default for MemoryLifecyclePersistence {
    fn default() -> Self {
        Self(
            Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(Duration::from_secs(86_400))
                .build(),
        )
    }
}

#[async_trait]
impl LifecyclePersistence for MemoryLifecyclePersistence {
    async fn record(&self, event: &LifecycleEvent) -> Result<()> {
        self.0
            .entry(event.actor.clone())
            .and_compute_with(|previous| async move {
                if previous.is_some_and(|previous| {
                    let old = &previous.value().report;
                    (old.owner_epoch, old.sequence)
                        >= (event.report.owner_epoch, event.report.sequence)
                }) {
                    Op::Nop
                } else {
                    Op::Put(event.clone())
                }
            })
            .await;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/actor_lifecycle.rs"]
mod tests;
