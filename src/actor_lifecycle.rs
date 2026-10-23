use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use moka::{future::Cache, ops::compute::Op};
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    host_leases::WaitingOperation,
    placement::{
        ActorInstanceOverview, ActorInventory, ActorInventoryReader, ActorInventorySnapshot,
        ActorResidency,
    },
    sandbox::ResourceLimits,
};

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER_MS: u64 = 35_000;

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
    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>>;
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

#[async_trait]
impl ActorInventoryReader for LifecycleStore {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let now = SystemClock.now_ms()?;
        let mut actors = BTreeMap::<String, ActorInventory>::new();
        for event in self.0.latest(project).await? {
            let status = residency(&event, now);
            let actor = actors
                .entry(event.actor.actor_name.clone())
                .or_insert_with(|| ActorInventory {
                    actor_name: event.actor.actor_name.clone(),
                    ..Default::default()
                });
            match status {
                ActorResidency::Live => actor.live += 1,
                ActorResidency::Dormant => actor.dormant += 1,
                ActorResidency::Unknown => actor.unknown += 1,
            }
            actor.instances.push(ActorInstanceOverview {
                actor_id: event.actor.actor_id,
                status,
                connections: Vec::new(),
                waiting: match status {
                    ActorResidency::Unknown => None,
                    ActorResidency::Dormant if event.report.kind == LifecycleKind::Stopped => {
                        Some(Vec::new())
                    }
                    _ => Some(event.report.waiting),
                },
            });
        }
        Ok(ActorInventorySnapshot {
            actors: actors.into_values().collect(),
            connections_complete: true,
        })
    }
}

fn residency(event: &LifecycleEvent, now: u64) -> ActorResidency {
    if event.report.kind == LifecycleKind::Stopped {
        return ActorResidency::Dormant;
    }
    if now.saturating_sub(event.received_at_ms) > STALE_AFTER_MS {
        return ActorResidency::Unknown;
    }
    match event.report.resident {
        Some(true) => ActorResidency::Live,
        Some(false) => ActorResidency::Dormant,
        None => ActorResidency::Unknown,
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

    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>> {
        let mut events: Vec<_> = self
            .0
            .iter()
            .filter(|(actor, _)| actor.project_id == project)
            .map(|(_, event)| event)
            .collect();
        events.sort_by(|a, b| {
            (&a.actor.actor_name, &a.actor.actor_id).cmp(&(&b.actor.actor_name, &b.actor.actor_id))
        });
        Ok(events)
    }
}

#[cfg(test)]
#[path = "../tests/unit/actor_lifecycle.rs"]
mod tests;
