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

pub(crate) mod routing;

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
pub(crate) struct LifecycleStore {
    persistence: Arc<dyn LifecyclePersistence>,
    routing: Arc<dyn routing::RoutingHints>,
}

impl Default for LifecycleStore {
    fn default() -> Self {
        Self::new(Arc::new(MemoryLifecyclePersistence::default()))
    }
}

impl LifecycleStore {
    pub(crate) fn new(persistence: Arc<dyn LifecyclePersistence>) -> Self {
        Self {
            persistence,
            routing: Arc::new(routing::MemoryRoutingHints::new(Duration::from_secs(
                86_460,
            ))),
        }
    }

    pub(crate) fn with_routing(mut self, routing: Arc<dyn routing::RoutingHints>) -> Self {
        self.routing = routing;
        self
    }

    pub(crate) async fn record(&self, event: LifecycleEvent) -> Result<()> {
        ensure!(event.report.sequence > 0, "invalid lifecycle sequence");
        ensure!(event.report.owner_epoch > 0, "invalid ownership epoch");
        ensure!(
            event.report.occurred_at_ms >= event.report.started_at_ms,
            "invalid lifecycle time range"
        );
        let (persisted, ()) =
            tokio::join!(self.persistence.record(&event), self.record_routing(&event));
        persisted
    }

    pub(crate) async fn routing(
        &self,
        actor: &crate::actor_state::ActorStorageKey,
        host: &str,
        session: &str,
        epoch: u64,
    ) -> Option<LifecycleKind> {
        match self
            .routing
            .get(&routing::session_key(actor, host, session, epoch))
            .await
        {
            Ok(hint) => hint,
            Err(error) => {
                tracing::warn!(%error, "shutdown hint unavailable; routing with ownership lease");
                None
            }
        }
    }

    async fn record_routing(&self, event: &LifecycleEvent) {
        if !matches!(
            event.report.kind,
            LifecycleKind::Draining | LifecycleKind::Stopped
        ) {
            return;
        }
        let key = routing::session_key(
            &event.actor.storage_key(),
            &event.host_id,
            &event.session_id,
            event.report.owner_epoch,
        );
        if let Err(error) = self.routing.record(&key, event.report.kind).await {
            tracing::warn!(%error, "host shutdown hint could not be recorded");
        }
    }
}

#[async_trait]
impl ActorInventoryReader for LifecycleStore {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let now = SystemClock.now_ms()?;
        let mut actors = BTreeMap::<String, ActorInventory>::new();
        for event in self.persistence.latest(project).await? {
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
