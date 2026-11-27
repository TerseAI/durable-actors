use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{actor_state::ActorStorageKey, host::HostId};

mod instances;
pub use instances::{
    ActorTypesPage, ActorTypesQuery, InstancePage, InstancePageQuery, InstanceRow, InstanceSort,
    ObservedActorType,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub lease: crate::host_leases::HostLease,
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub owner_epoch: u64,
    pub home_region: String,
    pub state_version: u64,
    pub state_object: Option<String>,
    pub last_request_id: Option<String>,
}

#[async_trait]
pub trait ObjectPlacementStore: Send + Sync {
    async fn get_owner(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self.get_owner_with_hint(object).await?.0)
    }

    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(
        Option<ObjectPlacement>,
        Option<crate::ownership::OwnershipHint>,
    )>;

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>>;
}

pub fn validate_region(region: &str) -> Result<()> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            }),
        "sandbox region is invalid"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorResidency {
    Live,
    Dormant,
    Unknown,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInstanceOverview {
    pub actor_id: String,
    pub status: ActorResidency,
    pub last_active_at_ms: Option<u64>,
    /// When the control plane received the instance's latest report.
    pub reported_at_ms: Option<u64>,
    /// When a live or dormant status turns unknown unless another report arrives; stopped
    /// instances stay dormant.
    pub stale_at_ms: Option<u64>,
    pub connections: Vec<ActorConnectionInventory>,
    pub waiting: Option<Vec<crate::host_leases::WaitingOperation>>,
    /// The SQLite database size in the latest report; null when that report did not measure it.
    pub database_bytes: Option<u64>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorConnectionInventory {
    pub id: String,
    pub gateway_id: String,
    pub metadata: serde_json::Value,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInventory {
    pub actor_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    pub live: u64,
    pub dormant: u64,
    pub unknown: u64,
    pub instances: Vec<ActorInstanceOverview>,
}

#[async_trait]
pub trait ActorInventoryReader: Send + Sync {
    async fn actor_inventory(&self, project_id: &str) -> Result<ActorInventorySnapshot>;

    async fn instance_page(
        &self,
        project: &str,
        query: &InstancePageQuery,
    ) -> Result<InstancePage> {
        let Some(actor) = self
            .actor_inventory(project)
            .await?
            .actors
            .into_iter()
            .find(|actor| actor.actor_name == query.actor_name)
        else {
            return Ok(InstancePage::default());
        };
        let rows = actor
            .instances
            .into_iter()
            .map(|instance| {
                let count = instance.connections.len() as u64;
                InstanceRow::from_overview(instance, count)
            })
            .collect();
        Ok(InstancePage::from_rows(rows, query))
    }

    async fn actor_types_page(
        &self,
        project: &str,
        query: &ActorTypesQuery,
        deployed: &[String],
    ) -> Result<ActorTypesPage> {
        let snapshot = self.actor_inventory(project).await?;
        let open_sockets = snapshot
            .actors
            .iter()
            .flat_map(|actor| &actor.instances)
            .map(|instance| instance.connections.len() as u64)
            .sum();
        let mut by_name: std::collections::BTreeMap<_, _> = snapshot
            .actors
            .into_iter()
            .map(|actor| {
                let ActorInventory {
                    actor_name,
                    live,
                    dormant,
                    unknown,
                    ..
                } = actor;
                (
                    actor_name.clone(),
                    ObservedActorType {
                        actor_name,
                        live,
                        dormant,
                        unknown,
                    },
                )
            })
            .collect();
        for name in deployed {
            by_name
                .entry(name.clone())
                .or_insert_with(|| ObservedActorType {
                    actor_name: name.clone(),
                    ..Default::default()
                });
        }
        let mut page = ActorTypesPage::from_types(by_name.into_values().collect(), query);
        page.open_sockets = open_sockets;
        page.gateways = snapshot.gateways;
        Ok(page)
    }

    async fn residency_history(
        &self,
        _project_id: &str,
        _query: &ResidencyQuery,
    ) -> Result<ResidencyHistory> {
        Ok(ResidencyHistory::default())
    }
}

/// One actor class's instances, or one instance when `actor_id` is set, over `[from_ms, to_ms]`.
#[derive(Clone, Debug)]
pub struct ResidencyQuery {
    pub actor_name: String,
    pub actor_id: Option<String>,
    pub from_ms: u64,
    pub to_ms: u64,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyHistory {
    pub periods: Vec<ResidencyPeriod>,
    pub truncated: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResidencyPeriod {
    pub actor_id: String,
    pub status: ActorResidency,
    pub from_ms: u64,
    pub to_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInventorySnapshot {
    pub actors: Vec<ActorInventory>,
    pub connections_complete: bool,
    /// When each gateway holding an open connection last showed it was alive.
    pub gateways: std::collections::BTreeMap<String, u64>,
}
