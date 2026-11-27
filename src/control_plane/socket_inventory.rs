use crate::{
    actor::ActorKey,
    actor_lifecycle::LifecycleStore,
    placement::{
        ActorConnectionInventory, ActorInstanceOverview, ActorInventory, ActorInventoryReader,
        ActorInventorySnapshot, ActorResidency, ActorTypesPage, ActorTypesQuery, InstancePage,
        InstancePageQuery,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// The actor inventory with each instance's open WebSocket connections, as the gateways reported
/// them through socket events.
pub(super) struct ConnectionInventoryReader {
    actors: Arc<LifecycleStore>,
    sockets: crate::request_traces::TraceStore,
}

impl ConnectionInventoryReader {
    pub(super) fn new(
        actors: Arc<LifecycleStore>,
        sockets: crate::request_traces::TraceStore,
    ) -> Self {
        Self { actors, sockets }
    }
}

#[async_trait]
impl ActorInventoryReader for ConnectionInventoryReader {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let (actors, open) = tokio::try_join!(
            self.actors.actor_inventory(project),
            self.sockets.open_connections(project, None)
        )?;
        Ok(ActorInventorySnapshot {
            actors: merge_rooms(actors.actors, open.rooms),
            connections_complete: actors.connections_complete,
            gateways: open.gateways,
        })
    }

    async fn instance_page(
        &self,
        project: &str,
        query: &InstancePageQuery,
    ) -> Result<InstancePage> {
        let open = self
            .sockets
            .open_connections(
                project,
                Some((&query.actor_name, query.actor_id.as_deref())),
            )
            .await?;
        let sockets = open
            .rooms
            .into_iter()
            .map(|(key, connections)| (key.actor_id, connections.len() as u64))
            .collect();
        self.actors
            .read_instance_page(project, query, &sockets)
            .await
    }

    async fn actor_types_page(
        &self,
        project: &str,
        query: &ActorTypesQuery,
        deployed: &[String],
    ) -> Result<ActorTypesPage> {
        let open = self.sockets.open_connections(project, None).await?;
        let sockets = open
            .rooms
            .into_iter()
            .map(|(key, connections)| ((key.actor_name, key.actor_id), connections.len() as u64))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut page = self
            .actors
            .read_actor_types_page(project, query, &sockets, deployed)
            .await?;
        page.open_sockets = sockets.values().sum();
        page.gateways = open.gateways;
        Ok(page)
    }

    async fn residency_history(
        &self,
        project: &str,
        query: &crate::placement::ResidencyQuery,
    ) -> Result<crate::placement::ResidencyHistory> {
        self.actors.residency_history(project, query).await
    }
}

fn merge_rooms(
    actors: Vec<ActorInventory>,
    rooms: Vec<(ActorKey, Vec<ActorConnectionInventory>)>,
) -> Vec<ActorInventory> {
    let mut actors: std::collections::BTreeMap<_, _> = actors
        .into_iter()
        .map(|actor| (actor.actor_name.clone(), actor))
        .collect();
    let mut instances: std::collections::HashMap<_, _> = actors
        .values()
        .flat_map(|actor| {
            actor.instances.iter().enumerate().map(|(index, instance)| {
                ((actor.actor_name.clone(), instance.actor_id.clone()), index)
            })
        })
        .collect();
    for (room, connections) in rooms {
        let key = (room.actor_name.clone(), room.actor_id.clone());
        let actor = actors
            .entry(room.actor_name.clone())
            .or_insert_with(|| ActorInventory {
                actor_name: room.actor_name,
                ..Default::default()
            });
        let index = *instances.entry(key).or_insert_with(|| {
            actor.dormant += 1;
            actor.instances.push(ActorInstanceOverview {
                actor_id: room.actor_id,
                status: ActorResidency::Dormant,
                last_active_at_ms: None,
                reported_at_ms: None,
                stale_at_ms: None,
                connections: vec![],
                waiting: None,
                database_bytes: None,
            });
            actor.instances.len() - 1
        });
        actor.instances[index].connections = connections;
    }
    actors.into_values().collect()
}
