use super::*;
use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    request_traces::{
        TraceStore, live::InProcessFeed, persistence::sqlite::SqliteTracePersistence,
    },
    sockets::events::{SocketClose, SocketEvent},
};

struct EmptyInventory;

#[async_trait]
impl ActorInventoryReader for EmptyInventory {
    async fn actor_inventory(&self, _: &str) -> Result<ActorInventorySnapshot> {
        Ok(ActorInventorySnapshot {
            actors: vec![],
            connections_complete: true,
            gateways: Default::default(),
        })
    }

    async fn residency_history(
        &self,
        _: &str,
        _: &str,
        _: u64,
        _: u64,
    ) -> Result<crate::placement::ResidencyHistory> {
        unreachable!("not read here")
    }
}

#[tokio::test]
async fn instances_list_the_connections_gateways_report_open() -> Result<()> {
    let store = Arc::new(SqliteTracePersistence::in_memory());
    let actor = ActorKey {
        project_id: "alpha".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    let now = SystemClock.now_ms()?;
    let opened = |connection_id: &str| SocketEvent::Opened {
        actor: actor.clone(),
        connection_id: connection_id.into(),
        metadata: serde_json::json!({"user": connection_id}),
        at_ms: now - 2_000,
    };
    store
        .append_socket_events(
            "gateway".into(),
            vec![
                opened("open"),
                opened("closed"),
                SocketEvent::Closed {
                    actor: actor.clone(),
                    connection_id: "closed".into(),
                    close: SocketClose {
                        code: 1000,
                        reason: String::new(),
                        clean: true,
                        messages_in: 0,
                        messages_out: 0,
                        bytes_in: 0,
                        bytes_out: 0,
                        duration_ms: 1_000,
                    },
                    at_ms: now - 1_000,
                },
                SocketEvent::Alive {
                    project_id: "alpha".into(),
                    at_ms: now,
                },
            ],
        )
        .await?;
    let reader = ConnectionInventoryReader::new(
        Arc::new(EmptyInventory),
        TraceStore::new(store, Arc::new(InProcessFeed::default())),
    );
    let inventory = reader.actor_inventory("alpha").await?;
    assert_eq!(inventory.actors.len(), 1);
    let instance = &inventory.actors[0].instances[0];
    assert_eq!(instance.actor_id, "one");
    let connections: Vec<_> = instance
        .connections
        .iter()
        .map(|connection| (connection.id.as_str(), connection.metadata.clone()))
        .collect();
    assert_eq!(connections, [("open", serde_json::json!({"user": "open"}))]);
    assert!(reader.actor_inventory("beta").await?.actors.is_empty());
    Ok(())
}
