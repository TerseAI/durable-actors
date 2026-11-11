use anyhow::Result;
use rusqlite::{Connection, params};

use crate::sockets::events::SocketEvent;

pub(super) fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS socket_events (
            position INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT NOT NULL,
            gateway_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            actor_name TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            connection_id TEXT NOT NULL,
            at_ms INTEGER NOT NULL,
            detail TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS socket_events_connection ON socket_events(project_id, connection_id);
        CREATE INDEX IF NOT EXISTS socket_events_time ON socket_events(project_id, at_ms);",
    )?;
    Ok(())
}

/// Connection events follow the trace retention; only each gateway's latest heartbeat is kept.
pub(super) fn append(
    connection: &mut Connection,
    gateway_id: &str,
    events: &[SocketEvent],
    retention: usize,
) -> Result<()> {
    let transaction = connection.transaction()?;
    for event in events {
        let (actor, connection_id, detail) = match event {
            SocketEvent::Opened {
                actor,
                connection_id,
                metadata,
                ..
            } => (Some(actor), connection_id.as_str(), metadata.to_string()),
            SocketEvent::Closed {
                actor,
                connection_id,
                close,
                ..
            } => (
                Some(actor),
                connection_id.as_str(),
                serde_json::to_string(close)?,
            ),
            SocketEvent::Alive { .. } => {
                transaction.execute(
                    "DELETE FROM socket_events WHERE event = ?1 AND gateway_id = ?2",
                    params![event.name(), gateway_id],
                )?;
                (None, "", "{}".to_owned())
            }
        };
        transaction.execute(
            "INSERT INTO socket_events
                (event, gateway_id, project_id, actor_name, actor_id, connection_id, at_ms, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.name(),
                gateway_id,
                actor.map_or("", |actor| actor.project_id.as_str()),
                actor.map_or("", |actor| actor.actor_name.as_str()),
                actor.map_or("", |actor| actor.actor_id.as_str()),
                connection_id,
                i64::try_from(event.at_ms())?,
                detail,
            ],
        )?;
    }
    transaction.execute(
        "DELETE FROM socket_events WHERE position <= COALESCE((SELECT position FROM socket_events ORDER BY position DESC LIMIT 1 OFFSET ?1), 0)",
        [retention as i64],
    )?;
    transaction.commit()?;
    Ok(())
}
