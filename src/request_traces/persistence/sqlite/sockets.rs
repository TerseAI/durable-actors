use std::collections::HashMap;

use anyhow::Result;
use rusqlite::{Connection, params};

use crate::platform_events::PlatformEvent;
use crate::{
    request_traces::sockets::SocketRecord,
    sockets::events::{ALIVE, CLOSED, OPENED, SocketClose, SocketEvent},
};

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

/// Connection events follow the trace retention; only each gateway's latest heartbeat per project
/// is kept.
pub(super) fn append(
    connection: &mut Connection,
    gateway_id: &str,
    events: &[SocketEvent],
    retention: usize,
) -> Result<()> {
    let transaction = connection.transaction()?;
    for event in events {
        let (actor_name, actor_id, connection_id, detail) = match event {
            SocketEvent::Opened {
                actor,
                connection_id,
                metadata,
                ..
            } => (
                actor.actor_name.as_str(),
                actor.actor_id.as_str(),
                connection_id.as_str(),
                metadata.to_string(),
            ),
            SocketEvent::Closed {
                actor,
                connection_id,
                close,
                ..
            } => (
                actor.actor_name.as_str(),
                actor.actor_id.as_str(),
                connection_id.as_str(),
                serde_json::to_string(close)?,
            ),
            SocketEvent::Alive { project_id, .. } => {
                transaction.execute(
                    "DELETE FROM socket_events WHERE event = ?1 AND gateway_id = ?2 AND project_id = ?3",
                    params![event.name(), gateway_id, project_id],
                )?;
                ("", "", "", "{}".to_owned())
            }
        };
        transaction.execute(
            "INSERT INTO socket_events
                (event, gateway_id, project_id, actor_name, actor_id, connection_id, at_ms, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.name(),
                gateway_id,
                event.project_id(),
                actor_name,
                actor_id,
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

/// Local volumes are small, so connections are assembled from their events here.
pub(super) fn records(
    connection: &mut Connection,
    project_id: &str,
    to_ms: u64,
    open_only: bool,
    limit: usize,
) -> Result<Vec<SocketRecord>> {
    let mut statement = connection.prepare(
        "SELECT connection_id, actor_name, actor_id, gateway_id, event, at_ms, detail
         FROM socket_events WHERE project_id = ?1 AND event IN (?2, ?3) ORDER BY position",
    )?;
    let mut rows = statement.query(params![project_id, OPENED, CLOSED])?;
    let mut opened = HashMap::<String, SocketRecord>::new();
    let mut closed = HashMap::<String, (u64, SocketClose)>::new();
    while let Some(row) = rows.next()? {
        let connection_id: String = row.get(0)?;
        let at_ms = u64::try_from(row.get::<_, i64>(5)?)?;
        let detail: String = row.get(6)?;
        if row.get::<_, String>(4)? == OPENED {
            opened.insert(
                connection_id.clone(),
                SocketRecord {
                    connection_id,
                    actor_name: row.get(1)?,
                    actor_id: row.get(2)?,
                    gateway_id: row.get(3)?,
                    opened_at_ms: at_ms,
                    metadata: serde_json::from_str(&detail).ok(),
                    closed: None,
                },
            );
        } else {
            closed.insert(connection_id, (at_ms, serde_json::from_str(&detail)?));
        }
    }
    let mut records: Vec<_> = opened
        .into_values()
        .map(|mut record| {
            record.closed = closed.remove(&record.connection_id);
            record
        })
        .filter(|record| record.opened_at_ms <= to_ms && !(open_only && record.closed.is_some()))
        .collect();
    records.sort_by(|a, b| {
        (b.opened_at_ms, &a.connection_id).cmp(&(a.opened_at_ms, &b.connection_id))
    });
    records.truncate(limit);
    Ok(records)
}

pub(super) fn heartbeats(
    connection: &mut Connection,
    project_id: &str,
) -> Result<HashMap<String, u64>> {
    let mut statement = connection.prepare(
        "SELECT gateway_id, MAX(at_ms) FROM socket_events
         WHERE event = ?1 AND project_id = ?2 GROUP BY gateway_id",
    )?;
    statement
        .query_map(params![ALIVE, project_id], |row| {
            Ok((row.get(0)?, row.get::<_, i64>(1)?))
        })?
        .map(|row| {
            let (gateway, at_ms) = row?;
            Ok((gateway, u64::try_from(at_ms)?))
        })
        .collect()
}
