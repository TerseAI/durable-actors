//! WebSocket sessions as gateways reported them. Each connection has a recorded open and maybe a
//! close; an unclosed connection is open only while its gateway keeps reporting that it is alive.
//! The open itself counts as a report, since the gateway was alive to record it.

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::{
    actor::ActorKey,
    placement::ActorConnectionInventory,
    sockets::events::{ALIVE_INTERVAL, SocketClose},
};

/// Sessions per response, newest first.
pub(crate) const SESSION_LIMIT: usize = 500;
/// Connections a project can hold open at once, for the inventory.
pub(crate) const OPEN_LIMIT: usize = 10_000;
/// A gateway this long without a heartbeat has stopped; its unclosed connections are lost.
pub(crate) const GATEWAY_STALE_AFTER_MS: u64 = 3 * ALIVE_INTERVAL.as_millis() as u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SocketStatus {
    Open,
    Closed,
    /// The gateway stopped reporting before the connection closed.
    Lost,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SocketSession {
    pub connection_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub gateway_id: String,
    pub status: SocketStatus,
    pub opened_at_ms: u64,
    /// When it closed, or for a lost session when its gateway last reported.
    pub closed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    /// Text messages each way, known once the connection closes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_in: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_out: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// One connection's recorded open and close, before its status is known.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SocketRecord {
    pub connection_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub gateway_id: String,
    pub opened_at_ms: u64,
    pub metadata: Option<serde_json::Value>,
    pub closed: Option<(u64, SocketClose)>,
}

/// Applies the status rules: closed with a recorded close, open while the gateway reports, and
/// otherwise lost when the gateway last reported.
pub(crate) fn sessions(
    records: Vec<SocketRecord>,
    heartbeats: &HashMap<String, u64>,
    now_ms: u64,
) -> Vec<SocketSession> {
    records
        .into_iter()
        .map(|record| {
            let alive = alive_at(heartbeats, &record);
            let (status, closed_at_ms, close) = match record.closed {
                Some((at_ms, close)) => (SocketStatus::Closed, Some(at_ms), Some(close)),
                None if alive + GATEWAY_STALE_AFTER_MS >= now_ms => {
                    (SocketStatus::Open, None, None)
                }
                None => (SocketStatus::Lost, Some(alive), None),
            };
            SocketSession {
                connection_id: record.connection_id,
                actor_name: record.actor_name,
                actor_id: record.actor_id,
                gateway_id: record.gateway_id,
                status,
                opened_at_ms: record.opened_at_ms,
                closed_at_ms,
                close_code: close.as_ref().map(|close| close.code),
                messages_in: close.as_ref().map(|close| close.messages_in),
                messages_out: close.as_ref().map(|close| close.messages_out),
                close_reason: close.map(|close| close.reason),
                metadata: record.metadata,
            }
        })
        .collect()
}

fn alive_at(heartbeats: &HashMap<String, u64>, record: &SocketRecord) -> u64 {
    heartbeats
        .get(&record.gateway_id)
        .copied()
        .unwrap_or_default()
        .max(record.opened_at_ms)
}

pub(crate) fn overlaps(session: &SocketSession, from_ms: u64, to_ms: u64) -> bool {
    session.opened_at_ms <= to_ms && session.closed_at_ms.is_none_or(|closed| closed >= from_ms)
}

/// The project's open connections by actor, and when each of their gateways last showed it was
/// alive, so observers can tell when a silent gateway's connections are lost.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct OpenConnections {
    pub rooms: Vec<(ActorKey, Vec<ActorConnectionInventory>)>,
    pub gateways: BTreeMap<String, u64>,
}

pub(crate) fn open_connections(
    project_id: &str,
    sessions: Vec<SocketSession>,
    heartbeats: &HashMap<String, u64>,
) -> OpenConnections {
    let mut open = OpenConnections::default();
    let mut rooms = BTreeMap::<(String, String), Vec<ActorConnectionInventory>>::new();
    for session in sessions
        .into_iter()
        .filter(|session| session.status == SocketStatus::Open)
    {
        let seen = heartbeats
            .get(&session.gateway_id)
            .copied()
            .unwrap_or_default()
            .max(session.opened_at_ms);
        let gateway = open.gateways.entry(session.gateway_id.clone()).or_default();
        *gateway = (*gateway).max(seen);
        rooms
            .entry((session.actor_name, session.actor_id))
            .or_default()
            .push(ActorConnectionInventory {
                id: session.connection_id,
                gateway_id: session.gateway_id,
                metadata: session.metadata.unwrap_or_default(),
            });
    }
    open.rooms = rooms
        .into_iter()
        .map(|((actor_name, actor_id), mut connections)| {
            connections.sort_by(|a, b| a.id.cmp(&b.id));
            let actor = ActorKey {
                project_id: project_id.to_owned(),
                actor_name,
                actor_id,
            };
            (actor, connections)
        })
        .collect();
    open
}

#[cfg(test)]
#[path = "../../tests/unit/request_traces/sockets.rs"]
mod tests;
