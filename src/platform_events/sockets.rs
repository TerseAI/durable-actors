//! What each gateway reports about its WebSocket connections for observability: every open and
//! close, plus a heartbeat for each project it holds connections for, so readers can tell that a
//! stopped gateway's connections have ended.

use std::time::Duration;

use opentelemetry::logs::AnyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::ActorKey,
    platform_events::{PlatformEvent, PlatformRecord, count},
    request_traces::otlp,
};

/// How often a gateway reports that it is alive.
pub(crate) const ALIVE_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) const OPENED: &str = "durable_actors.socket.opened";
pub(crate) const CLOSED: &str = "durable_actors.socket.closed";
pub(crate) const ALIVE: &str = "durable_actors.gateway.alive";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SocketClose {
    pub code: u16,
    pub reason: String,
    pub clean: bool,
    pub messages_in: u64,
    pub messages_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SocketEvent {
    Opened {
        actor: ActorKey,
        connection_id: String,
        metadata: Value,
        at_ms: u64,
    },
    Closed {
        actor: ActorKey,
        connection_id: String,
        close: SocketClose,
        at_ms: u64,
    },
    Alive {
        project_id: String,
        at_ms: u64,
    },
}

impl SocketEvent {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Opened { .. } => OPENED,
            Self::Closed { .. } => CLOSED,
            Self::Alive { .. } => ALIVE,
        }
    }

    pub(crate) fn at_ms(&self) -> u64 {
        match self {
            Self::Opened { at_ms, .. } | Self::Closed { at_ms, .. } | Self::Alive { at_ms, .. } => {
                *at_ms
            }
        }
    }

    pub(crate) fn actor(&self) -> Option<&ActorKey> {
        match self {
            Self::Opened { actor, .. } | Self::Closed { actor, .. } => Some(actor),
            Self::Alive { .. } => None,
        }
    }

    /// The record's attributes. The project travels on each record because one gateway serves
    /// many projects.
    pub(crate) fn attributes(&self) -> Vec<(&'static str, AnyValue)> {
        let mut attributes = vec![(
            otlp::PROJECT_ID,
            AnyValue::from(self.project_id().to_owned()),
        )];
        if let (Some(actor), Some(connection_id)) = (self.actor(), self.connection_id()) {
            attributes.extend([
                (otlp::ACTOR_NAME, AnyValue::from(actor.actor_name.clone())),
                (otlp::ACTOR_ID, AnyValue::from(actor.actor_id.clone())),
                (
                    otlp::CONNECTION_ID,
                    AnyValue::from(connection_id.to_owned()),
                ),
            ]);
        }
        match self {
            Self::Opened { metadata, .. } => {
                attributes.push((otlp::METADATA, AnyValue::from(metadata.to_string())));
            }
            Self::Closed { close, .. } => attributes.extend([
                (otlp::CLOSE_CODE, AnyValue::from(i64::from(close.code))),
                (otlp::CLOSE_REASON, AnyValue::from(close.reason.clone())),
                (otlp::CLOSE_CLEAN, AnyValue::from(close.clean)),
                (otlp::MESSAGES_IN, count(close.messages_in)),
                (otlp::MESSAGES_OUT, count(close.messages_out)),
                (otlp::BYTES_IN, count(close.bytes_in)),
                (otlp::BYTES_OUT, count(close.bytes_out)),
                (otlp::DURATION_MS, count(close.duration_ms)),
            ]),
            Self::Alive { .. } => {}
        }
        attributes
    }

    fn connection_id(&self) -> Option<&str> {
        match self {
            Self::Opened { connection_id, .. } | Self::Closed { connection_id, .. } => {
                Some(connection_id)
            }
            Self::Alive { .. } => None,
        }
    }
}

impl PlatformEvent for SocketEvent {
    fn project_id(&self) -> &str {
        match self {
            Self::Opened { actor, .. } | Self::Closed { actor, .. } => &actor.project_id,
            Self::Alive { project_id, .. } => project_id,
        }
    }

    fn to_record(&self) -> PlatformRecord {
        PlatformRecord {
            name: self.name(),
            at_ms: self.at_ms(),
            attributes: self.attributes(),
            body: None,
        }
    }
}
