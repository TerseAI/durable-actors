//! What each gateway reports about its WebSocket connections for observability: every open and
//! close, plus a heartbeat so readers can tell that a stopped gateway's connections have ended.

use std::time::Duration;

use opentelemetry::logs::AnyValue;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue as ProtoValue, KeyValue, any_value::Value as ProtoKind},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{actor::ActorKey, request_traces::otlp};

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
            Self::Opened { at_ms, .. } | Self::Closed { at_ms, .. } | Self::Alive { at_ms } => {
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
        let mut attributes = Vec::new();
        if let (Some(actor), Some(connection_id)) = (self.actor(), self.connection_id()) {
            attributes.extend([
                (otlp::PROJECT_ID, AnyValue::from(actor.project_id.clone())),
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

impl SocketEvent {
    /// The event as the collector's Redis exporter publishes it, for gateways without a collector.
    pub(crate) fn export_request(&self, gateway_id: &str) -> ExportLogsServiceRequest {
        let attribute = |key: &str, value: AnyValue| KeyValue {
            key: key.to_owned(),
            value: Some(ProtoValue {
                value: match value {
                    AnyValue::Int(value) => Some(ProtoKind::IntValue(value)),
                    AnyValue::Boolean(value) => Some(ProtoKind::BoolValue(value)),
                    AnyValue::String(value) => Some(ProtoKind::StringValue(value.to_string())),
                    _ => None,
                },
            }),
            ..Default::default()
        };
        let record = LogRecord {
            time_unix_nano: self.at_ms() * 1_000_000,
            event_name: self.name().to_owned(),
            attributes: self
                .attributes()
                .into_iter()
                .map(|(key, value)| attribute(key, value))
                .collect(),
            ..Default::default()
        };
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![attribute(
                        otlp::GATEWAY_ID,
                        AnyValue::from(gateway_id.to_owned()),
                    )],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![record],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }
}

fn count(value: u64) -> AnyValue {
    AnyValue::from(i64::try_from(value).unwrap_or(i64::MAX))
}

/// Records connection events without blocking the connection; delivery is best effort.
pub(crate) trait SocketEvents: Send + Sync {
    fn record(&self, event: SocketEvent);
}

/// For gateways without socket telemetry configured.
pub(crate) struct NoSocketEvents;

impl SocketEvents for NoSocketEvents {
    fn record(&self, _event: SocketEvent) {}
}

#[cfg(test)]
#[path = "../../tests/unit/sockets/events.rs"]
mod tests;
