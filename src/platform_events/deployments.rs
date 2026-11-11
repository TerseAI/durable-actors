//! Each deployment the control plane commits, so observability reads the deployed actor classes
//! from the analytics store. PostgreSQL stays the source of truth for running them.

use opentelemetry::logs::AnyValue;
use serde_json::Value;

use super::{PlatformEvent, PlatformRecord};
use crate::request_traces::otlp;

pub(crate) const PUBLISHED: &str = "durable_actors.deployment.published";
pub(crate) const REMOVED: &str = "durable_actors.deployment.removed";

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DeploymentEvent {
    /// A deployment went live, with its public actor contract when it has one.
    Published {
        project_id: String,
        contract: Option<Value>,
        at_ms: u64,
    },
    Removed {
        project_id: String,
        at_ms: u64,
    },
}

impl DeploymentEvent {
    pub(crate) fn at_ms(&self) -> u64 {
        match self {
            Self::Published { at_ms, .. } | Self::Removed { at_ms, .. } => *at_ms,
        }
    }

    /// The contract the event leaves deployed.
    pub(crate) fn contract(&self) -> Option<&Value> {
        match self {
            Self::Published { contract, .. } => contract.as_ref(),
            Self::Removed { .. } => None,
        }
    }
}

impl PlatformEvent for DeploymentEvent {
    fn project_id(&self) -> &str {
        match self {
            Self::Published { project_id, .. } | Self::Removed { project_id, .. } => project_id,
        }
    }

    fn to_record(&self) -> PlatformRecord {
        PlatformRecord {
            name: match self {
                Self::Published { .. } => PUBLISHED,
                Self::Removed { .. } => REMOVED,
            },
            at_ms: self.at_ms(),
            attributes: vec![(
                otlp::PROJECT_ID,
                AnyValue::from(self.project_id().to_owned()),
            )],
            body: self.contract().map(Value::to_string),
        }
    }
}
