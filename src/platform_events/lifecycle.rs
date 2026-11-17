//! Each lifecycle report the control plane accepts, so observers read actor residency from the
//! analytics store and live feed like every other platform event.

use opentelemetry::logs::AnyValue;

use super::{PlatformEvent, PlatformRecord};
use crate::{actor_lifecycle::LifecycleEvent, request_traces::otlp};

pub(crate) const REPORTED: &str = "durable_actors.actor.reported";

impl PlatformEvent for LifecycleEvent {
    fn project_id(&self) -> &str {
        &self.actor.project_id
    }

    fn to_record(&self) -> PlatformRecord {
        PlatformRecord {
            name: REPORTED,
            at_ms: self.received_at_ms,
            attributes: vec![
                (
                    otlp::PROJECT_ID,
                    AnyValue::from(self.actor.project_id.clone()),
                ),
                (
                    otlp::ACTOR_NAME,
                    AnyValue::from(self.actor.actor_name.clone()),
                ),
                (otlp::ACTOR_ID, AnyValue::from(self.actor.actor_id.clone())),
            ],
            body: serde_json::to_string(self).ok(),
        }
    }
}
