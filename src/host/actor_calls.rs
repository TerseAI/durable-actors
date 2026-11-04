use super::actor_database::ActorStorage;
use crate::{
    actor::{ActorCall, ActorCaller},
    control_plane::{CONTROL_PLANE_REQUEST_TIMEOUT, ControlPlaneClient},
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

// Leaves the control plane time to answer before the host's request times out.
const ACTOR_CALL_TIMEOUT: Duration =
    CONTROL_PLANE_REQUEST_TIMEOUT.saturating_sub(Duration::from_secs(5));

pub(super) struct HostActorCalls {
    storage: Arc<dyn ActorStorage>,
    control_plane: Arc<ControlPlaneClient>,
}

impl HostActorCalls {
    pub(super) fn new(
        storage: Arc<dyn ActorStorage>,
        control_plane: Arc<ControlPlaneClient>,
    ) -> Self {
        Self {
            storage,
            control_plane,
        }
    }
}

#[async_trait]
impl ActorCaller for HostActorCalls {
    async fn call(&self, call: ActorCall) -> Value {
        if let Err(error) = self.storage.ensure_authority() {
            return failed("unavailable", format!("{error:#}"));
        }
        match self
            .control_plane
            .invoke_actor(call, ACTOR_CALL_TIMEOUT)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => failed("outcome_unknown", format!("{error:#}")),
        }
    }
}

fn failed(code: &str, message: String) -> Value {
    json!({"type": "failed", "code": code, "message": message})
}
