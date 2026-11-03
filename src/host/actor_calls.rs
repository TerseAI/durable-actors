use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::actor_database::ActorStorage;
use crate::{
    actor::{ActorInvocation, ActorKey},
    control_plane::{CONTROL_PLANE_REQUEST_TIMEOUT, ControlPlaneClient},
};

// Leaves the control plane time to answer before the gRPC request itself times out.
const ACTOR_CALL_TIMEOUT: Duration =
    CONTROL_PLANE_REQUEST_TIMEOUT.saturating_sub(Duration::from_secs(5));

/// Loopback stand-in for the control plane's invoke route. The actor's own SDK client calls it,
/// and the host forwards the call with its credentials; the control plane enforces the project.
pub(super) struct ActorCallProxy {
    url: String,
    project_id: String,
}

impl ActorCallProxy {
    pub(super) async fn start(
        caller: &ActorKey,
        storage: Arc<dyn ActorStorage>,
        control_plane: Arc<ControlPlaneClient>,
        stop: CancellationToken,
    ) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let url = format!("http://{}", listener.local_addr()?);
        let routes = Router::new()
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke",
                post(invoke),
            )
            .with_state(Arc::new(Forwarder {
                storage,
                control_plane,
            }));
        tokio::spawn(async move {
            let serving =
                axum::serve(listener, routes).with_graceful_shutdown(stop.cancelled_owned());
            if let Err(error) = serving.await {
                tracing::warn!(error = %error, "actor call proxy stopped");
            }
        });
        Ok(Self {
            url,
            project_id: caller.project_id.clone(),
        })
    }

    /// Points the actor's SDK client at this proxy.
    pub(super) fn environment(&self) -> [(&'static str, String); 2] {
        [
            ("DURABLE_ACTORS_CONTROL_PLANE_URL", self.url.clone()),
            ("DURABLE_ACTORS_PROJECT_ID", self.project_id.clone()),
        ]
    }
}

struct Forwarder {
    storage: Arc<dyn ActorStorage>,
    control_plane: Arc<ControlPlaneClient>,
}

async fn invoke(
    State(forwarder): State<Arc<Forwarder>>,
    Path(actor): Path<ActorKey>,
    Json(request): Json<InvokeRequest>,
) -> Response {
    if let Err(error) = forwarder.storage.ensure_authority() {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "unavailable", error);
    }
    let invocation = ActorInvocation {
        request_id: request.request_id,
        actor,
        method: request.method,
        args: request.args,
    };
    match forwarder
        .control_plane
        .invoke_actor(invocation, ACTOR_CALL_TIMEOUT)
        .await
    {
        Ok(outcome) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(json!({"outcome": outcome})),
        )
            .into_response(),
        Err(error) => failure(StatusCode::BAD_GATEWAY, "outcome_unknown", error),
    }
}

fn failure(status: StatusCode, code: &str, error: anyhow::Error) -> Response {
    let message = format!("{error:#}");
    (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InvokeRequest {
    request_id: String,
    method: String,
    args: Vec<Value>,
}
