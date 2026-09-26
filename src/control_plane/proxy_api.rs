use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde_json::{Value, json};

use super::{ActorJwtIssuer, admin::AdminService};
use crate::{
    actor::ActorKey,
    regional::{
        Region,
        proxy::{ProxyConfig, ProxyRequest, ProxyTicket, ProxyTransport, pool::ProxyPool},
    },
};

#[derive(Clone)]
struct ProxyApi {
    pool: Arc<ProxyPool>,
    issuer: ActorJwtIssuer,
    admin: AdminService,
    region: Region,
    jwt_issuer: String,
}

pub(super) fn router(
    pool: Arc<ProxyPool>,
    issuer: ActorJwtIssuer,
    admin: AdminService,
    region: Region,
    jwt_issuer: String,
) -> Router {
    Router::new()
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-proxy",
            post(prepare),
        )
        .layer(DefaultBodyLimit::max(
            super::MAX_CONTROL_PLANE_MESSAGE_BYTES,
        ))
        .with_state(ProxyApi {
            pool,
            issuer,
            admin,
            region,
            jwt_issuer,
        })
}

async fn prepare(
    State(state): State<ProxyApi>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    Json(request): Json<ProxyRequest>,
) -> Result<Json<Value>, StatusCode> {
    state
        .admin
        .authenticate(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default(),
        )
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    actor.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    let object_id = uuid::Uuid::parse_str(&request.object_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .to_string();
    let destination = request.destination;
    let template = ProxyConfig {
        actor,
        session: uuid::Uuid::new_v4().to_string(),
        region: state.region,
        keys: state.issuer.verifier_keys_json().map_err(unavailable)?,
        issuer: state.jwt_issuer.clone(),
    };
    ProxyTicket::new(&template, destination.clone()).map_err(|_| StatusCode::BAD_REQUEST)?;
    let ready = state
        .pool
        .ensure(&object_id, &template)
        .await
        .map_err(unavailable)?;
    let ticket =
        ProxyTicket::new(&ready.config, destination.clone()).map_err(|_| StatusCode::CONFLICT)?;
    let token = state.issuer.issue_proxy(&ticket).map_err(unavailable)?;
    let mut response = json!({
        "objectId": object_id, "homeRegion": request.home_region,
        "ingressRegion": state.region, "route": ready.handle.route, "token": token,
        "ownerEpoch": destination.owner_epoch, "expiresAtMs": ticket.expires_at_ms(),
    });
    if destination.kind == ProxyTransport::Socket {
        let credentials = state
            .pool
            .socket_credentials(&ready)
            .await
            .map_err(unavailable)?;
        let mut url = reqwest::Url::parse(&credentials.url).map_err(|_| StatusCode::BAD_GATEWAY)?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        url.set_path("/v1/socket");
        if !credentials.token.is_empty() {
            url.query_pairs_mut()
                .append_pair("_modal_connect_token", &credentials.token);
        }
        url.query_pairs_mut().append_pair("key", &token);
        response["websocketUrl"] = url.as_str().into();
        response["connectByMs"] = ticket.expires_at_ms().into();
    }
    Ok(Json(response))
}

fn unavailable(error: anyhow::Error) -> StatusCode {
    tracing::warn!(error = %error, "regional proxy preparation failed");
    StatusCode::SERVICE_UNAVAILABLE
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/proxy_api.rs"]
mod tests;
