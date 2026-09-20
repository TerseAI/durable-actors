use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::actor::ActorKey;

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminService, HostLaunchSpec},
    contracts::{ContractRevisionConflict, PublicActorContract},
    service::{ControlPlaneService, TargetResolutionTimings},
};

#[derive(Clone)]
struct PublicApiState {
    invocations: ControlPlaneService,
    admin: AdminService,
}

pub(super) fn router(invocations: ControlPlaneService, admin: AdminService) -> Router {
    let contracts = super::contract_api::router(admin.clone());
    Router::new()
        .route("/.well-known/jwks.json", get(jwks))
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/v1/deployment",
            put(register_deployment)
                .get(get_deployment)
                .delete(delete_deployment),
        )
        .route(
            "/v1/actors/{actor_type}/{actor_id}/connect",
            post(connect_actor),
        )
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_MESSAGE_BYTES))
        .with_state(PublicApiState { invocations, admin })
        .merge(contracts)
}

async fn connect_actor(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    request: Result<Json<ConnectRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(request) = request.map_err(ApiError::json)?;
    match request {
        ConnectRequest::Grpc { home_region } => {
            resolve_actor_target(
                State(state),
                Path(path),
                headers,
                TargetRequest { home_region },
            )
            .await
        }
        ConnectRequest::Websocket {
            home_region,
            metadata,
            authorization_lifetime_ms,
            backend,
        } => {
            issue_socket_ticket(
                State(state),
                Path(path),
                headers,
                Json(IssueSocketTicketRequest {
                    home_region,
                    metadata,
                    authorization_lifetime_ms,
                    backend,
                }),
            )
            .await
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase", deny_unknown_fields)]
enum ConnectRequest {
    Grpc {
        #[serde(default, rename = "homeRegion")]
        home_region: Option<String>,
    },
    Websocket {
        #[serde(default, rename = "homeRegion")]
        home_region: Option<String>,
        metadata: Value,
        #[serde(
            default = "socket_authorization_lifetime",
            rename = "authorizationLifetimeMs"
        )]
        authorization_lifetime_ms: i64,
        #[serde(default)]
        backend: bool,
    },
}

async fn issue_socket_ticket(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    Json(request): Json<IssueSocketTicketRequest>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    state
        .invocations
        .validate_home_region(request.home_region.as_deref())
        .map_err(ApiError::assignment)?;
    let mut grant = super::socket_ticket::SocketGrant {
        actor: path.into_actor(),
        region: request
            .home_region
            .clone()
            .unwrap_or_else(|| state.invocations.default_region().into()),
        target: None,
        backend: request.backend,
        metadata: request.metadata,
        authorization_lifetime_ms: request.authorization_lifetime_ms,
    };
    grant.validate().map_err(ApiError::bad_request)?;
    let (region, target, credentials) = state
        .invocations
        .socket_destination(&grant.actor, &grant.region, request.home_region.as_deref())
        .await
        .map_err(ApiError::routing)?;
    grant.region = region;
    grant.target = Some(target);
    let issued = state
        .admin
        .issue_direct_socket(grant, credentials)
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(issued)).into_response())
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IssueSocketTicketRequest {
    #[serde(default)]
    backend: bool,
    #[serde(default)]
    home_region: Option<String>,
    metadata: Value,
    #[serde(default = "socket_authorization_lifetime")]
    authorization_lifetime_ms: i64,
}

fn socket_authorization_lifetime() -> i64 {
    900_000
}

async fn get_deployment(
    State(state): State<PublicApiState>,
    headers: HeaderMap,
) -> Result<Json<RegisterDeploymentRequest>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let spec = state
        .admin
        .current_deployment()
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "deployment not found"))?;
    let source = spec
        .source
        .unwrap_or_else(|| super::admin::DeploymentSource {
            image_ref: spec.image_ref,
            working_directory: spec.working_directory,
            actor_entrypoint: spec.actor_entrypoint,
        });
    Ok(Json(RegisterDeploymentRequest {
        code_revision: spec.code_revision,
        contract: None,
        image_ref: source.image_ref,
        working_directory: source.working_directory,
        actor_entrypoint: source.actor_entrypoint,
        secret_refs: spec.secret_refs,
    }))
}

async fn delete_deployment(
    State(state): State<PublicApiState>,
    headers: HeaderMap,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let changed = state
        .invocations
        .delete_deployment(&state.admin)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(DeploymentReply { changed }))
}

async fn register_deployment(
    State(state): State<PublicApiState>,
    headers: HeaderMap,
    request: Result<Json<RegisterDeploymentRequest>, JsonRejection>,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(request) = request.map_err(ApiError::json)?;
    let contract = request
        .contract
        .map(PublicActorContract::new)
        .transpose()
        .map_err(ApiError::bad_request)?;
    let spec = HostLaunchSpec {
        source: None,
        code_snapshot: None,
        code_revision: request.code_revision,
        image_ref: request.image_ref,
        working_directory: request.working_directory,
        actor_entrypoint: request.actor_entrypoint,
        secret_refs: request.secret_refs,
    };
    let changed = state
        .invocations
        .deploy_source(&state.admin, &spec, contract.as_ref())
        .await
        .map_err(|error| {
            if error.is::<ContractRevisionConflict>() {
                ApiError::conflict(error.to_string())
            } else {
                ApiError::bad_request(error)
            }
        })?;
    Ok(Json(DeploymentReply { changed }))
}

async fn jwks(State(state): State<PublicApiState>) -> Result<Json<Value>, ApiError> {
    let document = serde_json::from_slice(&state.admin.jwks_json().map_err(ApiError::internal)?)
        .map_err(ApiError::internal)?;
    Ok(Json(document))
}

async fn resolve_actor_target(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    request: TargetRequest,
) -> Result<Response, ApiError> {
    let mut timings = TargetResolutionTimings::new();
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    authorized_admin(&state.admin, &headers)?;
    let actor = path.into_actor();
    actor.validate().map_err(ApiError::bad_request)?;
    state
        .invocations
        .validate_home_region(request.home_region.as_deref())
        .map_err(ApiError::assignment)?;
    let result: Result<Json<ActorTargetReply>, ApiError> = async {
        actor.validate().map_err(ApiError::bad_request)?;
        timings.request_validated_at_ms = Some(timings.elapsed_ms());
        timings.client_authenticated_at_ms = Some(timings.elapsed_ms());
        let target = state
            .invocations
            .resolve_actor_target_timed(&actor, request.home_region.as_deref(), &mut timings)
            .await
            .map_err(ApiError::routing)?;
        Ok(Json(ActorTargetReply {
            transport: "grpc",
            home_region: target.home_region,
            route: target.route,
            token: target.token,
            owner_epoch: target.owner_epoch,

            expires_at_ms: target.expires_at_ms,
        }))
    }
    .await;
    let completed_at_ms = timings.elapsed_ms();
    match &result {
        Ok(_) => info!(
            event = "actor_target_resolution",
            request_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            client_authenticated_at_ms = timings.client_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,

            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "resolved",
            "actor target resolution completed"
        ),
        Err(error) => warn!(
            event = "actor_target_resolution",
            request_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            client_authenticated_at_ms = timings.client_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,

            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "failed",
            error_code = %error.code,
            error = %error.message,
            "actor target resolution failed"
        ),
    }
    result.map(IntoResponse::into_response)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TargetRequest {
    home_region: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ActorPath {
    actor_type: String,
    actor_id: String,
}

impl ActorPath {
    pub(super) fn into_actor(self) -> ActorKey {
        ActorKey {
            actor_type: self.actor_type,
            actor_id: self.actor_id,
        }
    }
}

pub(super) fn authorized_admin(admin: &AdminService, headers: &HeaderMap) -> Result<(), ApiError> {
    let authorization = authorization(headers)?;
    admin
        .authenticate(authorization)
        .map_err(|_| ApiError::unauthorized("admin credential was rejected"))?;
    Ok(())
}

fn authorization(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthorized("bearer credential is required"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("bearer credential is invalid"))
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegisterDeploymentRequest {
    code_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contract: Option<Value>,
    image_ref: String,
    working_directory: String,
    #[serde(default)]
    actor_entrypoint: Option<String>,
    #[serde(default)]
    secret_refs: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentReply {
    changed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorTargetReply {
    transport: &'static str,
    home_region: String,
    route: String,
    token: String,
    owner_epoch: u64,

    expires_at_ms: i64,
}

pub(super) struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
}

impl ApiError {
    fn json(error: JsonRejection) -> Self {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                error.body_text(),
            )
        } else {
            Self::bad_request(error.body_text())
        }
    }

    pub(super) fn assignment(error: anyhow::Error) -> Self {
        if error.is::<super::service::RegionConflict>() {
            Self::conflict(error.to_string())
        } else {
            Self::bad_request(error)
        }
    }

    pub(super) fn routing(error: anyhow::Error) -> Self {
        if error.is::<super::service::RegionConflict>() {
            Self::conflict(error.to_string())
        } else {
            Self::unavailable(format!("actor host is unavailable: {error:#}"))
        }
    }

    pub(super) fn bad_request(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        )
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub(super) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    pub(super) fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            error.to_string(),
        )
    }

    pub(super) fn new(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorDocument {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorDocument {
    error: ErrorBody,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: String,
    message: String,
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/public_api.rs"]
mod tests;
