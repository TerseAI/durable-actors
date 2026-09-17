use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::{actor::ActorKey, host::ActorProcessRole};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminService, HostLaunchSpec},
    service::{ControlPlaneService, TargetResolutionTimings},
};

#[derive(Clone)]
struct PublicApiState {
    invocations: ControlPlaneService,
    admin: AdminService,
}

pub(super) fn router(invocations: ControlPlaneService, admin: AdminService) -> Router {
    let sockets = super::websocket::router(
        invocations.clone(),
        invocations.sockets.clone(),
        admin.clone(),
    );
    Router::new()
        .route("/.well-known/jwks.json", get(jwks))
        .route(
            "/v1/deployment",
            put(register_deployment)
                .get(get_deployment)
                .delete(delete_deployment),
        )
        .route("/v1/session-scoped-token", post(issue_workflow_token))
        .route(
            "/v1/actors/{actor_type}/{actor_id}/socket-ticket",
            post(issue_socket_ticket),
        )
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/socket-ticket",
            post(issue_socket_ticket),
        )
        .route(
            "/v1/actors/{actor_type}/{actor_id}/target",
            post(resolve_actor_target),
        )
        .route(
            "/v1/namespaces/{namespace_id}/deployment",
            put(register_deployment)
                .get(get_deployment)
                .delete(delete_deployment),
        )
        .route(
            "/v1/namespaces/{namespace_id}/session-scoped-token",
            post(issue_workflow_token),
        )
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/target",
            post(resolve_actor_target),
        )
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_MESSAGE_BYTES))
        .with_state(PublicApiState { invocations, admin })
        .merge(sockets)
}

async fn issue_socket_ticket(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    Json(request): Json<IssueSocketTicketRequest>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let principal = state
        .invocations
        .authenticate_application(
            &state.admin,
            headers[header::AUTHORIZATION]
                .to_str()
                .map_err(ApiError::bad_request)?,
            path.namespace_id.as_deref(),
        )
        .map_err(ApiError::bad_request)?;
    let grant = super::socket_ticket::SocketGrant {
        actor: path.into_actor(&principal.scope.namespace_id),
        region: principal.region,
        metadata: request.metadata,
        authorization_lifetime_ms: request.authorization_lifetime_ms,
        connection_id: request.connection_id,
    };
    grant.validate().map_err(ApiError::bad_request)?;
    let spec = state
        .invocations
        .runtime_deployment(&grant.actor.namespace_id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::conflict("actor deployment is not registered"))?;
    let issued = state
        .admin
        .issue_socket(grant, spec.socket_gateway_url.as_deref())
        .map_err(ApiError::bad_request)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(issued)).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IssueSocketTicketRequest {
    metadata: Value,
    #[serde(default = "socket_authorization_lifetime")]
    authorization_lifetime_ms: i64,
    #[serde(default)]
    connection_id: Option<String>,
}

fn socket_authorization_lifetime() -> i64 {
    900_000
}

async fn get_deployment(
    State(state): State<PublicApiState>,
    Path(path): Path<NamespacePath>,
    headers: HeaderMap,
) -> Result<Json<Option<HostLaunchSpec>>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    Ok(Json(
        state
            .admin
            .current_deployment(
                path.namespace_id
                    .as_deref()
                    .unwrap_or(&state.admin.default_namespace),
            )
            .await
            .map_err(ApiError::internal)?,
    ))
}

async fn delete_deployment(
    State(state): State<PublicApiState>,
    Path(path): Path<NamespacePath>,
    headers: HeaderMap,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let changed = state
        .invocations
        .delete_deployment(
            &state.admin,
            path.namespace_id
                .as_deref()
                .unwrap_or(&state.admin.default_namespace),
        )
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(DeploymentReply { changed }))
}

async fn register_deployment(
    State(state): State<PublicApiState>,
    Path(path): Path<NamespacePath>,
    headers: HeaderMap,
    Json(request): Json<RegisterDeploymentRequest>,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let spec = HostLaunchSpec {
        namespace_id: path
            .namespace_id
            .unwrap_or_else(|| state.admin.default_namespace.clone()),
        code_revision: request.code_revision,
        image_ref: request.image_ref,
        working_directory: request.working_directory,
        actor_entrypoint: request.actor_entrypoint,
        secret_refs: request.secret_refs,
        socket_gateway_url: request.socket_gateway_url,
    };
    let changed = state
        .invocations
        .register_deployment(&state.admin, &spec)
        .await
        .map_err(ApiError::bad_request)?;
    if let Some(region) = request.warm_region {
        state.invocations.warm_deployment_image(spec, region);
    }
    Ok(Json(DeploymentReply { changed }))
}

async fn issue_workflow_token(
    State(state): State<PublicApiState>,
    Path(path): Path<NamespacePath>,
    headers: HeaderMap,
    Json(request): Json<IssueWorkflowTokenRequest>,
) -> Result<Json<IssueWorkflowTokenReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let namespace_id = path
        .namespace_id
        .as_deref()
        .unwrap_or(&state.admin.default_namespace);
    if request.execution_id.is_empty() || request.execution_id.len() > 255 {
        return Err(ApiError::bad_request("workflow execution ID is invalid"));
    }
    if request.deadline_unix_ms <= 0 {
        return Err(ApiError::bad_request("workflow deadline is required"));
    }
    if state
        .invocations
        .runtime_deployment(namespace_id)
        .await
        .map_err(ApiError::internal)?
        .is_none()
    {
        return Err(ApiError::conflict("project has no registered actor code"));
    }
    let issued = state
        .admin
        .issue_workflow_token(
            namespace_id,
            &request.execution_id,
            &request.storage_region,
            request.deadline_unix_ms,
        )
        .map_err(ApiError::bad_request)?;
    Ok(Json(IssueWorkflowTokenReply {
        token: issued.token,
        expires_at_ms: issued.expires_at_ms,
    }))
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
) -> Result<Json<ActorTargetReply>, ApiError> {
    let mut timings = TargetResolutionTimings::new();
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let principal = state
        .invocations
        .authenticate_application(
            &state.admin,
            authorization(&headers)?,
            path.namespace_id.as_deref(),
        )
        .map_err(|_| ApiError::unauthorized("application credential was rejected"))?;
    let actor = path.into_actor(&principal.scope.namespace_id);
    let result: Result<Json<ActorTargetReply>, ApiError> = async {
        actor.validate().map_err(ApiError::bad_request)?;
        timings.request_validated_at_ms = Some(timings.elapsed_ms());
        authorize_actor(&principal, &actor)?;
        timings.workflow_authenticated_at_ms = Some(timings.elapsed_ms());
        let target = state
            .invocations
            .resolve_workflow_target_timed(&principal, &actor, &mut timings)
            .await
            .map_err(|error| {
                ApiError::unavailable(format!("actor host is unavailable: {error:#}"))
            })?;
        Ok(Json(ActorTargetReply {
            namespace_id: actor.namespace_id.clone(),
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
            namespace_id = %actor.namespace_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            workflow_authenticated_at_ms = timings.workflow_authenticated_at_ms,
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
            namespace_id = %actor.namespace_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            workflow_authenticated_at_ms = timings.workflow_authenticated_at_ms,
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
    result
}

fn authorize_actor(
    principal: &super::auth::ActorPrincipal,
    actor: &ActorKey,
) -> Result<(), ApiError> {
    if principal.process_role != ActorProcessRole::Workflow {
        return Err(ApiError::forbidden(
            "credential cannot call application actors",
        ));
    }
    if !principal.scope.contains(actor) {
        return Err(ApiError::forbidden(
            "workflow token cannot cross namespace scope",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
struct NamespacePath {
    namespace_id: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ActorPath {
    pub namespace_id: Option<String>,
    actor_type: String,
    actor_id: String,
}

impl ActorPath {
    pub(super) fn into_actor(self, namespace_id: &str) -> ActorKey {
        ActorKey {
            namespace_id: self.namespace_id.unwrap_or_else(|| namespace_id.to_owned()),
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterDeploymentRequest {
    code_revision: String,
    image_ref: String,
    working_directory: String,
    #[serde(default)]
    actor_entrypoint: Option<String>,
    #[serde(default)]
    secret_refs: Vec<String>,
    #[serde(default)]
    socket_gateway_url: Option<String>,
    #[serde(default)]
    warm_region: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueWorkflowTokenRequest {
    execution_id: String,
    deadline_unix_ms: i64,
    storage_region: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentReply {
    changed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IssueWorkflowTokenReply {
    token: String,
    expires_at_ms: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorTargetReply {
    namespace_id: String,
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

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
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
mod tests {
    use super::*;

    #[test]
    fn workflow_token_request_accepts_a_storage_region() {
        let request: IssueWorkflowTokenRequest = serde_json::from_value(serde_json::json!({
            "executionId": "execution-1",
            "deadlineUnixMs": 1_800_000_000_000_i64,
            "storageRegion": "north-america-west"
        }))
        .unwrap();

        assert_eq!(request.storage_region, "north-america-west");
    }

    #[test]
    fn deployment_registration_accepts_a_background_warm_region() {
        let request: RegisterDeploymentRequest = serde_json::from_value(serde_json::json!({
            "codeRevision": "revision-1",
            "imageRef": "im-actor",
            "workingDirectory": "/workspace",
            "warmRegion": "north-america-west"
        }))
        .unwrap();

        assert_eq!(request.warm_region.as_deref(), Some("north-america-west"));
    }
}
