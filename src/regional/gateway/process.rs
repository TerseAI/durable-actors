use std::future::Future;

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Request as HttpRequest, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::Response as HttpResponse,
    routing::{get, post},
};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::*;
use crate::grpc::{
    forward,
    proto::{
        ActorCall, ActorReply, Empty, SocketEffects, SocketFrame,
        actor_service_server::{ActorService, ActorServiceServer},
    },
};

pub struct GatewayConfig {
    pub bind: std::net::SocketAddr,
    pub ingress: Region,
    pub control_plane_urls: HashMap<String, String>,
    pub secret: String,
    pub control_plane_secret: String,
    pub cloud_run_auth: bool,
}

impl GatewayConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            bind: std::env::var("DURABLE_ACTORS_GATEWAY_BIND")
                .unwrap_or_else(|_| "0.0.0.0:8080".into())
                .parse()?,
            ingress: std::env::var("DURABLE_ACTORS_REGION")?.parse()?,
            control_plane_urls: serde_json::from_str(&std::env::var(
                "DURABLE_ACTORS_CONTROL_PLANE_URLS",
            )?)?,
            control_plane_secret: std::env::var("DURABLE_ACTORS_CONTROL_PLANE_SECRET")
                .context("internal control-plane authentication is required")?,
            cloud_run_auth: std::env::var("DURABLE_ACTORS_CLOUD_RUN_AUTH")
                .ok()
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(false),
            secret: std::env::var("DURABLE_ACTORS_SECRET")
                .context("gateway authentication is required")?,
        })
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.control_plane_urls
                .keys()
                .any(|region| region.parse::<Region>().ok() == Some(self.ingress)),
            "the gateway's own region requires a control-plane endpoint"
        );
        self.validate_credentials()
    }

    fn validate_credentials(&self) -> Result<()> {
        for secret in [&self.secret, &self.control_plane_secret] {
            ensure!(
                !secret.is_empty() && secret.trim() == secret,
                "invalid gateway credential"
            );
        }
        ensure!(
            self.secret != self.control_plane_secret,
            "gateway client and internal control-plane credentials must differ"
        );
        Ok(())
    }
}

pub async fn serve_gateway(
    config: GatewayConfig,
    store: Arc<dyn super::super::DirectoryStore>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    config.validate()?;
    let endpoints = Arc::new(
        HttpRegionalEndpoints::new(config.control_plane_urls, config.control_plane_secret)?
            .with_cloud_run_auth(config.cloud_run_auth)?,
    );
    let gateway = Arc::new(Gateway::new(
        Arc::new(ActorDirectory::new(store)),
        config.ingress,
        endpoints.clone(),
    ));
    let state = GatewayApi {
        gateway,
        endpoints,
        secret: config.secret,
    };
    let grpc = ActorServiceServer::new(state.clone())
        .max_decoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
        .max_encoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES);
    let routes = tonic::service::Routes::from(router(state))
        .add_service(grpc)
        .into_axum_router();
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    axum::serve(listener, routes)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

#[derive(Clone)]
struct GatewayApi {
    gateway: Arc<Gateway>,
    endpoints: Arc<HttpRegionalEndpoints>,
    secret: String,
}

fn router(state: GatewayApi) -> Router {
    Router::new()
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/resolve",
            post(resolve_identity),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-actor",
            post(find_actor),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke",
            post(invoke_actor),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-websocket",
            post(find_socket),
        )
        .route(
            "/v1/projects/{project}/objects/{object_id}",
            get(find_identity),
        )
        .route(
            "/v1/projects/{project}/objects/{object_id}/find-actor",
            post(find_by_id),
        )
        .merge(customer_admin_routes())
        .layer(DefaultBodyLimit::max(
            crate::control_plane::MAX_CONTROL_PLANE_MESSAGE_BYTES,
        ))
        .layer(middleware::from_fn_with_state(
            state.secret.clone(),
            authenticate,
        ))
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/openapi.yaml",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/yaml")],
                    include_str!("../../../docs/reference/openapi.yaml"),
                )
            }),
        )
        .with_state(state)
}

fn customer_admin_routes() -> Router<GatewayApi> {
    let mut routes = Router::new()
        .route(
            "/v1/projects/{project}/deployment",
            get(admin_request).put(admin_request).delete(admin_request),
        )
        .route(
            "/v1/projects/{project}/deployment/contract",
            get(admin_request),
        );
    for path in [
        "/v1/projects/{project}/observe/actors",
        "/v1/projects/{project}/observe/events",
        "/v1/projects/{project}/observe/requests",
        "/v1/projects/{project}/observe/metrics",
        "/v1/projects/{project}/observe/queue-waits",
        "/v1/projects/{project}/observe/websockets",
        "/v1/projects/{project}/observe/requests/events",
    ] {
        routes = routes.route(path, get(admin_request));
    }
    routes
}

async fn authenticate(
    State(secret): State<String>,
    request: HttpRequest,
    next: Next,
) -> HttpResponse {
    let authorized = check_authorization(
        &secret,
        request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default(),
    );
    let response = match authorized {
        Ok(()) => next.run(request).await,
        Err(status) => axum::response::IntoResponse::into_response(status),
    };
    let structured = response.extensions().get::<StructuredError>().is_some();
    if !structured && (response.status().is_client_error() || response.status().is_server_error()) {
        let status = response.status();
        let message = status
            .canonical_reason()
            .unwrap_or("regional request failed");
        return axum::response::IntoResponse::into_response((
            status,
            Json(json!({
                "error": {"code": status.as_str(), "message": message}
            })),
        ));
    }
    response
}

fn check_authorization(secret: &str, supplied: &str) -> Result<(), StatusCode> {
    let expected = format!("Bearer {secret}");
    if !bool::from(expected.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FindActor {
    #[serde(default, rename = "homeRegion")]
    _home_region: Option<String>,
}

async fn find_actor(
    State(state): State<GatewayApi>,
    Path(actor): Path<ActorKey>,
    Json(_request): Json<FindActor>,
) -> Result<Json<Value>, StatusCode> {
    actor.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(
        state
            .gateway
            .resolve(&actor, None)
            .await
            .map_err(unavailable)?,
    ))
}

#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayInvocation {
    request_id: String,
    method: String,
    args: Vec<Value>,
}

async fn invoke_actor(
    State(state): State<GatewayApi>,
    Path(actor): Path<ActorKey>,
    Json(call): Json<GatewayInvocation>,
) -> HttpResponse {
    if actor.validate().is_err() || call.request_id.is_empty() || call.method.is_empty() {
        return json_status(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "invalid actor invocation",
        );
    }
    let call = serde_json::to_value(&call).expect("serializable invocation");
    match state.gateway.invoke(&actor, &call).await {
        super::InvocationOutcome::Reply(reply) => {
            axum::response::IntoResponse::into_response(Json(reply))
        }
        super::InvocationOutcome::Unavailable => json_status(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "actor host could not be reached before execution",
        ),
        super::InvocationOutcome::OutcomeUnknown => json_status(
            StatusCode::BAD_GATEWAY,
            "outcome_unknown",
            "actor host request failed after dispatch; the outcome is unknown",
        ),
    }
}

#[derive(Clone)]
struct StructuredError;

fn json_status(status: StatusCode, code: &str, message: &str) -> HttpResponse {
    let mut response = axum::response::IntoResponse::into_response((
        status,
        Json(serde_json::json!({"error": {"code": code, "message": message}})),
    ));
    response.extensions_mut().insert(StructuredError);
    response
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FindSocket {
    #[serde(default, rename = "homeRegion")]
    _home_region: Option<String>,
    metadata: Value,
    #[serde(default = "socket_lifetime")]
    authorization_lifetime_ms: i64,
}

fn socket_lifetime() -> i64 {
    900_000
}

async fn find_socket(
    State(state): State<GatewayApi>,
    Path(actor): Path<ActorKey>,
    Json(request): Json<FindSocket>,
) -> Result<Json<Value>, StatusCode> {
    actor.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    crate::actor::validate_socket_metadata(&request.metadata)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if !(1000..=86_400_000).contains(&request.authorization_lifetime_ms) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let body = json!({"metadata": request.metadata, "authorizationLifetimeMs": request.authorization_lifetime_ms});
    Ok(Json(
        state
            .gateway
            .resolve(&actor, Some(body))
            .await
            .map_err(unavailable)?,
    ))
}

async fn resolve_identity(
    State(state): State<GatewayApi>,
    Path(actor): Path<ActorKey>,
) -> Result<Json<ObjectAssignment>, StatusCode> {
    actor.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(
        state
            .gateway
            .directory
            .get_or_create(&actor, state.gateway.ingress)
            .await
            .map_err(unavailable)?,
    ))
}

#[derive(Deserialize)]
struct ObjectPath {
    project: String,
    object_id: String,
}

async fn find_identity(
    State(state): State<GatewayApi>,
    Path(path): Path<ObjectPath>,
) -> Result<Json<ObjectAssignment>, StatusCode> {
    Ok(Json(lookup(&state, &path).await?))
}

async fn find_by_id(
    State(state): State<GatewayApi>,
    Path(path): Path<ObjectPath>,
) -> Result<Json<Value>, StatusCode> {
    let assignment = lookup(&state, &path).await?;
    Ok(Json(
        state
            .gateway
            .resolve_assignment(&assignment, None)
            .await
            .map_err(unavailable)?,
    ))
}

async fn lookup(state: &GatewayApi, path: &ObjectPath) -> Result<ObjectAssignment, StatusCode> {
    uuid::Uuid::parse_str(&path.object_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let assignment = state
        .gateway
        .directory
        .lookup_by_id(&path.object_id)
        .await
        .map_err(unavailable)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if assignment.actor.project_id != path.project {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(assignment)
}

async fn admin_request(
    State(state): State<GatewayApi>,
    request: HttpRequest,
) -> Result<HttpResponse, StatusCode> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    let body = axum::body::to_bytes(body, crate::control_plane::MAX_CONTROL_PLANE_MESSAGE_BYTES)
        .await
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    let response = state
        .endpoints
        .request(
            state.gateway.ingress,
            parts.method,
            parts
                .uri
                .path_and_query()
                .map(|uri| uri.as_str())
                .unwrap_or(path),
            body,
        )
        .await
        .map_err(unavailable)?;
    let mut result = HttpResponse::builder().status(response.status());
    for name in [header::CONTENT_TYPE, header::CACHE_CONTROL] {
        if let Some(value) = response.headers().get(&name) {
            result = result.header(name, value);
        }
    }
    result
        .body(Body::from(
            response
                .bytes()
                .await
                .map_err(|_| StatusCode::BAD_GATEWAY)?,
        ))
        .map_err(|_| StatusCode::BAD_GATEWAY)
}

#[tonic::async_trait]
impl ActorService for GatewayApi {
    async fn invoke(
        &self,
        mut request: Request<ActorCall>,
    ) -> Result<Response<ActorReply>, Status> {
        self.authenticate_grpc(&request)?;
        let deadline = forward::deadline(&request)?;
        let invocation = crate::grpc::actor::decode_call(request.get_ref())?;
        let work = async {
            let target = self
                .gateway
                .resolve(&invocation.actor, None)
                .await
                .map_err(grpc_unavailable)?;
            request.get_mut().owner_epoch = target["ownerEpoch"]
                .as_u64()
                .ok_or_else(|| Status::internal("missing actor epoch"))?;
            forward::authorize(
                &mut request,
                target["token"]
                    .as_str()
                    .ok_or_else(|| Status::internal("missing actor capability"))?,
                deadline,
            )?;
            forward::client(
                target["route"]
                    .as_str()
                    .ok_or_else(|| Status::internal("missing actor route"))?,
            )
            .map_err(grpc_unavailable)?
            .invoke(request)
            .await
        };
        tokio::time::timeout_at(deadline.into(), work)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("deadline elapsed; invocation outcome may be unknown")
            })?
    }

    async fn publish(
        &self,
        mut request: Request<SocketEffects>,
    ) -> Result<Response<Empty>, Status> {
        self.authenticate_grpc(&request)?;
        let deadline = forward::deadline(&request)?;
        let call = request
            .get_ref()
            .target
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing actor target"))?;
        let actor = ActorKey {
            project_id: call.project_id.clone(),
            actor_name: call.actor_name.clone(),
            actor_id: call.actor_id.clone(),
        };
        actor
            .validate()
            .map_err(|_| Status::invalid_argument("invalid actor identity"))?;
        let work = async {
            let target = self
                .gateway
                .resolve(&actor, None)
                .await
                .map_err(grpc_unavailable)?;
            request.get_mut().target.as_mut().unwrap().owner_epoch = target["ownerEpoch"]
                .as_u64()
                .ok_or_else(|| Status::internal("missing actor epoch"))?;
            forward::authorize(
                &mut request,
                target["token"]
                    .as_str()
                    .ok_or_else(|| Status::internal("missing actor capability"))?,
                deadline,
            )?;
            forward::client(
                target["route"]
                    .as_str()
                    .ok_or_else(|| Status::internal("missing actor route"))?,
            )
            .map_err(grpc_unavailable)?
            .publish(request)
            .await
        };
        tokio::time::timeout_at(deadline.into(), work)
            .await
            .map_err(|_| Status::deadline_exceeded("socket publication deadline elapsed"))?
    }

    type SocketSessionStream = ReceiverStream<Result<SocketFrame, Status>>;

    async fn socket_session(
        &self,
        _: Request<Streaming<SocketFrame>>,
    ) -> Result<Response<Self::SocketSessionStream>, Status> {
        Err(Status::failed_precondition(
            "resolve a regional socket endpoint before connecting",
        ))
    }
}

impl GatewayApi {
    fn authenticate_grpc<T>(&self, request: &Request<T>) -> Result<(), Status> {
        check_authorization(
            &self.secret,
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default(),
        )
        .map_err(|_| Status::unauthenticated("gateway authentication required"))
    }
}

fn unavailable(error: anyhow::Error) -> StatusCode {
    tracing::warn!(error = %error, "regional routing failed");
    StatusCode::SERVICE_UNAVAILABLE
}

fn grpc_unavailable(error: impl std::fmt::Display) -> Status {
    tracing::warn!(error = %error, "regional routing failed");
    Status::unavailable("regional routing failed")
}

#[cfg(test)]
#[path = "../../../tests/unit/regional/gateway_api.rs"]
mod tests;
