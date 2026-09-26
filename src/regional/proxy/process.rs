use std::{future::Future, sync::Arc, time::Duration};

use super::activity::Activity;
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::Response as HttpResponse,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::*;
use crate::grpc::{
    actor::{decode_call, decode_frame, encode_frame},
    forward,
    proto::{
        ActorCall, ActorReply, Empty, SocketEffects, SocketFrame,
        actor_service_server::{ActorService, ActorServiceServer},
    },
};

pub async fn serve_proxy(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let config: ProxyConfig = serde_json::from_str(&std::env::var("DURABLE_ACTORS_PROXY_CONFIG")?)?;
    let activity = Arc::new(Activity::default());
    let routes = router_with_activity(config.clone(), activity.clone())?;
    let bind = std::env::var("DURABLE_ACTORS_HOST_BIND").unwrap_or_else(|_| "0.0.0.0:7101".into());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    write_readiness(&config).await?;
    axum::serve(listener, routes)
        .with_graceful_shutdown(async move {
            tokio::select! { _ = shutdown => {}, _ = activity.wait_until_idle(Duration::from_secs(300)) => {} }
        })
        .await?;
    Ok(())
}

#[cfg(test)]
fn router(config: ProxyConfig) -> Result<Router> {
    router_with_activity(config, Arc::new(Activity::default()))
}

fn router_with_activity(config: ProxyConfig, activity: Arc<Activity>) -> Result<Router> {
    let service = ProxyService {
        verifier: ProxyVerifier::new(config)?,
        activity: activity.clone(),
    };
    let http = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke",
            post(invoke),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/socket-effects",
            post(publish),
        )
        .route("/v1/socket", get(socket))
        .layer(DefaultBodyLimit::max(
            crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
        ))
        .with_state(Arc::new(service.clone()));
    let grpc = ActorServiceServer::new(service)
        .max_decoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
        .max_encoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES);
    Ok(tonic::service::Routes::from(http)
        .add_service(grpc)
        .into_axum_router()
        .layer(axum::middleware::from_fn_with_state(
            activity,
            track_activity,
        )))
}

async fn track_activity(
    State(activity): State<Arc<Activity>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<HttpResponse, StatusCode> {
    if request.uri().path() == "/healthz" {
        return Ok(next.run(request).await);
    }
    let guard = activity.enter().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let response = next.run(request).await;
    Ok(response.map(|body| {
        axum::body::Body::new(http_body_util::StreamBody::new(
            futures_util::stream::unfold(
                (http_body_util::BodyStream::new(body), guard),
                |(mut stream, guard)| async move {
                    stream.next().await.map(|chunk| (chunk, (stream, guard)))
                },
            ),
        ))
    }))
}

#[derive(Clone)]
struct ProxyService {
    verifier: ProxyVerifier,
    activity: Arc<Activity>,
}

#[tonic::async_trait]
impl ActorService for ProxyService {
    async fn invoke(
        &self,
        mut request: Request<ActorCall>,
    ) -> Result<Response<ActorReply>, Status> {
        let deadline = forward::deadline(&request)?;
        let ticket = self.ticket(&request, ProxyTransport::Invocation)?;
        let invocation = decode_call(request.get_ref())?;
        if invocation.actor != self.verifier.config.actor
            || request.get_ref().owner_epoch != ticket.destination.owner_epoch
        {
            return Err(Status::permission_denied(
                "actor or epoch does not match proxy grant",
            ));
        }
        forward::authorize(&mut request, &ticket.destination.token, deadline)?;
        let mut client = forward::client(&ticket.destination.route).map_err(unavailable)?;
        // No replay: a lost response may follow a committed actor invocation.
        tokio::time::timeout_at(deadline.into(), client.invoke(request))
            .await
            .map_err(|_| {
                Status::deadline_exceeded("deadline elapsed; invocation outcome may be unknown")
            })?
    }

    async fn publish(
        &self,
        mut request: Request<SocketEffects>,
    ) -> Result<Response<Empty>, Status> {
        let deadline = forward::deadline(&request)?;
        let ticket = self.ticket(&request, ProxyTransport::Invocation)?;
        let target = request
            .get_ref()
            .target
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing actor target"))?;
        let actor = &self.verifier.config.actor;
        if target.project_id != actor.project_id
            || target.actor_name != actor.actor_name
            || target.actor_id != actor.actor_id
            || target.owner_epoch != ticket.destination.owner_epoch
        {
            return Err(Status::permission_denied(
                "socket effects do not match proxy grant",
            ));
        }
        forward::authorize(&mut request, &ticket.destination.token, deadline)?;
        let mut client = forward::client(&ticket.destination.route).map_err(unavailable)?;
        tokio::time::timeout_at(deadline.into(), client.publish(request))
            .await
            .map_err(|_| Status::deadline_exceeded("socket publication deadline elapsed"))?
    }

    type SocketSessionStream = Streaming<SocketFrame>;

    async fn socket_session(
        &self,
        mut request: Request<Streaming<SocketFrame>>,
    ) -> Result<Response<Self::SocketSessionStream>, Status> {
        let ticket = self.ticket(&request, ProxyTransport::Socket)?;
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", ticket.destination.token)
                .parse()
                .map_err(|_| Status::unauthenticated("invalid upstream credential"))?,
        );
        let request = request.map(|stream| {
            stream
                .take_while(|frame| std::future::ready(frame.is_ok()))
                .filter_map(|frame| std::future::ready(frame.ok()))
        });
        forward::client(&ticket.destination.route)
            .map_err(unavailable)?
            .socket_session(request)
            .await
    }
}

impl ProxyService {
    fn ticket<T>(&self, request: &Request<T>, kind: ProxyTransport) -> Result<ProxyTicket, Status> {
        self.verifier
            .verify(crate::grpc::transport::token(request)?, kind)
            .map_err(|_| Status::unauthenticated("invalid proxy capability"))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvokeBody {
    request_id: String,
    owner_epoch: u64,
    method: String,
    args: Vec<Value>,
}

async fn invoke(
    State(service): State<Arc<ProxyService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    Json(body): Json<InvokeBody>,
) -> Result<Json<Value>, StatusCode> {
    let call = ActorCall {
        project_id: actor.project_id,
        actor_name: actor.actor_name,
        actor_id: actor.actor_id,
        request_id: body.request_id,
        owner_epoch: body.owner_epoch,
        method: body.method,
        args_json: serde_json::to_vec(&body.args).map_err(|_| StatusCode::BAD_REQUEST)?,
    };
    let reply = service
        .invoke(http_request(call, &headers)?)
        .await
        .map_err(http_status)?
        .into_inner();
    Ok(Json(
        serde_json::from_slice(&reply.reply_json).map_err(|_| StatusCode::BAD_GATEWAY)?,
    ))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishBody {
    owner_epoch: u64,
    effects: Vec<crate::actor::ActorSocketEffect>,
}

async fn publish(
    State(service): State<Arc<ProxyService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    Json(body): Json<PublishBody>,
) -> Result<StatusCode, StatusCode> {
    let target = ActorCall {
        project_id: actor.project_id,
        actor_name: actor.actor_name,
        actor_id: actor.actor_id,
        owner_epoch: body.owner_epoch,
        ..Default::default()
    };
    let request = SocketEffects {
        target: Some(target),
        effects_json: serde_json::to_vec(&body.effects).map_err(|_| StatusCode::BAD_REQUEST)?,
    };
    service
        .publish(http_request(request, &headers)?)
        .await
        .map_err(http_status)?;
    Ok(StatusCode::NO_CONTENT)
}

fn http_request<T>(value: T, headers: &HeaderMap) -> Result<Request<T>, StatusCode> {
    let mut request = Request::new(value);
    let authorization = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    request.metadata_mut().insert(
        "authorization",
        authorization
            .to_str()
            .map_err(|_| StatusCode::UNAUTHORIZED)?
            .parse()
            .map_err(|_| StatusCode::UNAUTHORIZED)?,
    );
    for name in ["traceparent", "tracestate", "x-request-id"] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok())
            && let Ok(value) = value.parse()
        {
            request.metadata_mut().insert(name, value);
        }
    }
    Ok(request)
}

#[derive(Deserialize)]
struct SocketQuery {
    key: String,
}

async fn socket(
    State(service): State<Arc<ProxyService>>,
    Query(query): Query<SocketQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<HttpResponse, StatusCode> {
    let ticket = service
        .verifier
        .verify(&query.key, ProxyTransport::Socket)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let activity = service
        .activity
        .enter()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let until = ticket
        .destination
        .authorized_until_ms
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? as i64;
    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(until.saturating_sub(now).max(0) as u64);
    let (outbound, receiver) = mpsc::unbounded_channel();
    let mut request = Request::new(UnboundedReceiverStream::new(receiver));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", ticket.destination.token)
            .parse()
            .map_err(|_| StatusCode::UNAUTHORIZED)?,
    );
    let stream = forward::client(&ticket.destination.route)
        .map_err(|_| StatusCode::BAD_GATEWAY)?
        .socket_session(request)
        .await
        .map_err(http_status)?
        .into_inner();
    Ok(upgrade
        .max_message_size(crate::actor::MAX_SOCKET_MESSAGE_BYTES)
        .max_frame_size(crate::actor::MAX_SOCKET_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let _activity = activity;
            bridge(socket, outbound, stream, deadline).await;
        }))
}

async fn bridge(
    socket: WebSocket,
    outbound: mpsc::UnboundedSender<SocketFrame>,
    mut inbound: Streaming<SocketFrame>,
    deadline: tokio::time::Instant,
) {
    let (mut sink, mut source) = socket.split();
    let upstream = async {
        while let Some(Ok(frame)) = source.next().await {
            let closed = matches!(frame, Message::Close(_));
            if outbound.send(encode_frame(frame)).is_err() || closed {
                break;
            }
        }
    };
    let downstream = async {
        while let Ok(Some(frame)) = inbound.message().await {
            let Ok(frame) = decode_frame(frame) else {
                break;
            };
            let closed = matches!(frame, Message::Close(_));
            if sink.send(frame).await.is_err() || closed {
                break;
            }
        }
    };
    let (code, reason) = tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => (4408, "socket authorization expired"),
        _ = upstream => (1012, "proxy session ended; reconnect"),
        _ = downstream => (1012, "proxy session ended; reconnect"),
    };
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        sink.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}

async fn write_readiness(config: &ProxyConfig) -> Result<()> {
    let metadata = serde_json::json!({
        "hostId": format!("proxy.v1.{}", config.session),
        "sessionId": config.session, "canonicalRegion": config.region.as_str(),
    });
    tokio::fs::write(
        "/tmp/durable-actors-host.json",
        serde_json::to_vec(&metadata)?,
    )
    .await?;
    tokio::fs::write("/tmp/durable-actors-ready", b"ready").await?;
    tokio::fs::write("/tmp/durable-actors-spare-ready", b"ready").await?;
    Ok(())
}

fn unavailable(error: impl std::fmt::Display) -> Status {
    tracing::warn!(error = %error, "regional forwarding failed");
    Status::unavailable("regional endpoint unavailable")
}

fn http_status(status: Status) -> StatusCode {
    match status.code() {
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::BAD_GATEWAY,
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/regional/proxy_transport.rs"]
mod tests;
