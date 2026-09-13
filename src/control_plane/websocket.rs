use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;

use crate::{
    actor::{
        ActorKey, ActorSocketConnection, ActorSocketEffect, ActorSocketEvent,
        ActorSocketInvocation, ActorSocketMessage, ActorSocketTagMatch, MAX_SOCKET_MESSAGE_BYTES,
        MAX_SOCKET_METADATA_BYTES, validate_socket_effects,
    },
    control_plane::ActorPrincipal,
};

use super::service::ControlPlaneService;
use super::{admin::AdminService, public_api::ActorPath};

const MAX_CONNECTIONS_PER_ACTOR: usize = 128;
const MAX_SOCKET_EFFECTS_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const SOCKET_INITIALIZATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
pub(super) struct SocketServerState {
    pub(super) service: ControlPlaneService,
    pub(super) registry: SocketRegistry,
    pub(super) admin: AdminService,
}

#[derive(Clone, Default)]
pub(crate) struct SocketRegistry {
    entries: Arc<RwLock<HashMap<ActorKey, HashMap<String, RegisteredSocket>>>>,
}

#[derive(Clone)]
struct RegisteredSocket {
    connection: ActorSocketConnection,
    outbound: mpsc::UnboundedSender<OutboundMessage>,
    open: bool,
    state_ready: bool,
    trigger_id: Option<String>,
}

struct SocketAccess {
    actor: ActorKey,
    principal: ActorPrincipal,
    metadata: Option<Value>,
    trigger_id: Option<String>,
}

pub(super) enum OutboundMessage {
    Message(ActorSocketMessage),
    Control(Value),
    Close { code: u16, reason: String },
}

pub(crate) fn router(
    service: ControlPlaneService,
    registry: SocketRegistry,
    admin: AdminService,
) -> Router {
    Router::new()
        .route(
            "/v1/actors/{actor_type}/{actor_id}/websocket",
            get(connect_workflow),
        )
        .route(
            "/v1/actors/{actor_type}/{actor_id}/socket-effects",
            post(apply_effects),
        )
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/websocket",
            get(connect_workflow),
        )
        .route("/v1/socket", get(super::browser_socket::connect))
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/socket-effects",
            post(apply_effects),
        )
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/connections",
            get(list_connections),
        )
        .layer(DefaultBodyLimit::max(MAX_SOCKET_EFFECTS_REQUEST_BYTES))
        .with_state(SocketServerState {
            service,
            registry,
            admin,
        })
}

async fn list_connections(
    State(state): State<SocketServerState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
) -> Result<Json<Vec<ActorSocketConnection>>, SocketApiError> {
    let (actor, principal) = authenticate_socket(&state, &headers, path)?;
    state
        .service
        .socket_connections(&principal, &actor)
        .await
        .map(Json)
        .map_err(|_| SocketApiError::forbidden("host cannot list connections for this actor"))
}

async fn apply_effects(
    State(state): State<SocketServerState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    Json(request): Json<ApplyEffectsRequest>,
) -> Result<StatusCode, SocketApiError> {
    let (actor, principal) = authenticate_socket(&state, &headers, path)?;
    if principal.process_role == crate::host::ActorProcessRole::Host {
        state
            .service
            .publish_socket_effects(&principal, actor, request.effects)
            .await
            .map_err(|_| SocketApiError::forbidden("host cannot publish for this actor"))?;
        return Ok(StatusCode::NO_CONTENT);
    }
    authorize_workflow(&principal, &actor)?;
    validate_socket_effects(&request.effects).map_err(SocketApiError::bad_request)?;
    state.registry.apply(&actor, request.effects).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn connect_workflow(
    State(state): State<SocketServerState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, SocketApiError> {
    let (actor, principal) = authenticate_socket(&state, &headers, path)?;
    authorize_workflow(&principal, &actor)?;
    upgrade_socket(
        upgrade,
        state,
        SocketAccess {
            actor,
            principal,
            metadata: None,
            trigger_id: None,
        },
    )
}

fn upgrade_socket(
    upgrade: WebSocketUpgrade,
    state: SocketServerState,
    access: SocketAccess,
) -> Result<Response, SocketApiError> {
    Ok(upgrade
        .max_frame_size(MAX_SOCKET_MESSAGE_BYTES)
        .max_message_size(MAX_SOCKET_MESSAGE_BYTES)
        .on_upgrade(move |socket| run_connection(socket, state, access))
        .into_response())
}

async fn run_connection(mut socket: WebSocket, state: SocketServerState, access: SocketAccess) {
    let actor = access.actor;
    let principal = access.principal;
    let metadata = match access.metadata {
        Some(metadata) => metadata,
        None => {
            let Some(metadata) = receive_metadata(&mut socket).await else {
                let _ =
                    close_socket(&mut socket, 1002, "socket metadata was not initialized").await;
                return;
            };
            metadata
        }
    };
    let connection = ActorSocketConnection {
        id: uuid::Uuid::new_v4().to_string(),
        metadata,
        tags: Vec::new(),
    };
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
    if !state
        .registry
        .insert(&actor, connection.clone(), outbound_tx, access.trigger_id)
        .await
    {
        let _ = close_socket(&mut socket, 1013, "actor connection limit reached").await;
        return;
    }

    if !dispatch(
        &state,
        &actor,
        &principal,
        ActorSocketEvent::Connect {
            connection: connection.clone(),
        },
        false,
    )
    .await
    {
        state.registry.remove(&actor, &connection.id).await;
        let _ = close_socket(&mut socket, 1011, "actor rejected the connection").await;
        return;
    }

    let mut disconnect = (1006, String::new(), false);
    loop {
        tokio::select! {
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(Message::Text(data))) => {
                        match run_handler_with_output(&mut socket, &mut outbound_rx, dispatch(&state, &actor, &principal, ActorSocketEvent::Message {
                            connection_id: connection.id.clone(),
                            message: ActorSocketMessage::Text { data: data.to_string() },
                        }, true)).await {
                            Ok(true) => {},
                            Ok(false) => { disconnect = (1011, "actor socket handler failed".into(), false); break; },
                            Err(closed) => { disconnect = closed; break; },
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        match run_handler_with_output(&mut socket, &mut outbound_rx, dispatch(&state, &actor, &principal, ActorSocketEvent::Message {
                            connection_id: connection.id.clone(),
                            message: ActorSocketMessage::Binary { data: STANDARD.encode(data) },
                        }, true)).await {
                            Ok(true) => {},
                            Ok(false) => { disconnect = (1011, "actor socket handler failed".into(), false); break; },
                            Err(closed) => { disconnect = closed; break; },
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        disconnect = frame.map_or((1000, String::new(), true), |frame| (frame.code, frame.reason.to_string(), true));
                        break;
                    }
                    Some(Err(_)) | None => break,
                }
            }
            outbound = outbound_rx.recv() => {
                if let Err(closed) = send_outbound(&mut socket, outbound).await {
                    disconnect = closed;
                    break;
                }
            }
        }
    }

    let _ = dispatch(
        &state,
        &actor,
        &principal,
        ActorSocketEvent::Disconnect {
            connection: connection.clone(),
            code: disconnect.0,
            reason: disconnect.1,
            was_clean: disconnect.2,
        },
        false,
    )
    .await;
    state.registry.remove(&actor, &connection.id).await;
}

type SocketDisconnect = (u16, String, bool);

async fn run_handler_with_output(
    socket: &mut WebSocket,
    outbound: &mut mpsc::UnboundedReceiver<OutboundMessage>,
    handler: impl std::future::Future<Output = bool>,
) -> Result<bool, SocketDisconnect> {
    tokio::pin!(handler);
    loop {
        tokio::select! {
            result = &mut handler => return Ok(result),
            message = outbound.recv() => send_outbound(socket, message).await?,
        }
    }
}

async fn send_outbound(
    socket: &mut WebSocket,
    message: Option<OutboundMessage>,
) -> Result<(), SocketDisconnect> {
    match message {
        Some(OutboundMessage::Control(value)) => socket
            .send(Message::Text(value.to_string().into()))
            .await
            .map_err(|_| (1006, String::new(), false)),
        Some(OutboundMessage::Message(message)) => {
            let message = websocket_message(message).ok_or_else(|| {
                (
                    1011,
                    "actor produced an invalid socket message".into(),
                    false,
                )
            })?;
            socket
                .send(message)
                .await
                .map_err(|_| (1006, String::new(), false))
        }
        Some(OutboundMessage::Close { code, reason }) => {
            let _ = close_socket(socket, code, &reason).await;
            Err((code, reason, true))
        }
        None => Err((1006, String::new(), false)),
    }
}

pub(super) async fn dispatch(
    state: &SocketServerState,
    actor: &ActorKey,
    principal: &ActorPrincipal,
    event: ActorSocketEvent,
    trigger_message: bool,
) -> bool {
    let (event, connections) = state.registry.prepare_event(actor, event).await;
    let connecting = match &event {
        ActorSocketEvent::Connect { connection } => Some(connection.id.clone()),
        _ => None,
    };
    let message_connection_id = match &event {
        ActorSocketEvent::Message { connection_id, .. } => Some(connection_id.clone()),
        _ => None,
    };
    let delivered_event = event.clone();
    let invocation = ActorSocketInvocation {
        request_id: uuid::Uuid::new_v4().to_string(),
        actor: actor.clone(),
        event,
        connections,
    };
    match state
        .service
        .dispatch_socket_event(principal, invocation)
        .await
    {
        Ok(effects) => {
            if let Err(error) = validate_socket_effects(&effects) {
                warn!(actor = %actor.storage_key(), error = %format!("{error:#}"), "actor returned invalid socket effects");
                return false;
            }
            state.registry.apply(actor, effects).await;
            if let Some(connection_id) = connecting {
                state.registry.activate(actor, &connection_id).await;
            }
            if trigger_message {
                let trigger_id = state
                    .registry
                    .trigger_id(actor, message_connection_id.as_deref())
                    .await;
                state
                    .service
                    .deliver_socket_message_event(actor, trigger_id, &delivered_event);
            }
            true
        }
        Err(error) => {
            warn!(actor = %actor.storage_key(), error = %format!("{error:#}"), "actor socket event failed");
            false
        }
    }
}

impl SocketRegistry {
    pub(super) async fn insert(
        &self,
        actor: &ActorKey,
        connection: ActorSocketConnection,
        outbound: mpsc::UnboundedSender<OutboundMessage>,
        trigger_id: Option<String>,
    ) -> bool {
        let mut entries = self.entries.write().await;
        let connections = entries.entry(actor.clone()).or_default();
        if connections.len() >= MAX_CONNECTIONS_PER_ACTOR {
            return false;
        }
        connections.insert(
            connection.id.clone(),
            RegisteredSocket {
                connection,
                outbound,
                open: false,
                state_ready: false,
                trigger_id,
            },
        );
        true
    }

    pub(super) async fn remove(
        &self,
        actor: &ActorKey,
        connection_id: &str,
    ) -> Option<ActorSocketConnection> {
        let mut entries = self.entries.write().await;
        let connections = entries.get_mut(actor)?;
        let removed = connections
            .remove(connection_id)
            .map(|entry| entry.connection);
        if connections.is_empty() {
            entries.remove(actor);
        }
        removed
    }

    pub(super) async fn connections(&self, actor: &ActorKey) -> Vec<ActorSocketConnection> {
        self.entries
            .read()
            .await
            .get(actor)
            .map(|entries| {
                entries
                    .values()
                    .filter(|entry| entry.open)
                    .map(|entry| entry.connection.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn trigger_id(&self, actor: &ActorKey, connection_id: Option<&str>) -> Option<String> {
        let connection_id = connection_id?;
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|connections| connections.get(connection_id))
            .and_then(|entry| entry.trigger_id.clone())
    }

    async fn prepare_event(
        &self,
        actor: &ActorKey,
        event: ActorSocketEvent,
    ) -> (ActorSocketEvent, Vec<ActorSocketConnection>) {
        match event {
            ActorSocketEvent::Connect { connection } => {
                let mut connections = self.connections(actor).await;
                connections.push(connection.clone());
                (ActorSocketEvent::Connect { connection }, connections)
            }
            ActorSocketEvent::Message {
                connection_id,
                message,
            } => (
                ActorSocketEvent::Message {
                    connection_id,
                    message,
                },
                self.connections(actor).await,
            ),
            ActorSocketEvent::Disconnect {
                connection,
                code,
                reason,
                was_clean,
            } => {
                let connection = self
                    .remove(actor, &connection.id)
                    .await
                    .unwrap_or(connection);
                (
                    ActorSocketEvent::Disconnect {
                        connection,
                        code,
                        reason,
                        was_clean,
                    },
                    self.connections(actor).await,
                )
            }
        }
    }

    pub(super) async fn apply(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) {
        for effect in effects {
            self.apply_one(actor, effect).await;
        }
    }

    async fn apply_one(&self, actor: &ActorKey, effect: ActorSocketEffect) {
        match effect {
            ActorSocketEffect::StateSnapshot {
                connection_id,
                state,
                version,
            } => {
                let Some(version) = version else {
                    return;
                };
                let mut entries = self.entries.write().await;
                if let Some(entry) = entries
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.state_ready = true;
                    let _ = entry.outbound.send(OutboundMessage::Control(
                        serde_json::json!({ "type":"state", "state":state, "version":version }),
                    ));
                }
            }
            ActorSocketEffect::StateUpdate {
                changes,
                removed,
                except_connection_ids,
                version,
            } => {
                let Some(version) = version else {
                    return;
                };
                let entries = self.entries.read().await;
                if let Some(connections) = entries.get(actor) {
                    let value = serde_json::json!({ "type":"state_update", "changes":changes, "removed":removed, "version":version });
                    for entry in connections.values().filter(|entry| {
                        entry.state_ready && !except_connection_ids.contains(&entry.connection.id)
                    }) {
                        let _ = entry.outbound.send(OutboundMessage::Control(value.clone()));
                    }
                }
            }
            ActorSocketEffect::Broadcast {
                message,
                except_connection_ids,
                tags,
                tag_match,
            } => {
                let recipients = self
                    .entries
                    .read()
                    .await
                    .get(actor)
                    .map(|connections| {
                        connections
                            .values()
                            .filter(|entry| {
                                entry.open
                                    && !except_connection_ids.contains(&entry.connection.id)
                                    && (tags.is_empty()
                                        || match tag_match {
                                            ActorSocketTagMatch::All => tags
                                                .iter()
                                                .all(|tag| entry.connection.tags.contains(tag)),
                                            ActorSocketTagMatch::Any => tags
                                                .iter()
                                                .any(|tag| entry.connection.tags.contains(tag)),
                                        })
                            })
                            .map(|entry| entry.outbound.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for sender in recipients {
                    let _ = sender.send(OutboundMessage::Message(message.clone()));
                }
            }
            ActorSocketEffect::SetMetadata {
                connection_id,
                metadata,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.metadata = metadata;
                }
            }
            ActorSocketEffect::SetTags {
                connection_id,
                tags,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.tags = tags;
                }
            }
            ActorSocketEffect::Send {
                connection_id,
                message,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Message(message));
                }
            }
            ActorSocketEffect::Close {
                connection_id,
                code,
                reason,
            }
            | ActorSocketEffect::Reject {
                connection_id,
                code,
                reason,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Close { code, reason });
                }
            }
        }
    }

    async fn sender(
        &self,
        actor: &ActorKey,
        connection_id: &str,
    ) -> Option<mpsc::UnboundedSender<OutboundMessage>> {
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|connections| connections.get(connection_id))
            .map(|entry| entry.outbound.clone())
    }

    async fn activate(&self, actor: &ActorKey, connection_id: &str) {
        if let Some(entry) = self
            .entries
            .write()
            .await
            .get_mut(actor)
            .and_then(|connections| connections.get_mut(connection_id))
        {
            entry.open = true;
        }
    }
}

fn authenticate_socket(
    state: &SocketServerState,
    headers: &HeaderMap,
    path: ActorPath,
) -> Result<(ActorKey, ActorPrincipal), SocketApiError> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| SocketApiError::unauthorized("application credential is required"))?
        .to_str()
        .map_err(|_| SocketApiError::unauthorized("application credential is invalid"))?;
    let principal = state
        .service
        .authenticate_application(&state.admin, authorization, path.namespace_id.as_deref())
        .map_err(|_| SocketApiError::unauthorized("application credential was rejected"))?;
    let actor = path.into_actor(&principal.scope.namespace_id);
    actor.validate().map_err(SocketApiError::bad_request)?;
    Ok((actor, principal))
}

fn authorize_workflow(principal: &ActorPrincipal, actor: &ActorKey) -> Result<(), SocketApiError> {
    if principal.process_role != crate::host::ActorProcessRole::Workflow
        || !principal.scope.contains(actor)
    {
        return Err(SocketApiError::forbidden(
            "workflow token is not valid for this actor",
        ));
    }
    Ok(())
}

async fn receive_metadata(socket: &mut WebSocket) -> Option<Value> {
    let message = tokio::time::timeout(SOCKET_INITIALIZATION_TIMEOUT, socket.recv())
        .await
        .ok()??
        .ok()?;
    let Message::Text(document) = message else {
        return None;
    };
    if document.len() > MAX_SOCKET_METADATA_BYTES + 128 {
        return None;
    }
    let initialization = serde_json::from_str::<SocketInitialization>(&document).ok()?;
    crate::actor::validate_socket_metadata(&initialization.metadata)
        .ok()
        .map(|()| initialization.metadata)
}

fn websocket_message(message: ActorSocketMessage) -> Option<Message> {
    match message {
        ActorSocketMessage::Text { data } => Some(Message::Text(data.into())),
        ActorSocketMessage::Binary { data } => STANDARD
            .decode(data)
            .ok()
            .map(|data| Message::Binary(data.into())),
    }
}

async fn close_socket(socket: &mut WebSocket, code: u16, reason: &str) -> Result<(), axum::Error> {
    socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.to_owned().into(),
        })))
        .await
}

#[derive(Deserialize)]
struct SocketInitialization {
    #[serde(rename = "type")]
    _message_type: SocketInitializationType,
    metadata: Value,
}

#[derive(Deserialize)]
struct ApplyEffectsRequest {
    effects: Vec<ActorSocketEffect>,
}

#[derive(Deserialize)]
enum SocketInitializationType {
    #[serde(rename = "initialize")]
    Initialize,
}

struct SocketApiError {
    status: StatusCode,
    message: &'static str,
}

impl SocketApiError {
    fn bad_request(_error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: "invalid socket request",
        }
    }

    fn unauthorized(message: &'static str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message,
        }
    }

    fn forbidden(message: &'static str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message,
        }
    }
}

impl IntoResponse for SocketApiError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn broadcast_matches_all_or_any_tags_and_preserves_exclusions() {
        let registry = SocketRegistry::default();
        let actor = ActorKey {
            namespace_id: "default".into(),
            actor_type: "Files".into(),
            actor_id: "files".into(),
        };
        let mut receivers = Vec::new();
        for (id, tags, open) in [
            ("a", vec!["file:a"], true),
            ("b", vec!["file:b"], true),
            ("both", vec!["file:a", "file:b", "file:c"], true),
            ("neither", vec!["file:c"], true),
            ("excluded", vec!["file:a", "file:b"], true),
            ("connecting", vec!["file:a", "file:b"], false),
        ] {
            let (outbound, receiver) = mpsc::unbounded_channel();
            assert!(
                registry
                    .insert(
                        &actor,
                        ActorSocketConnection {
                            id: id.into(),
                            metadata: json!({}),
                            tags: Vec::new(),
                        },
                        outbound,
                        None
                    )
                    .await
            );
            if open {
                registry.activate(&actor, id).await;
            }
            registry
                .apply(
                    &actor,
                    vec![ActorSocketEffect::SetTags {
                        connection_id: id.into(),
                        tags: tags.into_iter().map(String::from).collect(),
                    }],
                )
                .await;
            receivers.push((id, receiver));
        }
        for (mode, tags, expected) in [
            (None, vec!["file:a", "file:b"], vec!["both"]),
            (Some("all"), vec!["file:a", "file:b"], vec!["both"]),
            (
                Some("any"),
                vec!["file:a", "file:b"],
                vec!["a", "b", "both"],
            ),
            (Some("any"), vec!["file:missing"], vec![]),
            (Some("all"), vec![], vec!["a", "b", "both", "neither"]),
            (Some("any"), vec![], vec!["a", "b", "both", "neither"]),
        ] {
            let mut effect = json!({
                "type": "broadcast", "message": {"type": "text", "data": "ready"},
                "except_connection_ids": ["excluded"], "tags": tags,
            });
            if let Some(mode) = mode {
                effect["tag_match"] = json!(mode);
            }
            registry
                .apply(&actor, vec![serde_json::from_value(effect).unwrap()])
                .await;
            for (id, receiver) in &mut receivers {
                let delivered = receiver.try_recv().ok();
                assert_eq!(
                    delivered.is_some(),
                    expected.contains(id),
                    "mode={mode:?}, socket={id}"
                );
                if let Some(message) = delivered {
                    assert!(
                        matches!(message, OutboundMessage::Message(ActorSocketMessage::Text { data }) if data == "ready")
                    );
                }
                assert!(receiver.try_recv().is_err(), "duplicate delivery to {id}");
            }
        }
    }

    #[test]
    fn broadcast_rejects_invalid_tag_matching_mode() {
        assert!(
            serde_json::from_value::<ActorSocketEffect>(json!({
                "type": "broadcast", "message": {"type": "text", "data": "ready"},
                "except_connection_ids": [], "tags": [], "tag_match": "either",
            }))
            .is_err()
        );
    }

    #[test]
    fn accepts_structured_public_state_effects_and_rejects_invalid_snapshots() -> anyhow::Result<()>
    {
        let effects: Vec<ActorSocketEffect> = serde_json::from_value(json!([
            {"type":"state_snapshot","connection_id":"socket","state":{"count":1}},
            {"type":"state_update","changes":{"count":2},"removed":["optional"]}
        ]))?;
        validate_socket_effects(&effects)?;
        let invalid: Vec<ActorSocketEffect> = serde_json::from_value(json!([
            {"type":"state_snapshot","connection_id":"socket","state":null}
        ]))?;
        assert!(validate_socket_effects(&invalid).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn socket_writer_runs_while_actor_handler_is_pending() -> anyhow::Result<()> {
        use futures_util::StreamExt;
        let (release, waiting) = tokio::sync::oneshot::channel::<()>();
        let waiting = Arc::new(std::sync::Mutex::new(Some(waiting)));
        let app = Router::new().route(
            "/",
            get(move |upgrade: WebSocketUpgrade| {
                let waiting = waiting.lock().unwrap().take().unwrap();
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let (outbound, mut incoming) = mpsc::unbounded_channel();
                        outbound
                            .send(OutboundMessage::Message(ActorSocketMessage::Text {
                                data: "before-return".into(),
                            }))
                            .unwrap();
                        let result = run_handler_with_output(&mut socket, &mut incoming, async {
                            waiting.await.unwrap();
                            true
                        })
                        .await;
                        assert!(matches!(result, Ok(true)));
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/")).await?;
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
            .await?
            .unwrap()?;
        assert_eq!(message.into_text()?, "before-return");
        release.send(()).unwrap();
        let _ = socket.next().await;
        server.abort();
        Ok(())
    }

    #[test]
    fn rejects_socket_effects_that_bypass_sdk_invariants() {
        let invalid = [
            ActorSocketEffect::Close {
                connection_id: "socket-1".into(),
                code: 1001,
                reason: String::new(),
            },
            ActorSocketEffect::SetTags {
                connection_id: "socket-1".into(),
                tags: vec!["x".repeat(257)],
            },
            ActorSocketEffect::SetMetadata {
                connection_id: "socket-1".into(),
                metadata: json!({ "data": "x".repeat(64 * 1024) }),
            },
            ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: ActorSocketMessage::Binary {
                    data: "not base64".into(),
                },
            },
        ];

        for effect in invalid {
            assert!(crate::actor::validate_socket_effects(&[effect]).is_err());
        }
    }

    #[tokio::test]
    async fn registry_retains_metadata_tags_and_outbound_messages() {
        let registry = SocketRegistry::default();
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "ChatRoom".into(),
            actor_id: "room-1".into(),
        };
        let (outbound, mut messages) = mpsc::unbounded_channel();
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    },
                    outbound,
                    None,
                )
                .await
        );
        registry.activate(&actor, "socket-1").await;

        registry
            .apply(
                &actor,
                vec![
                    ActorSocketEffect::SetMetadata {
                        connection_id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1", "ready": true }),
                    },
                    ActorSocketEffect::SetTags {
                        connection_id: "socket-1".into(),
                        tags: vec!["member".into()],
                    },
                    ActorSocketEffect::Send {
                        connection_id: "socket-1".into(),
                        message: ActorSocketMessage::Text {
                            data: "hello".into(),
                        },
                    },
                    ActorSocketEffect::Broadcast {
                        message: ActorSocketMessage::Text {
                            data: "everyone".into(),
                        },
                        except_connection_ids: Vec::new(),
                        tags: vec!["member".into()],
                        tag_match: ActorSocketTagMatch::All,
                    },
                ],
            )
            .await;

        assert_eq!(
            registry.connections(&actor).await,
            vec![ActorSocketConnection {
                id: "socket-1".into(),
                metadata: json!({ "userId": "user-1", "ready": true }),
                tags: vec!["member".into()],
            }]
        );
        assert!(matches!(
            messages.recv().await,
            Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "hello"
        ));
        assert!(matches!(
            messages.recv().await,
            Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "everyone"
        ));
    }
}
