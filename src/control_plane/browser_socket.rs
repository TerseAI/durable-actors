use std::{collections::VecDeque, time::Duration};

use axum::{
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;
use tokio::{sync::mpsc, task::JoinHandle};

use super::{
    ActorPrincipal,
    socket_ticket::SocketTicket,
    websocket::{OutboundMessage, SocketServerState, dispatch},
};
use crate::actor::{
    ActorSocketConnection, ActorSocketEvent, ActorSocketMessage, MAX_SOCKET_MESSAGE_BYTES,
};

#[derive(Deserialize)]
pub(super) struct SocketQuery {
    key: Option<String>,
}

type Closed = (u16, &'static str);

pub(super) async fn connect(
    State(state): State<SocketServerState>,
    Query(query): Query<SocketQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let ticket = state
        .admin
        .verify_socket(query.key.as_deref().unwrap_or_default())
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(upgrade
        .max_frame_size(MAX_SOCKET_MESSAGE_BYTES)
        .max_message_size(MAX_SOCKET_MESSAGE_BYTES)
        .on_upgrade(move |socket| run(socket, state, ticket)))
}

async fn run(mut socket: WebSocket, state: SocketServerState, ticket: SocketTicket) {
    let connection = ActorSocketConnection {
        id: uuid::Uuid::new_v4().to_string(),
        metadata: ticket.metadata.clone(),
        tags: vec![],
    };
    let (sender, receiver) = mpsc::unbounded_channel();
    if !state
        .registry
        .insert(&ticket.actor, connection.clone(), sender, None)
        .await
    {
        close(&mut socket, (1013, "actor connection limit reached")).await;
        return;
    }
    let principal = ActorPrincipal::for_application(
        &ticket.actor.namespace_id,
        ticket.region.clone(),
        ticket.authorized_until_ms.div_euclid(1000) + 1,
    );
    let mut session = Session {
        state,
        ticket,
        principal,
        connection,
        outbound: receiver,
        pending: VecDeque::new(),
        handler: None,
    };
    session.start(ActorSocketEvent::Connect {
        connection: session.connection.clone(),
    });
    let closed = session
        .run(&mut socket)
        .await
        .unwrap_or_else(|closed| closed);
    close(&mut socket, closed).await;
    session.disconnect(closed).await;
}

struct Session {
    state: SocketServerState,
    ticket: SocketTicket,
    principal: ActorPrincipal,
    connection: ActorSocketConnection,
    outbound: mpsc::UnboundedReceiver<OutboundMessage>,
    pending: VecDeque<String>,
    handler: Option<JoinHandle<bool>>,
}

impl Session {
    async fn run(&mut self, socket: &mut WebSocket) -> Result<Closed, Closed> {
        loop {
            let remaining = self.remaining()?;
            tokio::select! {
                biased;
                _ = tokio::time::sleep(remaining) => return Err((4408, "socket authorization expired")),
                result = async { self.handler.as_mut().unwrap().await }, if self.handler.is_some() => {
                    self.handler.take();
                    if !result.unwrap_or(false) { return Err((4400, "actor socket handler failed")); }
                    if let Some(data) = self.pending.pop_front() { self.start_message(data); }
                }
                inbound = socket.recv() => self.receive(socket, inbound).await?,
                outbound = self.outbound.recv() => self.send_outbound(socket, outbound.ok_or((1006, "socket closed"))?).await?,
            }
        }
    }

    async fn receive(
        &mut self,
        socket: &mut WebSocket,
        inbound: Option<Result<Message, axum::Error>>,
    ) -> Result<(), Closed> {
        match inbound {
            Some(Ok(Message::Text(text))) => {
                if self.handler.is_none() {
                    self.start_message(text.to_string());
                } else if self.pending.len() < 32 {
                    self.pending.push_back(text.to_string());
                } else {
                    return Err((1013, "socket operation queue is full"));
                }
                Ok(())
            }
            Some(Ok(Message::Ping(data))) => self.send_frame(socket, Message::Pong(data)).await,
            Some(Ok(Message::Pong(_))) => Ok(()),
            Some(Ok(Message::Close(_))) => Err((1000, "client closed")),
            Some(Ok(Message::Binary(_))) => Err((4400, "socket messages must be JSON text")),
            Some(Err(_)) | None => Err((1006, "transport closed")),
        }
    }

    fn start_message(&mut self, data: String) {
        self.start(ActorSocketEvent::Message {
            connection_id: self.connection.id.clone(),
            message: ActorSocketMessage::Text { data },
        });
    }

    fn start(&mut self, event: ActorSocketEvent) {
        let state = self.state.clone();
        let actor = self.ticket.actor.clone();
        let principal = self.principal.clone();
        let deliver = matches!(event, ActorSocketEvent::Message { .. });
        self.handler = Some(tokio::spawn(async move {
            dispatch(&state, &actor, &principal, event, deliver).await
        }));
    }

    async fn send_outbound(
        &self,
        socket: &mut WebSocket,
        outbound: OutboundMessage,
    ) -> Result<(), Closed> {
        match outbound {
            OutboundMessage::Control(_) => Ok(()),
            OutboundMessage::Message(ActorSocketMessage::Text { data }) => {
                self.send_frame(socket, Message::Text(data.into())).await
            }
            OutboundMessage::Message(_) => Err((4400, "actor produced a binary message")),
            OutboundMessage::Close { code, reason } => {
                let _ = self
                    .send_frame(
                        socket,
                        Message::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        })),
                    )
                    .await;
                Err((code, "actor closed connection"))
            }
        }
    }

    async fn send_frame(&self, socket: &mut WebSocket, frame: Message) -> Result<(), Closed> {
        tokio::time::timeout(
            self.remaining()?.min(Duration::from_secs(5)),
            socket.send(frame),
        )
        .await
        .map_err(|_| (4408, "socket delivery timed out"))?
        .map_err(|_| (1006, "transport closed"))
    }

    fn remaining(&self) -> Result<Duration, Closed> {
        let millis = self.ticket.authorized_until_ms - now_ms();
        if millis <= 0 {
            return Err((4408, "socket authorization expired"));
        }
        Ok(Duration::from_millis(millis as u64))
    }

    async fn disconnect(mut self, closed: Closed) {
        let connection = self
            .state
            .registry
            .remove(&self.ticket.actor, &self.connection.id)
            .await
            .unwrap_or(self.connection.clone());
        if let Some(handler) = self.handler.take() {
            let _ = handler.await;
        }
        let mut principal = self.principal.clone();
        principal.expires_at = now_ms().div_euclid(1000) + 60;
        let event = ActorSocketEvent::Disconnect {
            connection,
            code: closed.0,
            reason: closed.1.into(),
            was_clean: closed.0 == 1000,
        };
        dispatch(&self.state, &self.ticket.actor, &principal, event, false).await;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn close(socket: &mut WebSocket, (code, reason): Closed) {
    if code == 1006 {
        return;
    }
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        socket.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}
