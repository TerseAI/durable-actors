use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use axum::{
    extract::ws::{CloseFrame, Message},
    http::{HeaderMap, HeaderValue},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use super::proto::{
    ActorCall, ActorReply, Empty, SocketClose, SocketEffects, SocketFrame,
    actor_service_server::{ActorService, ActorServiceServer},
    socket_frame,
};
use crate::{
    actor::{
        ActorInvocation, ActorKey, MAX_ACTOR_EXECUTOR_MESSAGE_BYTES, MAX_SOCKET_MESSAGE_BYTES,
    },
    host::http::ActorHostHttpService,
    sockets::browser::{self, SocketServerState, SocketTransport},
};

#[derive(Clone)]
pub(crate) struct PrimaryService {
    invocations: Arc<ActorHostHttpService>,
    sockets: SocketServerState,
}

impl PrimaryService {
    pub(crate) fn new(invocations: ActorHostHttpService, sockets: SocketServerState) -> Self {
        Self {
            invocations: Arc::new(invocations),
            sockets,
        }
    }

    pub(crate) fn service(self) -> ActorServiceServer<Self> {
        ActorServiceServer::new(self)
            .max_decoding_message_size(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl ActorService for PrimaryService {
    async fn invoke(&self, request: Request<ActorCall>) -> Result<Response<ActorReply>, Status> {
        let deadline = super::forward::deadline(&request)?;
        let mut headers = HeaderMap::new();
        let token = super::transport::token(&request)?;
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Status::unauthenticated("invalid invocation credential"))?,
        );
        let principal = self
            .invocations
            .authenticate(&headers)
            .map_err(http_error)?;
        let call = request.into_inner();
        let invocation = decode_call(&call)?;
        self.invocations
            .authorize(&principal, &invocation.actor, call.owner_epoch)
            .map_err(http_error)?;
        let reply = tokio::time::timeout_at(
            deadline.into(),
            self.invocations.execute(invocation, call.owner_epoch),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded("deadline elapsed; invocation outcome may be unknown")
        })?;
        Ok(Response::new(ActorReply {
            reply_json: serde_json::to_vec(&reply)
                .map_err(|_| Status::internal("encode actor result"))?,
        }))
    }

    async fn publish(&self, request: Request<SocketEffects>) -> Result<Response<Empty>, Status> {
        let token = super::transport::token(&request)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Status::unauthenticated("invalid invocation credential"))?,
        );
        let principal = self
            .invocations
            .authenticate(&headers)
            .map_err(http_error)?;
        let request = request.into_inner();
        let call = request
            .target
            .ok_or_else(|| Status::invalid_argument("missing actor target"))?;
        let actor = ActorKey {
            project_id: call.project_id,
            actor_name: call.actor_name,
            actor_id: call.actor_id,
        };
        self.invocations
            .authorize(&principal, &actor, call.owner_epoch)
            .map_err(http_error)?;
        let effects = serde_json::from_slice(&request.effects_json)
            .map_err(|_| Status::invalid_argument("invalid socket effects"))?;
        self.invocations
            .sockets
            .publish_authorized(
                &actor,
                self.invocations.host.id(),
                call.owner_epoch,
                effects,
            )
            .await
            .map_err(super::transport::unavailable)?;
        Ok(Response::new(Empty {}))
    }

    type SocketSessionStream = UnboundedReceiverStream<Result<SocketFrame, Status>>;

    async fn socket_session(
        &self,
        request: Request<tonic::Streaming<SocketFrame>>,
    ) -> Result<Response<Self::SocketSessionStream>, Status> {
        let ticket = self
            .sockets
            .verifier
            .verify(super::transport::token(&request)?)
            .map_err(|_| Status::unauthenticated("invalid upstream socket credential"))?;
        self.sockets
            .dispatcher
            .authorize(&ticket)
            .await
            .map_err(|_| Status::failed_precondition("socket primary is no longer current"))?;
        let (outbound, incoming) = mpsc::unbounded_channel();
        let socket = GrpcSocket {
            inbound: request.into_inner(),
            outbound,
        };
        tokio::spawn(browser::run(socket, self.sockets.clone(), ticket));
        Ok(Response::new(UnboundedReceiverStream::new(incoming)))
    }
}

pub(crate) fn decode_call(call: &ActorCall) -> Result<ActorInvocation, Status> {
    let invocation = ActorInvocation {
        actor: ActorKey {
            project_id: call.project_id.clone(),
            actor_name: call.actor_name.clone(),
            actor_id: call.actor_id.clone(),
        },
        request_id: call.request_id.clone(),
        method: call.method.clone(),
        args: serde_json::from_slice(&call.args_json)
            .map_err(|_| Status::invalid_argument("invalid actor arguments"))?,
    };
    invocation
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(invocation)
}

fn http_error(error: crate::host::http::HttpError) -> Status {
    match error.0.as_u16() {
        400 => Status::invalid_argument(error.1),
        401 => Status::unauthenticated(error.1),
        403 => Status::permission_denied(error.1),
        _ => Status::unavailable(error.1),
    }
}

struct GrpcSocket {
    inbound: tonic::Streaming<SocketFrame>,
    outbound: mpsc::UnboundedSender<Result<SocketFrame, Status>>,
}

#[async_trait::async_trait]
impl SocketTransport for GrpcSocket {
    async fn recv(&mut self) -> Option<Result<Message>> {
        match self.inbound.message().await {
            Ok(Some(frame)) => Some(decode_frame(frame)),
            Ok(None) => None,
            Err(error) => Some(Err(error.into())),
        }
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        self.outbound
            .send(Ok(encode_frame(message)))
            .context("proxy session closed")
    }
}

pub(crate) fn encode_frame(message: Message) -> SocketFrame {
    use socket_frame::Payload;
    let payload = match message {
        Message::Text(text) => Payload::Text(text.to_string()),
        Message::Binary(bytes) => Payload::Binary(bytes.to_vec()),
        Message::Ping(bytes) => Payload::Ping(bytes.to_vec()),
        Message::Pong(bytes) => Payload::Pong(bytes.to_vec()),
        Message::Close(close) => {
            let close = close.unwrap_or(CloseFrame {
                code: 1000,
                reason: "".into(),
            });
            Payload::Close(SocketClose {
                code: u32::from(close.code),
                reason: close.reason.to_string(),
            })
        }
    };
    SocketFrame {
        payload: Some(payload),
    }
}

pub(crate) fn decode_frame(frame: SocketFrame) -> Result<Message> {
    use socket_frame::Payload;
    let message = match frame.payload.context("socket frame has no payload")? {
        Payload::Text(text) => Message::Text(text.into()),
        Payload::Binary(bytes) => Message::Binary(bytes.into()),
        Payload::Ping(bytes) => {
            ensure!(bytes.len() <= 125, "oversized ping");
            Message::Ping(bytes.into())
        }
        Payload::Pong(bytes) => {
            ensure!(bytes.len() <= 125, "oversized pong");
            Message::Pong(bytes.into())
        }
        Payload::Close(close) => {
            ensure!(
                (1000..5000).contains(&close.code)
                    && !matches!(close.code, 1004..=1006 | 1015)
                    && close.reason.len() <= 123,
                "invalid socket close frame"
            );
            Message::Close(Some(CloseFrame {
                code: close.code as u16,
                reason: close.reason.into(),
            }))
        }
    };
    let length = match &message {
        Message::Text(text) => text.len(),
        Message::Binary(bytes) | Message::Ping(bytes) | Message::Pong(bytes) => bytes.len(),
        Message::Close(_) => 0,
    };
    ensure!(
        length <= MAX_SOCKET_MESSAGE_BYTES,
        "socket frame exceeds message limit"
    );
    Ok(message)
}

#[cfg(test)]
#[path = "../../tests/unit/grpc/actor.rs"]
mod tests;
