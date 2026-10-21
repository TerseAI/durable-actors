use super::{
    service::{ActorTarget, ControlPlaneService},
    socket_bus::SocketBus,
    socket_inventory::SocketInventory,
    socket_ticket::SocketTicket,
};
use crate::request_tracking::HostState;
use crate::{
    actor::{ActorKey, ActorSocketEffect, ActorSocketInvocation},
    cache::{Cache, Caches},
    sockets::{SocketRegistry, browser::SocketDispatcher},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) use crate::sockets::operations::{SocketOperation, SocketOperationReply};
#[derive(Serialize, Deserialize)]
pub(crate) struct SocketEventRequest {
    #[serde(default)]
    pub host_state: HostState,
    pub routing_ms: f64,
    pub owner_epoch: u64,
    pub invocation: ActorSocketInvocation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SocketEventReply {
    Completed { effects: Vec<ActorSocketEffect> },
    NotExecuted,
    Failed { message: String },
}

type ActorRoute = Arc<tokio::sync::Mutex<Option<ActorTarget>>>;

/// Holds this process's browser sockets and reaches every other gateway's
/// through the socket bus: a socket stays on the gateway the browser reached,
/// and publishes go to every gateway holding sockets for the actor.
pub(super) struct SocketGateway {
    pub registry: SocketRegistry,
    pub stop: CancellationToken,
    pub(super) bus: Arc<dyn SocketBus>,
    /// Whether browsers' sockets may open here. With a dedicated socket
    /// gateway, control-plane pods only publish and ask.
    pub(super) accepts_sockets: bool,
    /// Each actor's resolved host, behind a lock so one resolution serves
    /// concurrent events. Locks only exist in this process.
    routes: Cache<ActorRoute>,
    pub(super) http: reqwest::Client,
}

impl SocketGateway {
    pub(super) fn start(
        bus: Arc<dyn SocketBus>,
        accepts_sockets: bool,
        caches: &Caches,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            registry: bus.registry().clone(),
            bus,
            accepts_sockets,
            stop,
            routes: caches.local("socket-routes", 100_000, Duration::from_secs(300)),
            http: reqwest::Client::builder()
                .http2_prior_knowledge()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .build()?,
        }))
    }

    /// The bus generation a socket opening now belongs to; `None` while this
    /// gateway cannot receive publishes.
    pub(super) fn generation(&self) -> Option<u64> {
        if self.stop.is_cancelled() {
            return None;
        }
        self.bus.generation()
    }

    /// Fails once a socket opened under `generation` may have missed a publish.
    pub(super) fn ensure_delivery(&self, generation: u64) -> Result<()> {
        ensure!(
            self.generation() == Some(generation),
            "socket gateway lost its subscription"
        );
        Ok(())
    }

    pub(super) async fn operation(
        &self,
        actor: &ActorKey,
        operation: SocketOperation,
    ) -> Result<SocketOperationReply> {
        ensure!(!self.stop.is_cancelled(), "socket gateway stopping");
        match operation {
            SocketOperation::Publish { effects } => {
                crate::actor::validate_socket_effects(&effects)?;
                self.bus.publish(actor, effects).await?;
                Ok(SocketOperationReply::Published)
            }
            SocketOperation::Connections { tag } => Ok(SocketOperationReply::Connections {
                connections: self.bus.connections(actor, tag.as_deref()).await?,
            }),
            SocketOperation::Count => Ok(SocketOperationReply::Count {
                count: self.bus.count(actor).await?,
            }),
        }
    }

    pub(super) async fn inventory(&self, project: &str) -> Result<SocketInventory> {
        ensure!(!self.stop.is_cancelled(), "socket gateway stopping");
        self.bus.inventory(project).await
    }

    async fn target(
        &self,
        service: &ControlPlaneService,
        ticket: &SocketTicket,
    ) -> Result<ActorTarget> {
        let route = self.route(&ticket.actor).await;
        let mut cached = route.lock().await;
        let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
        if let Some(target) = cached.as_ref().filter(|target| target.expires_at_ms > now) {
            return Ok(target.clone());
        }
        let target = service
            .resolve_actor_route(&ticket.actor, ticket.home_region.as_deref(), None, None)
            .await?;
        let mut cached_target = target.clone();
        cached_target.host_state = HostState::Warm;
        *cached = Some(cached_target);
        Ok(target)
    }

    async fn route(&self, actor: &ActorKey) -> ActorRoute {
        // A local cache never fails, and its load cannot.
        self.routes
            .get_or_try_init(actor.storage_key().as_str(), async { Ok(Arc::default()) })
            .await
            .unwrap_or_default()
    }

    async fn invalidate(&self, actor: &ActorKey, target: &ActorTarget) {
        let route = self.route(actor).await;
        let mut cached = route.lock().await;
        if cached.as_ref().is_some_and(|current| {
            current.owner_epoch == target.owner_epoch && current.route == target.route
        }) {
            *cached = None;
        }
    }

    async fn dispatch(
        &self,
        service: &ControlPlaneService,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let started = Instant::now();
        let retry_deadline = started + super::CONTROL_PLANE_REQUEST_TIMEOUT;
        let mut host_state = HostState::Warm;
        loop {
            ensure!(!self.stop.is_cancelled(), "socket gateway stopping");
            ensure!(
                Instant::now() < retry_deadline,
                "actor remained unavailable during socket handoff"
            );
            let target = tokio::time::timeout_at(retry_deadline, self.target(service, ticket))
                .await
                .context("actor remained unavailable during socket handoff")?;
            let target = match target {
                Ok(target) => target,
                Err(error) if error.is::<crate::sandbox::HostNotReady>() => {
                    self.wait_to_retry().await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if target.host_state == HostState::Cold {
                host_state = HostState::Cold;
            }
            let actor = &ticket.actor;
            let url = format!(
                "{}/v1/projects/{}/actors/{}/{}/socket-events",
                target.route.trim_end_matches('/'),
                actor.project_id,
                actor.actor_name,
                actor.actor_id
            );
            let reply = self
                .http
                .post(url)
                .bearer_auth(&target.token)
                .json(&SocketEventRequest {
                    host_state,
                    routing_ms: started.elapsed().as_secs_f64() * 1000.0,
                    owner_epoch: target.owner_epoch,
                    invocation: invocation.clone(),
                })
                .send()
                .await;
            match reply {
                Err(error) if error.is_connect() => {
                    self.invalidate(actor, &target).await;
                }
                Err(error) => {
                    return Err(error)
                        .context("socket event outcome unknown; event was not replayed");
                }
                Ok(reply)
                    if matches!(
                        reply.status(),
                        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                    ) =>
                {
                    self.invalidate(actor, &target).await;
                }
                Ok(reply) => match reply.error_for_status()?.json::<SocketEventReply>().await? {
                    SocketEventReply::Completed { effects } => return Ok(effects),
                    SocketEventReply::NotExecuted => {
                        self.invalidate(actor, &target).await;
                    }
                    SocketEventReply::Failed { message } => {
                        anyhow::bail!("socket handler failed: {message}")
                    }
                },
            }
            self.wait_to_retry().await?;
        }
    }

    async fn wait_to_retry(&self) -> Result<()> {
        tokio::select! {
            _ = self.stop.cancelled() => anyhow::bail!("socket gateway stopped"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => Ok(()),
        }
    }
}

pub(super) struct GatewaySocketDispatcher {
    pub gateway: Arc<SocketGateway>,
    pub service: ControlPlaneService,
    /// The bus generation the socket opened under.
    pub generation: u64,
}

#[async_trait]
impl SocketDispatcher for GatewaySocketDispatcher {
    fn ensure_authority(&self) -> Result<()> {
        self.gateway.ensure_delivery(self.generation)
    }
    async fn dispatch(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let result = self
            .gateway
            .dispatch(&self.service, ticket, invocation)
            .await;
        if let Err(error) = &result {
            tracing::warn!(actor = %ticket.actor.storage_key().as_str(), error = %format!("{error:#}"), "socket event delivery failed");
        }
        result
    }
    fn notify(&self, ticket: &SocketTicket, event: &crate::actor::ActorSocketEvent) {
        self.service
            .deliver_socket_message_event(&ticket.actor, None, event);
    }
}
