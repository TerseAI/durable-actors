use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use moka::future::Cache;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    actor::{ActorKey, ActorSocketEffect, ActorSocketEvent, ActorSocketInvocation},
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest, HostSocketEventRequest,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
        actor_host_service_client::ActorHostServiceClient,
    },
    host::{ActorProcessRole, HostId},
    host_leases::{HostLease, HostLeaseStore},
    placement::{ObjectPlacement, ObjectPlacementStore},
    sandbox::{
        EnsureHostRequest, HostSandboxRuntimeConfig, HostTermination, ImageWarmup,
        ProviderCommandFailure, SandboxProvider, TerminateHostsRequest, WarmImageRequest,
    },
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminRegistry, AdminService, HostLaunchSpec},
    auth::{ActorJwtVerifier, ActorPrincipal},
    issuer::ActorJwtIssuer,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
};

const FALLBACK_REGION: &str = "north-america-central";

#[derive(Clone)]
pub struct ControlPlaneService {
    pub(super) sockets: super::websocket::SocketRegistry,
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    leases: Arc<dyn HostLeaseStore>,
    placements: Arc<dyn ObjectPlacementStore>,
    auth: ActorJwtVerifier,
    host_token_issuer: ActorJwtIssuer,
    registry: Arc<dyn AdminRegistry>,
    provisioner: Arc<dyn HostProvisioner>,
    socket_targets: Cache<(ActorKey, HostId, String, i64), Arc<WorkflowActorTarget>>,
    host_channels: Cache<String, Channel>,
    socket_events: Option<Arc<dyn super::event_sink::SocketMessageEventSink>>,
}

impl ControlPlaneService {
    pub(crate) fn with_runtime_access(
        mut self,
        access: Arc<crate::bucket::access::RuntimeAccess>,
    ) -> Self {
        self.runtime_access = Some(access);
        self
    }

    pub(crate) fn new(
        leases: Arc<dyn HostLeaseStore>,
        placements: Arc<dyn ObjectPlacementStore>,
        auth: ActorJwtVerifier,
        registry: Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
        provisioner: Arc<dyn HostProvisioner>,
    ) -> Self {
        Self {
            runtime_access: None,
            sockets: super::websocket::SocketRegistry::default(),
            leases,
            placements,
            auth,
            host_token_issuer: issuer,
            registry,
            provisioner,
            socket_targets: Cache::builder()
                .max_capacity(4096)
                .time_to_live(Duration::from_secs(55))
                .build(),
            host_channels: Cache::builder()
                .max_capacity(256)
                .time_to_idle(Duration::from_secs(300))
                .build(),
            socket_events: None,
        }
    }

    pub(crate) fn with_socket_event_sink(
        mut self,
        sink: Option<Arc<dyn super::event_sink::SocketMessageEventSink>>,
    ) -> Self {
        self.socket_events = sink;
        self
    }

    pub fn into_internal_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
    }

    pub(super) fn authenticate_workflow(&self, authorization: &str) -> Result<ActorPrincipal> {
        self.auth.authenticate_authorization(authorization)
    }

    pub(super) fn authenticate_application(
        &self,
        admin: &AdminService,
        authorization: &str,
        namespace_id: Option<&str>,
    ) -> Result<ActorPrincipal> {
        if admin.authenticate(authorization).is_err() {
            return self.authenticate_workflow(authorization);
        }
        Ok(ActorPrincipal::for_application(
            namespace_id.unwrap_or(&admin.default_namespace),
            FALLBACK_REGION.into(),
            i64::MAX,
        ))
    }

    pub(super) async fn runtime_deployment(
        &self,
        namespace_id: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        self.registry.launch_spec(namespace_id).await
    }

    pub(super) async fn register_deployment(
        &self,
        admin: &AdminService,
        spec: &HostLaunchSpec,
        contract: Option<&super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        let previous = admin.current_deployment(&spec.namespace_id).await?;
        spec.validate()?;
        admin.validate_contract_registration(spec, contract).await?;
        if let Some(previous) = previous
            && previous != *spec
        {
            self.terminate_deployment_hosts(&previous).await?;
        }
        let changed = admin.register_deployment(spec, contract).await?;
        self.sockets.changes.notify(&spec.namespace_id);
        Ok(changed)
    }

    pub(super) async fn delete_deployment(
        &self,
        admin: &AdminService,
        namespace_id: &str,
    ) -> Result<bool> {
        let Some(previous) = admin.current_deployment(namespace_id).await? else {
            return Ok(false);
        };
        self.terminate_deployment_hosts(&previous).await?;
        admin.remove_deployment(namespace_id).await?;
        Ok(true)
    }

    pub(super) fn warm_deployment_image(&self, spec: HostLaunchSpec, region: String) {
        let provisioner = self.provisioner.clone();
        if super::regions::storage_region(&region).is_err() {
            warn!(
                event = "actor_image_warmup",
                namespace_id = %spec.namespace_id,
                code_revision = %spec.code_revision,
                region,
                outcome = "invalid_region",
                "actor image warmup skipped"
            );
            return;
        }
        tokio::spawn(async move {
            let started_at = Instant::now();
            match provisioner.warm_image(&spec, &region).await {
                Ok(warmup) => info!(
                    event = "actor_image_warmup",
                    namespace_id = %spec.namespace_id,
                    code_revision = %spec.code_revision,
                    region,
                    provider = %warmup.provider,
                    provider_resource_id = %warmup.resource_id,
                    provider_total_ms = warmup.total_ms,
                    total_ms = elapsed_ms(started_at),
                    outcome = "warmed",
                    "actor image warmup completed"
                ),
                Err(error) => warn!(
                    event = "actor_image_warmup",
                    namespace_id = %spec.namespace_id,
                    code_revision = %spec.code_revision,
                    region,
                    total_ms = elapsed_ms(started_at),
                    outcome = "failed",
                    error = %format!("{error:#}"),
                    "actor image warmup failed"
                ),
            }
        });
    }

    async fn terminate_deployment_hosts(&self, spec: &HostLaunchSpec) -> Result<()> {
        let started_at = Instant::now();
        match self
            .provisioner
            .terminate_hosts(
                spec,
                &super::regions::ALL
                    .iter()
                    .map(|r| (*r).into())
                    .collect::<Vec<_>>(),
            )
            .await
        {
            Ok(termination) => info!(
                event = "actor_hosts_terminated",
                namespace_id = %spec.namespace_id,
                code_revision = %spec.code_revision,
                provider = %termination.provider,
                resource_count = termination.resource_ids.len(),
                total_ms = elapsed_ms(started_at),
                outcome = "terminated",
                "replaced deployment hosts terminated"
            ),
            Err(error) => {
                return Err(error.context(
                    "previous actor hosts could not be terminated; retry the deployment update",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn resolve_workflow_target_timed(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
        timings: &mut TargetResolutionTimings,
    ) -> Result<WorkflowActorTarget> {
        self.resolve_workflow_route(principal, actor, Some(timings))
            .await
    }

    pub(super) async fn dispatch_socket_event(
        &self,
        principal: &ActorPrincipal,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let key = (
            invocation.actor.clone(),
            principal.host_id.clone(),
            principal.session_id.clone(),
            principal.expires_at,
        );
        for attempt in 0..2 {
            let target = self.socket_target(principal, &invocation.actor).await?;
            match self.send_socket_event(&target, &invocation).await {
                Ok(crate::actor::ActorExecutionResult::Completed { effects, .. }) => {
                    return Ok(effects);
                }
                Ok(
                    crate::actor::ActorExecutionResult::Reroute
                    | crate::actor::ActorExecutionResult::HostUnavailable,
                ) => {
                    self.socket_targets.invalidate(&key).await;
                    if attempt == 1 {
                        anyhow::bail!("actor host remained unavailable after socket reroute");
                    }
                }
                Ok(crate::actor::ActorExecutionResult::Failed { failure }) => {
                    anyhow::bail!("{}: {}", failure.code, failure.message)
                }
                Err(error) => {
                    self.socket_targets.invalidate(&key).await;
                    self.host_channels.invalidate(&target.route).await;
                    return Err(error);
                }
            }
        }
        unreachable!("socket dispatch returns after its final attempt")
    }

    async fn send_socket_event(
        &self,
        target: &WorkflowActorTarget,
        invocation: &ActorSocketInvocation,
    ) -> Result<crate::actor::ActorExecutionResult> {
        let channel = self
            .host_channels
            .try_get_with(target.route.clone(), async {
                Endpoint::new(target.route.clone())?
                    .connect_timeout(Duration::from_secs(5))
                    .timeout(Duration::from_secs(24 * 60 * 60))
                    .connect()
                    .await
                    .context("connect to actor host")
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))?;
        let mut client = ActorHostServiceClient::new(channel)
            .max_decoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
            .max_encoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES);
        let mut request = Request::new(HostSocketEventRequest {
            request_id: invocation.request_id.clone(),
            actor: Some(invocation.actor.clone().into()),
            event_json: serde_json::to_vec(&invocation.event)?,
            connections_json: serde_json::to_vec(&invocation.connections)?,
            owner_epoch: target.owner_epoch,
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {}", target.token).parse()?);
        let reply = client.handle_socket(request).await?.into_inner();
        crate::actor::ActorExecutionResult::try_from(reply)
    }

    async fn socket_target(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<Arc<WorkflowActorTarget>> {
        actor.validate()?;
        ensure!(
            principal.process_role == ActorProcessRole::Workflow && principal.scope.contains(actor),
            "workflow token cannot resolve this actor"
        );
        let now = unix_seconds()?;
        ensure!(principal.expires_at > now, "workflow token has expired");
        let key = (
            actor.clone(),
            principal.host_id.clone(),
            principal.session_id.clone(),
            principal.expires_at,
        );
        if let Some(target) = self.socket_targets.get(&key).await {
            if target.expires_at_ms > (now + 5) * 1000 {
                return Ok(target);
            }
            self.socket_targets.invalidate(&key).await;
        }
        self.socket_targets
            .try_get_with(key, async {
                self.resolve_workflow_route(principal, actor, None)
                    .await
                    .map(Arc::new)
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }

    pub(super) fn deliver_socket_message_event(
        &self,
        actor: &ActorKey,
        trigger_id: Option<String>,
        event: &ActorSocketEvent,
    ) {
        let ActorSocketEvent::Message {
            connection_id,
            message,
        } = event
        else {
            return;
        };
        if let Some(sink) = self.socket_events.clone() {
            let event = super::event_sink::SocketMessageEvent::new(
                actor,
                trigger_id,
                connection_id,
                message,
            );
            tokio::spawn(async move {
                if let Err(error) = sink.deliver(event).await {
                    warn!(error = %format!("{error:#}"), "actor socket message trigger delivery failed");
                }
            });
        }
        info!(
            event = "actor_socket_message_committed",
            namespace_id = %actor.namespace_id,
            actor_type = %actor.actor_type,
            actor_id = %actor.actor_id,
            connection_id,
            message_kind = match message {
                crate::actor::ActorSocketMessage::Text { .. } => "text",
                crate::actor::ActorSocketMessage::Binary { .. } => "binary",
            },
            "actor socket message committed"
        );
    }

    async fn resolve_workflow_route(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
        mut timings: Option<&mut TargetResolutionTimings>,
    ) -> Result<WorkflowActorTarget> {
        actor.validate()?;
        ensure!(
            principal.process_role == ActorProcessRole::Workflow && principal.scope.contains(actor),
            "workflow token cannot resolve this actor"
        );
        let target = self
            .route_actor(actor, &principal.region, timings.as_deref_mut())
            .await?;
        let issued = self.host_token_issuer.issue_invocation_target(
            actor,
            &target.lease.id,
            &target.lease.session_id,
            &target.spec.host_revision(),
            &target.placement.home_region,
            target.placement.owner_epoch,
            principal.expires_at,
        )?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.invocation_token_issued_at_ms = Some(timings.elapsed_ms());
        }
        let route = target.lease.route;
        if let Some(timings) = timings {
            timings.route_selected_at_ms = Some(timings.elapsed_ms());
        }
        Ok(WorkflowActorTarget {
            route,
            token: issued.token,
            owner_epoch: target.placement.owner_epoch,
            expires_at_ms: issued.expires_at_ms,
        })
    }
}

pub(super) struct WorkflowActorTarget {
    pub route: String,
    pub token: String,
    pub owner_epoch: u64,
    pub expires_at_ms: i64,
}

pub(super) struct TargetResolutionTimings {
    started_at: Instant,
    pub request_validated_at_ms: Option<f64>,
    pub workflow_authenticated_at_ms: Option<f64>,
    pub deployment_loaded_at_ms: Option<f64>,
    pub placement_loaded_at_ms: Option<f64>,
    pub lease_checked_at_ms: Option<f64>,
    pub host_ensured_at_ms: Option<f64>,
    pub placement_claimed_at_ms: Option<f64>,
    pub invocation_token_issued_at_ms: Option<f64>,
    pub route_selected_at_ms: Option<f64>,
}

impl TargetResolutionTimings {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            request_validated_at_ms: None,
            workflow_authenticated_at_ms: None,
            deployment_loaded_at_ms: None,
            placement_loaded_at_ms: None,
            lease_checked_at_ms: None,
            host_ensured_at_ms: None,
            placement_claimed_at_ms: None,
            invocation_token_issued_at_ms: None,
            route_selected_at_ms: None,
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

#[tonic::async_trait]
impl ActorControlPlaneService for ControlPlaneService {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> std::result::Result<Response<ControlPlaneReply>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        let command = decode_command(request.into_inner())
            .map_err(|error| Status::invalid_argument(format!("invalid command: {error:#}")))?;
        let reply = self
            .execute_command(&principal, command)
            .await
            .map_err(failed_precondition)?;
        Ok(Response::new(encode_reply(reply).map_err(internal)?))
    }
}

impl ControlPlaneService {
    async fn execute_command(
        &self,
        principal: &ActorPrincipal,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandReply> {
        ensure!(
            principal.process_role == ActorProcessRole::Host,
            "only hosts may use the internal control-plane API"
        );
        match command {
            ControlPlaneCommand::InventoryChanged => {
                let status = self.leases.lease_status(&principal.host_id).await?;
                ensure!(
                    status
                        .lease
                        .is_some_and(|lease| lease.session_id == principal.session_id),
                    "host session does not match"
                );
                self.sockets.changes.notify(&principal.scope.namespace_id);
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::RefreshStorageAccess => {
                self.require_active_host(principal).await?;
                let token = self
                    .runtime_access
                    .as_ref()
                    .context("direct storage is not configured")?
                    .issue(&principal.scope.namespace_id)
                    .await?;
                let replacement_token = self
                    .host_token_issuer
                    .issue_host(
                        &principal.scope.namespace_id,
                        &principal.host_id,
                        &principal.session_id,
                        principal
                            .code_revision
                            .as_deref()
                            .context("host revision missing")?,
                        &principal.region,
                    )?
                    .token;
                Ok(ControlPlaneCommandReply::StorageAccess {
                    token,
                    replacement_token,
                })
            }
        }
    }

    pub(super) async fn publish_socket_effects(
        &self,
        principal: &ActorPrincipal,
        actor: ActorKey,
        effects: Vec<ActorSocketEffect>,
    ) -> Result<ControlPlaneCommandReply> {
        crate::actor::validate_socket_effects(&effects)?;
        self.authorize_socket_host(principal, &actor).await?;
        self.sockets.apply(&actor, effects).await;
        Ok(ControlPlaneCommandReply::Unit)
    }

    pub(super) async fn socket_connections(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<Vec<crate::actor::ActorSocketConnection>> {
        self.authorize_socket_host(principal, actor).await?;
        Ok(self.sockets.connections(actor).await)
    }

    async fn authorize_socket_host(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<()> {
        actor.validate()?;
        ensure!(
            principal.process_role == ActorProcessRole::Host,
            "socket access requires a host"
        );
        ensure!(
            principal.scope.contains(actor),
            "actor crossed the host namespace"
        );
        let lease = self.require_active_host(principal).await?;
        let placement = self.current_placement(actor).await?;
        ensure!(
            self.placements.matches_lease(&placement, &lease).await?,
            "actor ownership belongs to another session"
        );
        validate_state_owner(
            principal,
            &principal.host_id,
            placement.owner_epoch,
            &placement,
        )
    }

    async fn route_actor(
        &self,
        actor: &ActorKey,
        storage_region: &str,
        mut timings: Option<&mut TargetResolutionTimings>,
    ) -> Result<RoutedActor> {
        let spec = self
            .runtime_deployment(&actor.namespace_id)
            .await?
            .context("project has no registered actor code")?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.deployment_loaded_at_ms = Some(timings.elapsed_ms());
        }
        loop {
            let current = self.placements.get_owner(&actor.storage_key()).await?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.placement_loaded_at_ms = Some(timings.elapsed_ms());
            }
            let lease_checked = current.as_ref().is_some_and(|placement| {
                host_matches_revision(&placement.owner, &spec.namespace_id, &spec.host_revision())
            });
            let active = self.active_target(&current, &spec).await?;
            if lease_checked && let Some(timings) = timings.as_deref_mut() {
                timings.lease_checked_at_ms = Some(timings.elapsed_ms());
            }
            if let Some(target) = active {
                return Ok(target);
            }
            let (region, lease) = self
                .ensure_actor_host(&spec, current.as_ref(), storage_region)
                .await?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.host_ensured_at_ms = Some(timings.elapsed_ms());
            }
            let placement = self.activate_host(actor, &spec, &lease, &region).await?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.placement_claimed_at_ms = Some(timings.elapsed_ms());
            }
            return Ok(RoutedActor {
                placement,
                lease,
                spec,
            });
        }
    }

    async fn activate_host(
        &self,
        actor: &ActorKey,
        spec: &HostLaunchSpec,
        lease: &HostLease,
        region: &str,
    ) -> Result<ObjectPlacement> {
        let token = self.host_token_issuer.issue_host(
            &actor.namespace_id,
            &lease.id,
            &lease.session_id,
            &spec.host_revision(),
            region,
        )?;
        let channel = self
            .host_channels
            .try_get_with(lease.route.clone(), async {
                Endpoint::new(lease.route.clone())?
                    .connect_timeout(Duration::from_secs(5))
                    .connect()
                    .await
                    .context("connect to actor host")
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))?;
        let mut request = Request::new(crate::grpc::proto::ActivateActorRequest {
            actor: Some(actor.clone().into()),
        });
        request.set_timeout(super::CONTROL_PLANE_REQUEST_TIMEOUT);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {}", token.token).parse()?);
        let activated = ActorHostServiceClient::new(channel)
            .activate(request)
            .await?
            .into_inner();
        ensure!(
            activated.owner_epoch > 0,
            "host activation returned no ownership epoch"
        );
        Ok(ObjectPlacement {
            object: actor.storage_key(),
            owner: lease.id.clone(),
            owner_epoch: activated.owner_epoch,
            home_region: region.into(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        })
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        current: Option<&ObjectPlacement>,
        requested_region: &str,
    ) -> Result<(String, HostLease)> {
        let region = self.target_region(current, requested_region)?;
        match self.provisioner.ensure_host(spec, &region).await {
            Ok(lease) => Ok((region, lease)),
            Err(error) if current.is_none() && region != FALLBACK_REGION => {
                warn!(
                    event = "actor_region_fallback",
                    namespace_id = %spec.namespace_id,
                    requested_region,
                    region,
                    fallback_region = FALLBACK_REGION,
                    error = %format!("{error:#}"),
                    "actor host provisioning failed; trying fallback for new actor"
                );
                let lease = self.provisioner.ensure_host(spec, FALLBACK_REGION).await
                    .with_context(|| format!("host provisioning failed in {region}: {error:#}; fallback {FALLBACK_REGION} also failed"))?;
                Ok((FALLBACK_REGION.to_owned(), lease))
            }
            Err(error) => Err(error),
        }
    }

    async fn require_active_host(&self, principal: &ActorPrincipal) -> Result<HostLease> {
        let status = self.leases.lease_status(&principal.host_id).await?;
        ensure!(status.is_active(), "host lease is not active");
        let lease = status.lease.context("active host lease is missing")?;
        ensure!(
            lease.session_id == principal.session_id,
            "host lease belongs to another session"
        );
        Ok(lease)
    }

    async fn current_placement(&self, actor: &ActorKey) -> Result<ObjectPlacement> {
        self.placements
            .get_owner(&actor.storage_key())
            .await?
            .context("actor has no current placement")
    }

    async fn active_target(
        &self,
        current: &Option<ObjectPlacement>,
        spec: &HostLaunchSpec,
    ) -> Result<Option<RoutedActor>> {
        let Some(placement) = current else {
            return Ok(None);
        };
        if !host_matches_revision(&placement.owner, &spec.namespace_id, &spec.host_revision()) {
            return Ok(None);
        }
        let status = self.leases.lease_status(&placement.owner).await?;
        if !status.is_active() {
            return Ok(None);
        }
        if !self
            .placements
            .matches_lease(
                placement,
                status.lease.as_ref().context("active lease is missing")?,
            )
            .await?
        {
            return Ok(None);
        }
        Ok(Some(RoutedActor {
            placement: placement.clone(),
            lease: status.lease.context("active lease is missing")?,
            spec: spec.clone(),
        }))
    }

    fn target_region(
        &self,
        current: Option<&ObjectPlacement>,
        storage_region: &str,
    ) -> Result<String> {
        select_target_region(current, storage_region)
    }
}

fn select_target_region(current: Option<&ObjectPlacement>, requested: &str) -> Result<String> {
    if let Some(placement) = current {
        return Ok(placement.home_region.clone());
    }
    let region = super::regions::storage_region(requested).unwrap_or(FALLBACK_REGION);
    crate::placement::validate_region(&region)?;
    Ok(region.into())
}

#[async_trait]
pub(crate) trait HostProvisioner: Send + Sync {
    async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease>;
    async fn warm_image(&self, spec: &HostLaunchSpec, region: &str) -> Result<ImageWarmup>;
    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination>;
}

pub(crate) struct SandboxHostProvisioner {
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    provider: Arc<dyn SandboxProvider>,
    runtime: HostSandboxRuntimeConfig,
    issuer: ActorJwtIssuer,
    leases: Arc<dyn HostLeaseStore>,
}

impl SandboxHostProvisioner {
    pub(crate) fn with_runtime_access(
        mut self,
        access: Arc<crate::bucket::access::RuntimeAccess>,
    ) -> Self {
        self.runtime_access = Some(access);
        self
    }
    pub(crate) fn new(
        provider: Arc<dyn SandboxProvider>,
        runtime: HostSandboxRuntimeConfig,
        issuer: ActorJwtIssuer,
        leases: Arc<dyn HostLeaseStore>,
    ) -> Self {
        Self {
            runtime_access: None,
            provider,
            runtime,
            issuer,
            leases,
        }
    }
}

#[async_trait]
impl HostProvisioner for SandboxHostProvisioner {
    async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease> {
        let started_at = Instant::now();
        let mut request = self.request(spec, region)?;
        if let Some(access) = &self.runtime_access {
            request.runtime_config = Some(access.bootstrap(&spec.namespace_id, region).await?);
        }
        let handle = match self.provider.ensure_host(&request).await {
            Ok(handle) => handle,
            Err(error) => {
                let command = error.downcast_ref::<ProviderCommandFailure>();
                warn!(
                    event = "actor_host_provisioning",
                    namespace_id = %spec.namespace_id,
                    code_revision = %spec.code_revision,
                    region,
                    host_id = %request.host_id,
                    started_at_ms = 0,
                    provider_process_spawned_at_ms = command.and_then(ProviderCommandFailure::spawned_at_ms),
                    provider_request_written_at_ms = command.and_then(ProviderCommandFailure::request_written_at_ms),
                    provider_process_completed_at_ms = command.and_then(ProviderCommandFailure::process_completed_at_ms),
                    provider_response_decoded_at_ms = command.and_then(ProviderCommandFailure::response_decoded_at_ms),
                    completed_at_ms = elapsed_ms(started_at),
                    outcome = "provider_failed",
                    error = %format!("{error:#}"),
                    "actor host provisioning failed"
                );
                return Err(error);
            }
        };
        let provisioning = handle.provisioning.clone();
        let lease = self.active_lease(handle, region).await?;
        let lease_validated_at_ms = elapsed_ms(started_at);
        info!(
            event = "actor_host_provisioning",
            namespace_id = %spec.namespace_id,
            code_revision = %spec.code_revision,
            region,
            host_id = %lease.id,
            provider = provisioning.as_ref().map(|value| value.provider.as_str()).unwrap_or("unknown"),
            provider_resource_id = provisioning.as_ref().map(|value| value.resource_id.as_str()).unwrap_or(""),
            provider_reused = provisioning.as_ref().is_some_and(|value| value.reused),
            started_at_ms = 0,
            provider_process_spawned_at_ms = provisioning.as_ref().and_then(|value| value.command_spawned_at_ms),
            provider_request_written_at_ms = provisioning.as_ref().and_then(|value| value.request_written_at_ms),
            provider_process_completed_at_ms = provisioning.as_ref().and_then(|value| value.process_completed_at_ms),
            provider_response_decoded_at_ms = provisioning.as_ref().and_then(|value| value.response_decoded_at_ms),
            modal_provider_started_at_ms = provisioning.as_ref().map(|value| value.started_at_ms),
            modal_input_parsed_at_ms = provisioning.as_ref().and_then(|value| value.input_parsed_at_ms),
            modal_sdk_loaded_at_ms = provisioning.as_ref().and_then(|value| value.sdk_loaded_at_ms),
            modal_resources_resolved_at_ms = provisioning.as_ref().and_then(|value| value.resources_resolved_at_ms),
            modal_existing_host_checked_at_ms = provisioning.as_ref().and_then(|value| value.existing_host_checked_at_ms),
            modal_sandbox_scheduled_at_ms = provisioning.as_ref().and_then(|value| value.sandbox_scheduled_at_ms),
            modal_host_ready_observed_at_ms = provisioning.as_ref().and_then(|value| value.host_ready_observed_at_ms),
            modal_route_read_at_ms = provisioning.as_ref().and_then(|value| value.route_read_at_ms),
            modal_metadata_written_at_ms = provisioning.as_ref().and_then(|value| value.metadata_written_at_ms),
            modal_provider_completed_at_ms = provisioning.as_ref().map(|value| value.completed_at_ms),
            lease_validated_at_ms,
            completed_at_ms = elapsed_ms(started_at),
            outcome = "ready",
            "actor host provisioning completed"
        );
        Ok(lease)
    }

    async fn warm_image(&self, spec: &HostLaunchSpec, region: &str) -> Result<ImageWarmup> {
        self.provider
            .warm_image(&WarmImageRequest {
                namespace_id: spec.namespace_id.clone(),
                code_revision: spec.code_revision.clone(),
                canonical_region: region.to_owned(),
                image_ref: spec.image_ref.clone(),
            })
            .await
    }

    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination> {
        self.provider
            .terminate_hosts(&TerminateHostsRequest {
                namespace_id: spec.namespace_id.clone(),
                code_revision: spec.host_revision(),
                canonical_regions: regions.to_vec(),
            })
            .await
    }
}

impl SandboxHostProvisioner {
    fn request(&self, spec: &HostLaunchSpec, region: &str) -> Result<EnsureHostRequest> {
        let revision = spec.host_revision();
        let host_id = HostId::new(format!(
            "host.v2.{}:{}.{}",
            spec.namespace_id,
            revision,
            uuid::Uuid::new_v4()
        ));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = self
            .issuer
            .issue_host(&spec.namespace_id, &host_id, &session_id, &revision, region)?
            .token;
        Ok(EnsureHostRequest {
            runtime_config: None,
            namespace_id: spec.namespace_id.clone(),
            code_revision: revision,
            canonical_region: region.to_owned(),
            host_id,
            session_id,
            host_token,
            jwt_public_keys: self.issuer.verifier_keys_json()?,
            control_plane_url: self.runtime.control_plane_url.clone(),
            jwt_issuer: self.runtime.jwt_issuer.clone(),
            invocation_jwt_audience: self.runtime.invocation_jwt_audience.clone(),
            image_ref: spec.image_ref.clone(),
            working_directory: spec.working_directory.clone(),
            actor_entrypoint: spec.actor_entrypoint.clone(),
            secret_refs: spec.secret_refs.clone(),
            socket_gateway_url: spec
                .socket_gateway_url
                .clone()
                .unwrap_or_else(|| self.runtime.control_plane_url.clone()),
            actor_idle_timeout_ms: self.runtime.actor_idle_timeout_ms,
            host_idle_timeout_ms: self.runtime.host_idle_timeout_ms,
        })
    }

    async fn active_lease(
        &self,
        handle: crate::sandbox::ActorHostHandle,
        region: &str,
    ) -> Result<HostLease> {
        ensure!(
            handle.canonical_region == region,
            "sandbox provider returned the wrong region"
        );
        validate_host_route(&handle.route)?;
        let status = self.leases.lease_status(&handle.host_id).await?;
        ensure!(status.is_active(), "sandbox host lease is not active");
        let lease = status.lease.context("active sandbox lease is missing")?;
        ensure!(
            lease.route == handle.route,
            "sandbox route does not match its lease"
        );
        Ok(lease)
    }
}

struct RoutedActor {
    placement: ObjectPlacement,
    lease: HostLease,
    spec: HostLaunchSpec,
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn validate_state_owner(
    principal: &ActorPrincipal,
    host_id: &HostId,
    owner_epoch: u64,
    placement: &ObjectPlacement,
) -> Result<()> {
    ensure!(placement.owner == *host_id, "host does not own this actor");
    ensure!(
        placement.owner_epoch == owner_epoch,
        "actor owner epoch is stale"
    );
    ensure!(
        placement.home_region == principal.region,
        "host is outside the actor home region"
    );
    Ok(())
}

fn host_matches_revision(host: &HostId, namespace: &str, revision: &str) -> bool {
    host.as_str()
        .starts_with(&format!("host.v2.{namespace}:{revision}."))
}

fn validate_host_route(route: &str) -> Result<()> {
    let route = reqwest::Url::parse(route)?;
    ensure!(
        matches!(route.scheme(), "http" | "https") && route.host_str().is_some(),
        "host route must be an HTTP origin"
    );
    ensure!(
        route.username().is_empty() && route.password().is_none(),
        "host route must not contain credentials"
    );
    ensure!(
        route.path() == "/" && route.query().is_none() && route.fragment().is_none(),
        "host route must not contain a path, query, or fragment"
    );
    Ok(())
}

fn unix_seconds() -> Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?)
}

fn failed_precondition(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex, time::Duration};

    use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
    use super::*;
    use crate::{
        actor::ActorScope,
        actor_state::ActorStorageKey,
        host_leases::{HostLeaseRegistry, HostLeaseRequest, HostLeaseStatus},
        placement::testing::LocalObjectPlacementStore,
    };
    use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
    use base64::{Engine, engine::general_purpose::STANDARD};

    pub(super) struct FakeLeaseStore {
        pub(super) leases: Mutex<HashMap<HostId, HostLease>>,
    }

    struct FakeSocketEventSink {
        delivered: tokio::sync::mpsc::UnboundedSender<serde_json::Value>,
    }

    #[async_trait]
    impl super::super::event_sink::SocketMessageEventSink for FakeSocketEventSink {
        async fn deliver(&self, event: super::super::event_sink::SocketMessageEvent) -> Result<()> {
            self.delivered.send(serde_json::to_value(event)?)?;
            Ok(())
        }
    }

    #[async_trait]
    impl HostLeaseRegistry for FakeLeaseStore {
        async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
            let lease = HostLease {
                id: request.id.clone(),
                session_id: request.session_id.clone(),
                route: request.route.clone(),
                expires_at_ms: 10_000,
            };
            self.leases
                .lock()
                .unwrap()
                .insert(lease.id.clone(), lease.clone());
            Ok(lease)
        }

        async fn unregister(&self, id: &HostId, _session_id: &str) -> Result<()> {
            self.leases.lock().unwrap().remove(id);
            Ok(())
        }
    }

    #[async_trait]
    impl HostLeaseStore for FakeLeaseStore {
        async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
            Ok(HostLeaseStatus {
                lease: self.leases.lock().unwrap().get(id).cloned(),
                store_now_ms: 0,
            })
        }
    }

    pub(super) struct FakeWarmProvisioner {
        pub(super) warmed: tokio::sync::mpsc::UnboundedSender<(HostLaunchSpec, String)>,
    }

    #[async_trait]
    impl HostProvisioner for FakeWarmProvisioner {
        async fn ensure_host(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<HostLease> {
            anyhow::bail!("host creation is outside this test")
        }

        async fn warm_image(&self, spec: &HostLaunchSpec, region: &str) -> Result<ImageWarmup> {
            self.warmed.send((spec.clone(), region.to_owned()))?;
            Ok(ImageWarmup {
                provider: "test".into(),
                resource_id: "sandbox-1".into(),
                total_ms: 1,
            })
        }

        async fn terminate_hosts(
            &self,
            _spec: &HostLaunchSpec,
            _regions: &[String],
        ) -> Result<HostTermination> {
            anyhow::bail!("host termination is outside this test")
        }
    }

    struct FakeRetiringProvisioner {
        retired: tokio::sync::mpsc::UnboundedSender<(HostLaunchSpec, Vec<String>)>,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl HostProvisioner for FakeRetiringProvisioner {
        async fn ensure_host(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<HostLease> {
            anyhow::bail!("host creation is outside this test")
        }

        async fn warm_image(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<ImageWarmup> {
            anyhow::bail!("image warmup is outside this test")
        }

        async fn terminate_hosts(
            &self,
            spec: &HostLaunchSpec,
            regions: &[String],
        ) -> Result<HostTermination> {
            self.retired.send((spec.clone(), regions.to_vec()))?;
            ensure!(
                !self.fail.load(std::sync::atomic::Ordering::Relaxed),
                "termination failed"
            );
            Ok(HostTermination {
                provider: "test".into(),
                resource_ids: vec!["sandbox-1".into()],
            })
        }
    }

    #[tokio::test]
    async fn gcs_routes_use_the_hosts_epoch_without_claiming_or_preparing_in_the_control_plane()
    -> Result<()> {
        use crate::grpc::proto::{
            self,
            actor_host_service_server::{ActorHostService, ActorHostServiceServer},
        };
        use tokio_stream::wrappers::TcpListenerStream;

        struct Host {
            auth: ActorJwtVerifier,
        }
        #[tonic::async_trait]
        impl ActorHostService for Host {
            async fn activate(
                &self,
                request: Request<proto::ActivateActorRequest>,
            ) -> Result<Response<proto::ActivateActorReply>, Status> {
                let principal = self.auth.authenticate(&request).await?;
                assert!(principal.invocation.is_none());
                assert_eq!(principal.scope.namespace_id, "project-1");
                assert_eq!(request.get_ref().actor.as_ref().unwrap().actor_id, "one");
                Ok(Response::new(proto::ActivateActorReply { owner_epoch: 42 }))
            }
            async fn invoke(
                &self,
                _: Request<proto::HostInvokeActorRequest>,
            ) -> Result<Response<proto::InvokeActorReply>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn handle_socket(
                &self,
                _: Request<proto::HostSocketEventRequest>,
            ) -> Result<Response<proto::InvokeActorReply>, Status> {
                Err(Status::unimplemented("unused"))
            }
        }
        struct Provisioner(HostLease);
        #[async_trait]
        impl HostProvisioner for Provisioner {
            async fn ensure_host(&self, _: &HostLaunchSpec, _: &str) -> Result<HostLease> {
                Ok(self.0.clone())
            }
            async fn warm_image(&self, _: &HostLaunchSpec, _: &str) -> Result<ImageWarmup> {
                anyhow::bail!("unused")
            }
            async fn terminate_hosts(
                &self,
                _: &HostLaunchSpec,
                _: &[String],
            ) -> Result<HostTermination> {
                anyhow::bail!("unused")
            }
        }
        let issuer = test_issuer()?;
        let invocation_auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let route = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ActorHostServiceServer::new(Host {
                    auth: invocation_auth,
                }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .ensure_namespace_and_register_deployment(&HostLaunchSpec {
                namespace_id: "project-1".into(),
                code_revision: "revision".into(),
                image_ref: "image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
                socket_gateway_url: None,
            })
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            placements.clone(),
            ActorJwtVerifier::for_scope(
                issuer.verifier_keys_json()?,
                "issuer",
                "authority",
                ActorTokenPurpose::ControlPlane,
                Duration::from_secs(60),
            )?,
            registry,
            issuer.clone(),
            Arc::new(Provisioner(HostLease {
                id: HostId::new("host.v2.project-1:revision.host"),
                session_id: uuid::Uuid::new_v4().to_string(),
                route: route.clone(),
                expires_at_ms: u64::MAX,
            })),
        );
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        };
        let principal =
            ActorPrincipal::for_application("project-1", "us-east".into(), unix_seconds()? + 60);
        let target = service
            .resolve_workflow_route(&principal, &actor, None)
            .await?;
        assert_eq!(target.route, route);
        assert_eq!(target.owner_epoch, 42);
        assert!(placements.get(&actor.storage_key()).await?.is_none());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn replacing_a_deployment_terminates_the_previous_revision_hosts() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = super::super::admin::AdminService::new(
            "admin-token".into(),
            registry.clone(),
            issuer.clone(),
        )?;
        let (retired_tx, mut retired_rx) = tokio::sync::mpsc::unbounded_channel();
        let provisioner = Arc::new(FakeRetiringProvisioner {
            retired: retired_tx,
            fail: std::sync::atomic::AtomicBool::new(true),
        });
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            registry,
            issuer,
            provisioner.clone(),
        );
        let first = HostLaunchSpec {
            namespace_id: "project-1".into(),
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
            socket_gateway_url: None,
        };
        let mut replacement = first.clone();
        replacement.code_revision = "revision-2".into();
        replacement.image_ref = "image-2".into();

        assert!(service.register_deployment(&admin, &first, None).await?);
        assert!(retired_rx.try_recv().is_err());
        assert!(
            service
                .register_deployment(&admin, &replacement, None)
                .await
                .is_err()
        );
        assert_eq!(
            admin.current_deployment("project-1").await?,
            Some(first.clone())
        );
        assert_eq!(
            retired_rx.recv().await,
            Some((
                first.clone(),
                super::super::regions::ALL
                    .iter()
                    .map(|r| (*r).into())
                    .collect()
            ))
        );
        provisioner
            .fail
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(
            service
                .register_deployment(&admin, &replacement, None)
                .await?
        );
        assert_eq!(
            retired_rx.recv().await,
            Some((
                first,
                super::super::regions::ALL
                    .iter()
                    .map(|r| (*r).into())
                    .collect()
            ))
        );
        assert_eq!(
            admin.current_deployment("project-1").await?,
            Some(replacement.clone())
        );
        assert!(
            !service
                .register_deployment(&admin, &replacement, None)
                .await?
        );
        assert!(retired_rx.try_recv().is_err());
        let mut secret_update = replacement.clone();
        secret_update.secret_refs = vec!["project-secrets-updated".into()];
        assert!(
            service
                .register_deployment(&admin, &secret_update, None)
                .await?
        );
        assert_eq!(
            retired_rx.recv().await,
            Some((
                replacement,
                super::super::regions::ALL
                    .iter()
                    .map(|r| (*r).into())
                    .collect()
            ))
        );
        provisioner
            .fail
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            service
                .delete_deployment(&admin, "project-1")
                .await
                .is_err()
        );
        assert_eq!(
            admin.current_deployment("project-1").await?,
            Some(secret_update)
        );
        provisioner
            .fail
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(service.delete_deployment(&admin, "project-1").await?);
        assert_eq!(admin.current_deployment("project-1").await?, None);
        assert!(!service.delete_deployment(&admin, "project-1").await?);
        Ok(())
    }

    #[tokio::test]
    async fn deployment_image_warmup_runs_in_the_background_without_creating_an_actor() -> Result<()>
    {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::new()),
        });
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let registry = Arc::new(LocalAdminRegistry::default());
        let (warmed_tx, mut warmed_rx) = tokio::sync::mpsc::unbounded_channel();
        let service = ControlPlaneService::new(
            leases,
            placements,
            auth,
            registry,
            issuer,
            Arc::new(FakeWarmProvisioner { warmed: warmed_tx }),
        );
        let spec = HostLaunchSpec {
            namespace_id: "project-1".into(),
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
            socket_gateway_url: None,
        };

        service.warm_deployment_image(spec.clone(), "north-america-east".into());

        let warmed = tokio::time::timeout(Duration::from_secs(1), warmed_rx.recv())
            .await?
            .context("warmup task stopped")?;
        assert_eq!(warmed, (spec, "north-america-east".into()));
        Ok(())
    }

    #[tokio::test]
    async fn accepted_socket_messages_are_delivered_to_the_configured_event_sink() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            Arc::new(LocalAdminRegistry::default()),
            issuer,
            Arc::new(FakeWarmProvisioner {
                warmed: tokio::sync::mpsc::unbounded_channel().0,
            }),
        )
        .with_socket_event_sink(Some(Arc::new(FakeSocketEventSink {
            delivered: delivered_tx,
        })));
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "ChatRoom".into(),
            actor_id: "room-1".into(),
        };

        service.deliver_socket_message_event(
            &actor,
            Some("trigger-1".into()),
            &ActorSocketEvent::Message {
                connection_id: "socket-1".into(),
                message: crate::actor::ActorSocketMessage::Text {
                    data: "hello".into(),
                },
            },
        );

        let delivered = tokio::time::timeout(Duration::from_secs(1), delivered_rx.recv())
            .await?
            .context("socket event delivery task stopped")?;
        assert_eq!(delivered["namespaceId"], "project-1");
        assert_eq!(delivered["actorType"], "ChatRoom");
        assert_eq!(delivered["actorId"], "room-1");
        assert_eq!(delivered["triggerId"], "trigger-1");
        assert_eq!(delivered["connectionId"], "socket-1");
        assert_eq!(delivered["message"]["type"], "text");
        assert_eq!(delivered["message"]["data"], "hello");
        Ok(())
    }

    #[tokio::test]
    async fn socket_dispatch_reuses_a_connection_and_does_not_replay_transport_failures()
    -> Result<()> {
        use crate::grpc::proto::{
            self,
            actor_host_service_server::{ActorHostService, ActorHostServiceServer},
        };
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use tokio_stream::{StreamExt, wrappers::TcpListenerStream};

        struct Host {
            calls: Arc<AtomicUsize>,
            fail: Arc<AtomicBool>,
        }
        #[tonic::async_trait]
        impl ActorHostService for Host {
            async fn activate(
                &self,
                _: Request<proto::ActivateActorRequest>,
            ) -> Result<Response<proto::ActivateActorReply>, Status> {
                Err(Status::unimplemented("outside this test"))
            }

            async fn invoke(
                &self,
                _: Request<proto::HostInvokeActorRequest>,
            ) -> Result<tonic::Response<proto::InvokeActorReply>, Status> {
                Err(Status::unimplemented(
                    "method invocation is outside this test",
                ))
            }
            async fn handle_socket(
                &self,
                request: Request<HostSocketEventRequest>,
            ) -> Result<tonic::Response<proto::InvokeActorReply>, Status> {
                assert_eq!(
                    request.metadata().get("authorization").unwrap(),
                    "Bearer test-token"
                );
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.fail.load(Ordering::SeqCst) {
                    return Err(Status::unavailable("reply lost after execution"));
                }
                Ok(tonic::Response::new(
                    crate::actor::ActorExecutionResult::Completed {
                        result: serde_json::Value::Null,
                        effects: vec![],
                    }
                    .into(),
                ))
            }
        }
        let connections = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let route = format!("http://{}", listener.local_addr()?);
        let accepted = connections.clone();
        let incoming = TcpListenerStream::new(listener).map(move |stream| {
            if stream.is_ok() {
                accepted.fetch_add(1, Ordering::SeqCst);
            }
            stream
        });
        let stop = tokio_util::sync::CancellationToken::new();
        let shutdown = stop.clone();
        let host = Host {
            calls: calls.clone(),
            fail: fail.clone(),
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ActorHostServiceServer::new(host))
                .serve_with_incoming_shutdown(incoming, async move { shutdown.cancelled().await }),
        );
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            Arc::new(LocalAdminRegistry::default()),
            issuer,
            Arc::new(FakeWarmProvisioner {
                warmed: tokio::sync::mpsc::unbounded_channel().0,
            }),
        );
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        };
        let principal = ActorPrincipal {
            scope: ActorScope {
                namespace_id: actor.namespace_id.clone(),
            },
            host_id: HostId::new("workflow.v1.project-1.test"),
            session_id: uuid::Uuid::new_v4().to_string(),
            process_role: ActorProcessRole::Workflow,
            region: "us-east".into(),
            code_revision: None,
            expires_at: unix_seconds()? + 60,
            invocation: None,
        };
        let key = (
            actor.clone(),
            principal.host_id.clone(),
            principal.session_id.clone(),
            principal.expires_at,
        );
        service
            .socket_targets
            .insert(
                key.clone(),
                Arc::new(WorkflowActorTarget {
                    route: route.clone(),
                    token: "test-token".into(),
                    owner_epoch: 1,
                    expires_at_ms: (unix_seconds()? + 30) * 1000,
                }),
            )
            .await;
        let invocation = |id: &str| ActorSocketInvocation {
            request_id: id.into(),
            actor: actor.clone(),
            event: ActorSocketEvent::Message {
                connection_id: "one".into(),
                message: crate::actor::ActorSocketMessage::Text {
                    data: "hello".into(),
                },
            },
            connections: vec![],
        };
        service
            .dispatch_socket_event(&principal, invocation("first"))
            .await?;
        service
            .dispatch_socket_event(&principal, invocation("second"))
            .await?;
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        fail.store(true, Ordering::SeqCst);
        assert!(
            service
                .dispatch_socket_event(&principal, invocation("third"))
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(service.socket_targets.get(&key).await.is_none());
        assert!(service.host_channels.get(&route).await.is_none());
        stop.cancel();
        server.await??;
        Ok(())
    }

    #[test]
    fn new_actors_use_the_workflow_region_and_existing_actors_stay_pinned() -> Result<()> {
        let actor = ActorStorageKey::new("object.v1.project.Counter.one");
        let current = ObjectPlacement {
            object: actor,
            owner: HostId::new("host.v2.project:revision.host"),
            owner_epoch: 1,
            home_region: "north-america-east".into(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        };

        assert_eq!(
            select_target_region(None, "north-america-central")?,
            "north-america-central"
        );
        assert_eq!(
            select_target_region(Some(&current), "north-america-west")?,
            "north-america-east"
        );
        for (reported, expected) in [
            ("us-east-1", "north-america-east"),
            ("us-west-2", "north-america-west"),
            ("us-central1", "north-america-central"),
            ("us-central1-a", "north-america-central"),
            ("us-ashburn-1", "north-america-east"),
            ("westus3", "north-america-west"),
        ] {
            assert_eq!(select_target_region(None, reported)?, expected);
            assert_eq!(
                select_target_region(Some(&current), reported)?,
                "north-america-east"
            );
        }
        for unsupported in ["", "unknown", "us-east-999", "us-central1-unknown"] {
            assert_eq!(
                select_target_region(None, unsupported)?,
                "north-america-central",
                "{unsupported}"
            );
            assert_eq!(
                select_target_region(Some(&current), unsupported)?,
                "north-america-east"
            );
        }
        Ok(())
    }

    #[test]
    fn execution_regions_do_not_require_separate_buckets() -> Result<()> {
        assert_eq!(
            select_target_region(None, "southcentralus")?,
            "north-america-south"
        );
        assert_eq!(select_target_region(None, "europe-west")?, "europe-west");
        Ok(())
    }

    struct FakeRoutingProvisioner {
        failed_regions: Vec<&'static str>,
        calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HostProvisioner for FakeRoutingProvisioner {
        async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease> {
            self.calls.lock().unwrap().push(region.to_owned());
            ensure!(
                !self.failed_regions.contains(&region),
                "host unavailable in {region}"
            );
            Ok(HostLease {
                id: HostId::new(format!(
                    "host.v2.{}:{}.{region}",
                    spec.namespace_id,
                    spec.host_revision()
                )),
                session_id: uuid::Uuid::new_v4().to_string(),
                route: "https://host.example.com".into(),
                expires_at_ms: u64::MAX,
            })
        }

        async fn warm_image(&self, _spec: &HostLaunchSpec, _region: &str) -> Result<ImageWarmup> {
            unreachable!()
        }

        async fn terminate_hosts(
            &self,
            _spec: &HostLaunchSpec,
            _regions: &[String],
        ) -> Result<HostTermination> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn provisioning_fallback_only_applies_before_initial_placement() -> Result<()> {
        let central = "north-america-central";
        let south = "north-america-south";
        for (failed_regions, existing, reported, expected_region, expected_calls) in [
            (vec![], false, "southcentralus", Some(south), vec![south]),
            (
                vec![],
                false,
                "eu-west-1",
                Some("europe-west"),
                vec!["europe-west"],
            ),
            (
                vec![],
                false,
                "unmapped-region",
                Some(central),
                vec![central],
            ),
            (
                vec![south],
                false,
                "southcentralus",
                Some(central),
                vec![south, central],
            ),
            (vec![south], true, "eastus", None, vec![south]),
            (
                vec![south, central],
                false,
                "southcentralus",
                None,
                vec![south, central],
            ),
            (vec![central], false, "unmapped-region", None, vec![central]),
        ] {
            let issuer = test_issuer()?;
            let auth = ActorJwtVerifier::for_scope(
                issuer.verifier_keys_json()?,
                "issuer",
                "invocation",
                ActorTokenPurpose::Invocation,
                Duration::from_secs(60),
            )?;
            let registry = Arc::new(LocalAdminRegistry::default());
            registry
                .ensure_namespace_and_register_deployment(&HostLaunchSpec {
                    namespace_id: "project".into(),
                    code_revision: "revision".into(),
                    image_ref: "image".into(),
                    working_directory: "/app".into(),
                    actor_entrypoint: None,
                    secret_refs: vec![],
                    socket_gateway_url: None,
                })
                .await?;
            let placements = Arc::new(LocalObjectPlacementStore::default());
            let actor = ActorKey {
                namespace_id: "project".into(),
                actor_type: "Counter".into(),
                actor_id: "one".into(),
            };
            if existing {
                placements
                    .claim(&actor.storage_key(), None, &HostId::new("old-host"), south)
                    .await?;
            }
            let before = placements.get(&actor.storage_key()).await?;
            let provisioner = Arc::new(FakeRoutingProvisioner {
                failed_regions,
                calls: Mutex::new(vec![]),
            });
            let service = ControlPlaneService::new(
                Arc::new(FakeLeaseStore {
                    leases: Mutex::new(HashMap::new()),
                }),
                placements.clone(),
                auth,
                registry.clone(),
                issuer,
                provisioner.clone(),
            );
            let spec = registry.launch_spec("project").await?.unwrap();
            let result = service
                .ensure_actor_host(&spec, before.as_ref(), reported)
                .await;
            assert_eq!(*provisioner.calls.lock().unwrap(), expected_calls);
            if let Some(region) = expected_region {
                let (selected, _) = result?;
                assert_eq!(selected, region);
                assert_eq!(placements.get(&actor.storage_key()).await?, before);
            } else {
                assert!(result.is_err());
                if expected_calls.len() == 2 {
                    let error = format!("{:#}", result.err().unwrap());
                    assert!(error.contains(south) && error.contains(central), "{error}");
                }
                assert_eq!(placements.get(&actor.storage_key()).await?, before);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn application_credentials_work_without_postgres() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .ensure_namespace_and_register_deployment(&HostLaunchSpec {
                namespace_id: "default".into(),
                code_revision: "v1".into(),
                image_ref: "image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
                socket_gateway_url: Some("https://gateway.example".into()),
            })
            .await?;
        let database = crate::postgres::PostgresDatabase::lazy(
            "postgresql://localhost:1/unavailable?sslmode=disable&connect_timeout=1",
        )?;
        let admin = AdminService::new(
            "api-key".into(),
            Arc::new(super::super::admin::PostgresAdminRegistry::from_database(
                database,
            )),
            issuer.clone(),
        )?;
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            registry,
            issuer.clone(),
            Arc::new(FakeRoutingProvisioner {
                failed_regions: vec![],
                calls: Mutex::new(vec![]),
            }),
        );
        let routes = super::super::public_api::router(service, admin);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let client = reqwest::Client::new();
        let requests = [
            (
                "actors/Room/lobby/socket-ticket",
                serde_json::json!({"metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000}),
            ),
            (
                "session-scoped-token",
                serde_json::json!({"executionId":"run","storageRegion":"north-america-east","deadlineUnixMs":(unix_seconds()? + 30) * 1000}),
            ),
        ];
        for (path, body) in requests {
            let response = client
                .post(format!("{origin}/v1/{path}"))
                .bearer_auth("api-key")
                .json(&body)
                .send()
                .await?;
            let status = response.status();
            let issued: serde_json::Value = response.json().await?;
            assert!(status.is_success(), "{path}: {status} {issued}");
            if path.ends_with("socket-ticket") {
                let mut url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
                assert_eq!(
                    url.query_pairs().find(|(name, _)| name == "key").unwrap().1,
                    issued["key"].as_str().unwrap()
                );
                url.set_query(None);
                assert_eq!(url.as_str(), "wss://gateway.example/v1/socket");
                let ticket = issuer.verify_socket(issued["key"].as_str().unwrap())?;
                assert_eq!(ticket.actor.namespace_id, "default");
                assert_eq!(ticket.metadata, body["metadata"]);
            } else {
                assert!(issued["token"].is_string());
            }
            assert!(
                !client
                    .post(format!("{origin}/v1/namespaces/missing/{path}"))
                    .bearer_auth("api-key")
                    .json(&body)
                    .send()
                    .await?
                    .status()
                    .is_success()
            );
        }
        assert_eq!(
            client
                .get(format!("{origin}/v1/deployment"))
                .bearer_auth("api-key")
                .send()
                .await?
                .status(),
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn socket_ticket_issuance_requires_api_key_and_cannot_delegate_backend_access()
    -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            registry,
            issuer.clone(),
            Arc::new(FakeRoutingProvisioner {
                failed_regions: vec![],
                calls: Mutex::new(vec![]),
            }),
        );
        let routes = super::super::public_api::router(service, admin);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let client = reqwest::Client::new();
        let url = format!("{origin}/v1/actors/Room/lobby/socket-ticket");
        let body =
            serde_json::json!({"metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000});
        let workflow = issuer
            .issue_workflow(
                "default",
                "run",
                "north-america-east",
                (unix_seconds()? + 30) * 1000,
            )?
            .token;
        for credential in ["", "wrong", &workflow] {
            assert_eq!(
                client
                    .post(&url)
                    .bearer_auth(credential)
                    .json(&body)
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }
        client.put(format!("{origin}/v1/deployment")).bearer_auth("api-key")
            .json(&serde_json::json!({"codeRevision":"v1","imageRef":"image","workingDirectory":"/app","socketGatewayUrl":"https://gateway.example"}))
            .send().await?.error_for_status()?;
        let issued = client
            .post(&url)
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(issued.headers().get("cache-control").unwrap(), "no-store");
        let issued: serde_json::Value = issued.json().await?;
        let key = issued["key"].as_str().unwrap();
        let socket_url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
        assert_eq!(socket_url.scheme(), "wss");
        assert_eq!(socket_url.host_str(), Some("gateway.example"));
        assert_eq!(socket_url.path(), "/v1/socket");
        assert_eq!(
            socket_url.query_pairs().collect::<Vec<_>>(),
            vec![("key".into(), key.into())]
        );
        assert_ne!(key, "api-key");
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(key)
                .json(&body)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .post(format!("{origin}/v1/actors/Room/lobby/target"))
                .bearer_auth(key)
                .json(&serde_json::json!({}))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn api_key_access_defaults_to_one_application_and_preserves_delegation() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::new()),
        });
        let placements = Arc::new(LocalObjectPlacementStore::default());
        for namespace in ["default", "customer"] {
            let host = HostId::new(format!("host.v2.{namespace}:revision.test"));
            leases
                .register(&HostLeaseRequest {
                    id: host.clone(),
                    session_id: "00000000-0000-4000-8000-000000000001".into(),
                    route: "https://host.example.com".into(),
                    duration_ms: 60_000,
                })
                .await?;
            let actor = ActorKey {
                namespace_id: namespace.into(),
                actor_type: "Counter".into(),
                actor_id: "one".into(),
            };
            placements
                .claim(&actor.storage_key(), None, &host, "north-america-east")
                .await?;
        }
        let service = ControlPlaneService::new(
            leases,
            placements,
            auth,
            registry,
            issuer.clone(),
            Arc::new(FakeRoutingProvisioner {
                failed_regions: vec![],
                calls: Mutex::new(vec![]),
            }),
        );
        let routes = super::super::public_api::router(service, admin);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let client = reqwest::Client::new();
        let deployment = serde_json::json!({ "codeRevision": "revision", "imageRef": "image", "workingDirectory": "/app" });
        let registered = client
            .put(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?;
        assert_eq!(registered.status(), reqwest::StatusCode::OK);
        for suffix in ["target", "socket-effects"] {
            let url = format!("{origin}/v1/actors/Counter/one/{suffix}");
            for key in ["", "wrong-key"] {
                assert_eq!(
                    client
                        .post(&url)
                        .bearer_auth(key)
                        .json(&serde_json::json!({"effects":[]}))
                        .send()
                        .await?
                        .status(),
                    reqwest::StatusCode::UNAUTHORIZED
                );
            }
        }
        let target: serde_json::Value = client
            .post(format!("{origin}/v1/actors/Counter/one/target"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(target["namespaceId"], "default");
        assert_ne!(target["token"], "api-key");
        assert_eq!(
            client
                .post(format!("{origin}/v1/actors/Counter/one/socket-effects"))
                .bearer_auth("api-key")
                .json(&serde_json::json!({"effects":[]}))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NO_CONTENT
        );
        client
            .put(format!("{origin}/v1/namespaces/customer/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?
            .error_for_status()?;
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64
            + 30_000;
        let token = issuer
            .issue_workflow("customer", "execution", "north-america-east", deadline)?
            .token;
        let delegated: serde_json::Value = client
            .post(format!("{origin}/v1/actors/Counter/one/target"))
            .bearer_auth(&token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(delegated["namespaceId"], "customer");
        assert_eq!(
            client
                .post(format!(
                    "{origin}/v1/namespaces/default/actors/Counter/one/target"
                ))
                .bearer_auth(&token)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
        assert_eq!(
            client
                .get(format!("{origin}/v1/deployment"))
                .bearer_auth(&token)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn session_scoped_token_endpoint_requires_the_api_key() -> Result<()> {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
        admin
            .ensure_namespace_and_register_deployment(&HostLaunchSpec {
                namespace_id: "project-1".into(),
                code_revision: "revision-1".into(),
                image_ref: "image-1".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
                socket_gateway_url: None,
            })
            .await?;
        let (retired, _retired_rx) = tokio::sync::mpsc::unbounded_channel();
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            registry,
            issuer,
            Arc::new(FakeRetiringProvisioner {
                retired,
                fail: std::sync::atomic::AtomicBool::new(false),
            }),
        );
        let routes = super::super::public_api::router(service, admin);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let client = reqwest::Client::new();
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            + 30_000;
        let body = serde_json::json!({ "executionId": "run-1", "storageRegion": "us-east", "deadlineUnixMs": deadline as i64 });
        let url = format!("{origin}/v1/namespaces/project-1/session-scoped-token");
        for credential in ["", "wrong-key"] {
            let response = client
                .post(&url)
                .bearer_auth(credential)
                .json(&body)
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        }
        let response = client
            .post(&url)
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let reply: serde_json::Value = response.json().await?;
        assert!(!reply["token"].as_str().unwrap_or_default().is_empty());
        assert!(reply["expiresAtMs"].as_i64().unwrap_or_default() > 0);
        let old = client
            .post(format!("{origin}/v1/namespaces/project-1/workflow-tokens"))
            .json(&body)
            .send()
            .await?;
        assert_eq!(old.status(), reqwest::StatusCode::NOT_FOUND);
        let deployment_url = format!("{origin}/v1/namespaces/project-1/deployment");
        for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
            assert_eq!(
                client
                    .request(method, &deployment_url)
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }
        let deployment: serde_json::Value = client
            .get(&deployment_url)
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(deployment["codeRevision"], "revision-1");
        assert_eq!(deployment["secretRefs"], serde_json::json!([]));
        for changed in [true, false] {
            let reply: serde_json::Value = client
                .delete(&deployment_url)
                .bearer_auth("api-key")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(reply["changed"], changed);
        }
        let deployment: serde_json::Value = client
            .get(&deployment_url)
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert!(deployment.is_null());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn contract_api_publishes_with_deployments_and_reads_only_the_active_revision()
    -> Result<()> {
        // Set up an HTTP server with an in-memory registry and fake infrastructure.
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
        // This execution credential should be rejected by the admin-only contract endpoint.
        let session_token = admin
            .issue_workflow_token(
                "default",
                "run",
                "us-east",
                i64::try_from(crate::clock::Clock::now_ms(&crate::clock::SystemClock)?)? + 30_000,
            )?
            .token;
        let (retired, _retired_rx) = tokio::sync::mpsc::unbounded_channel();
        let service = ControlPlaneService::new(
            Arc::new(FakeLeaseStore {
                leases: Mutex::new(HashMap::new()),
            }),
            Arc::new(LocalObjectPlacementStore::default()),
            auth,
            registry,
            issuer,
            Arc::new(FakeRetiringProvisioner {
                retired,
                fail: std::sync::atomic::AtomicBool::new(false),
            }),
        );
        let routes = super::super::public_api::router(service, admin);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let client = reqwest::Client::new();
        // 1. Both namespace routes reject invalid or non-admin credentials.
        for path in ["/v1/contract", "/v1/namespaces/team/contract"] {
            for credential in ["", "wrong", &session_token] {
                assert_eq!(
                    client
                        .get(format!("{origin}{path}"))
                        .bearer_auth(credential)
                        .send()
                        .await?
                        .status(),
                    reqwest::StatusCode::UNAUTHORIZED
                );
            }
            // Authorized reads return contract_not_found before anything is published.
            let response = client
                .get(format!("{origin}{path}"))
                .bearer_auth("api-key")
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
            assert_eq!(
                response.json::<serde_json::Value>().await?["error"]["code"],
                "contract_not_found"
            );
        }
        // 2. Publish r1 in both namespaces; repeating the same deployment is a no-op.
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../../sdk/fixtures/public-contract.json"))?;
        let mut deployment = serde_json::json!({"codeRevision":"r1", "imageRef":"image", "workingDirectory":"/app", "contract":document});
        for scope in ["/v1", "/v1/namespaces/team"] {
            // These are expected responses, not changes to the request.
            for changed in [true, false] {
                let reply: serde_json::Value = client
                    .put(format!("{origin}{scope}/deployment"))
                    .bearer_auth("api-key")
                    .json(&deployment)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                assert_eq!(reply["changed"], changed);
            }
            // Read back the published document, revision, and hash without HTTP caching.
            let response = client
                .get(format!("{origin}{scope}/contract"))
                .bearer_auth("api-key")
                .send()
                .await?
                .error_for_status()?;
            assert_eq!(response.headers()["cache-control"], "no-store");
            let reply: serde_json::Value = response.json().await?;
            assert_eq!(reply["contract"], document);
            assert_eq!(reply["codeRevision"], "r1");
            assert!(
                reply["contractHash"]
                    .as_str()
                    .unwrap()
                    .starts_with("sha256:")
            );
        }
        // 3. In the default namespace, changing the contract while keeping r1 conflicts.
        deployment["contract"] = serde_json::json!({"version":1,"actors":[]});
        let response = client
            .put(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        // 4. The changed contract is accepted under r2 and becomes the active contract.
        deployment["codeRevision"] = "r2".into();
        client
            .put(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?
            .error_for_status()?;
        let active: serde_json::Value = client
            .get(format!("{origin}/v1/contract"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(active["codeRevision"], "r2");
        assert_eq!(active["contract"]["actors"], serde_json::json!([]));
        // 5. Explicit revision reads find r2, but the previous r1 contract is gone.
        assert_eq!(
            client
                .get(format!("{origin}/v1/contract?revision=r1"))
                .bearer_auth("api-key")
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        let pinned: serde_json::Value = client
            .get(format!("{origin}/v1/contract?revision=r2"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(pinned, active);
        // 6. Reject a revision containing '/' and an unsupported query parameter.
        for suffix in ["?revision=bad%2Frevision", "?unknown=1"] {
            assert_eq!(
                client
                    .get(format!("{origin}/v1/contract{suffix}"))
                    .bearer_auth("api-key")
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::BAD_REQUEST
            );
        }
        // 7. Reject r3's unsupported contract format version and leave r2 deployed.
        deployment["codeRevision"] = "r3".into();
        deployment["contract"] = serde_json::json!({"version":2,"actors":[]});
        assert_eq!(
            client
                .put(format!("{origin}/v1/deployment"))
                .bearer_auth("api-key")
                .json(&deployment)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::BAD_REQUEST
        );
        let unchanged: serde_json::Value = client
            .get(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(unchanged["codeRevision"], "r2");
        server.abort();
        Ok(())
    }

    pub(super) fn test_issuer() -> Result<ActorJwtIssuer> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "test-key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )
    }
}
