use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    actor::{ActorKey, ActorSocketEvent},
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
        actor_host_service_client::ActorHostServiceClient,
    },
    host::HostId,
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
    pub(super) traces: crate::request_traces::TraceStore,
    pub(super) changes: tokio::sync::watch::Sender<()>,
    pub(super) region: Option<String>,
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    leases: Arc<dyn HostLeaseStore>,
    placements: Arc<dyn ObjectPlacementStore>,
    auth: ActorJwtVerifier,
    host_token_issuer: ActorJwtIssuer,
    registry: Arc<dyn AdminRegistry>,
    provisioner: Arc<dyn HostProvisioner>,
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
            traces: crate::request_traces::TraceStore::default(),
            changes: tokio::sync::watch::channel(()).0,
            runtime_access: None,
            region: None,
            leases,
            placements,
            auth,
            host_token_issuer: issuer,
            registry,
            provisioner,
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

    pub(crate) fn with_traces(mut self, traces: crate::request_traces::TraceStore) -> Self {
        self.traces = traces;
        self
    }

    pub fn into_internal_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
    }

    pub(super) fn default_region(&self) -> &str {
        self.region.as_deref().unwrap_or(FALLBACK_REGION)
    }

    pub(super) async fn runtime_deployment(&self) -> Result<Option<HostLaunchSpec>> {
        self.registry.launch_spec().await
    }

    pub(super) async fn register_deployment(
        &self,
        admin: &AdminService,
        spec: &HostLaunchSpec,
        contract: Option<&super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        let previous = admin.current_deployment().await?;
        spec.validate()?;
        admin.validate_contract_registration(spec, contract).await?;
        if let Some(previous) = previous
            && previous != *spec
        {
            self.terminate_deployment_hosts(&previous).await?;
        }
        let changed = admin.register_deployment(spec, contract).await?;
        self.changes.send_replace(());
        Ok(changed)
    }

    pub(super) async fn delete_deployment(&self, admin: &AdminService) -> Result<bool> {
        let Some(previous) = admin.current_deployment().await? else {
            return Ok(false);
        };
        self.terminate_deployment_hosts(&previous).await?;
        admin.remove_deployment().await?;
        self.changes.send_replace(());
        Ok(true)
    }

    pub(super) fn warm_deployment_image(&self, spec: HostLaunchSpec, region: String) {
        let provisioner = self.provisioner.clone();
        if super::regions::storage_region(&region).is_err() {
            warn!(
                event = "actor_image_warmup",
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

    pub(super) async fn resolve_actor_target_timed(
        &self,
        actor: &ActorKey,
        home_region: Option<&str>,
        timings: &mut TargetResolutionTimings,
    ) -> Result<ActorTarget> {
        self.resolve_actor_route(actor, home_region, Some(timings))
            .await
    }

    pub(super) fn validate_home_region(&self, assignment: Option<&str>) -> Result<()> {
        if let Some(assignment) = assignment {
            crate::placement::validate_region(assignment)?;
        }
        if let Some(local) = &self.region {
            let assignment =
                assignment.context("homeRegion is required by this regional control plane")?;
            if assignment != local {
                return Err(RegionConflict.into());
            }
        }
        Ok(())
    }

    pub(super) async fn socket_destination(
        &self,
        actor: &ActorKey,
        region: &str,
        home_region: Option<&str>,
    ) -> Result<(
        String,
        super::socket_ticket::SocketTarget,
        crate::sandbox::SocketCredentials,
    )> {
        let routed = self.route_actor(actor, region, home_region, None).await?;
        let credentials = self
            .provisioner
            .socket_credentials(&routed.spec, &routed.placement.home_region, &routed.lease)
            .await?;
        Ok((
            routed.placement.home_region,
            super::socket_ticket::SocketTarget {
                host_id: routed.lease.id,
                session_id: routed.lease.session_id,
                owner_epoch: routed.placement.owner_epoch,
            },
            credentials,
        ))
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

    async fn resolve_actor_route(
        &self,
        actor: &ActorKey,
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
    ) -> Result<ActorTarget> {
        actor.validate()?;
        let target = self
            .route_actor(
                actor,
                self.default_region(),
                home_region,
                timings.as_deref_mut(),
            )
            .await?;
        let issued = self.host_token_issuer.issue_invocation_target(
            actor,
            &target.lease.id,
            &target.lease.session_id,
            &target.spec.host_revision(),
            &target.placement.home_region,
            target.placement.owner_epoch,
        )?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.invocation_token_issued_at_ms = Some(timings.elapsed_ms());
        }
        let route = target.lease.route;
        if let Some(timings) = timings {
            timings.route_selected_at_ms = Some(timings.elapsed_ms());
        }
        Ok(ActorTarget {
            home_region: target.placement.home_region,
            route,
            token: issued.token,
            owner_epoch: target.placement.owner_epoch,
            expires_at_ms: issued.expires_at_ms,
        })
    }
}

pub(super) struct ActorTarget {
    pub home_region: String,
    pub route: String,
    pub token: String,
    pub owner_epoch: u64,
    pub expires_at_ms: i64,
}

pub(super) struct TargetResolutionTimings {
    started_at: Instant,
    pub request_validated_at_ms: Option<f64>,
    pub client_authenticated_at_ms: Option<f64>,
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
            client_authenticated_at_ms: None,
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
        match command {
            ControlPlaneCommand::RequestTraces { traces, dropped } => {
                self.require_active_host(principal).await?;
                ensure!(
                    traces.len() <= crate::request_traces::TRACE_BATCH_SIZE,
                    "trace batch too large"
                );
                for trace in &traces {
                    trace.validate()?;
                }
                self.traces
                    .record(
                        principal.host_id.as_str(),
                        &principal.session_id,
                        traces,
                        dropped,
                    )
                    .await?;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::InventoryChanged => {
                let status = self.leases.lease_status(&principal.host_id).await?;
                ensure!(
                    status
                        .lease
                        .is_some_and(|lease| lease.session_id == principal.session_id),
                    "host session does not match"
                );
                self.changes.send_replace(());
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::SocketMessage { actor, event } => {
                self.authorize_socket_host(principal, &actor).await?;
                self.deliver_socket_message_event(&actor, None, &event);
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::RefreshStorageAccess => {
                self.require_active_host(principal).await?;
                let token = self
                    .runtime_access
                    .as_ref()
                    .context("direct storage is not configured")?
                    .issue()
                    .await?;
                let replacement_token = self
                    .host_token_issuer
                    .issue_host(
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

    async fn authorize_socket_host(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<()> {
        actor.validate()?;
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
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
    ) -> Result<RoutedActor> {
        self.validate_home_region(home_region)?;
        let storage_region = match home_region {
            Some(region) => region.to_owned(),
            None => select_target_region(None, storage_region)?,
        };
        let spec = self
            .runtime_deployment()
            .await?
            .context("project has no registered actor code")?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.deployment_loaded_at_ms = Some(timings.elapsed_ms());
        }
        let current = self.placements.get_owner(&actor.storage_key()).await?;
        if let (Some(assigned), Some(placement)) = (home_region, current.as_ref())
            && assigned != placement.home_region
        {
            return Err(RegionConflict.into());
        }
        if let Some(timings) = timings.as_deref_mut() {
            timings.placement_loaded_at_ms = Some(timings.elapsed_ms());
        }
        let lease_checked = current.as_ref().is_some_and(|placement| {
            host_matches_revision(&placement.owner, &spec.host_revision())
        });
        let active = self.active_target(&current, &spec).await?;
        if lease_checked && let Some(timings) = timings.as_deref_mut() {
            timings.lease_checked_at_ms = Some(timings.elapsed_ms());
        }
        if let Some(target) = active {
            return Ok(target);
        }
        let (region, lease) = self
            .ensure_actor_host(&spec, current.as_ref(), &storage_region)
            .await?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.host_ensured_at_ms = Some(timings.elapsed_ms());
        }
        let placement = self.activate_host(actor, &spec, &lease, &region).await?;
        if let Some(timings) = timings {
            timings.placement_claimed_at_ms = Some(timings.elapsed_ms());
        }
        Ok(RoutedActor {
            placement,
            lease,
            spec,
        })
    }

    async fn activate_host(
        &self,
        actor: &ActorKey,
        spec: &HostLaunchSpec,
        lease: &HostLease,
        region: &str,
    ) -> Result<ObjectPlacement> {
        let token = self.host_token_issuer.issue_host(
            &lease.id,
            &lease.session_id,
            &spec.host_revision(),
            region,
        )?;
        let channel = Endpoint::new(lease.route.clone())?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .context("connect to actor host")?;
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
        let region = current
            .map(|p| p.home_region.clone())
            .unwrap_or_else(|| requested_region.to_owned());
        let lease = self.provisioner.ensure_host(spec, &region).await?;
        Ok((region, lease))
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
        if !host_matches_revision(&placement.owner, &spec.host_revision()) {
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
}

fn select_target_region(current: Option<&ObjectPlacement>, requested: &str) -> Result<String> {
    if let Some(placement) = current {
        return Ok(placement.home_region.clone());
    }
    let region = super::regions::storage_region(requested).unwrap_or(FALLBACK_REGION);
    crate::placement::validate_region(region)?;
    Ok(region.into())
}

#[derive(Debug)]
pub(super) struct RegionConflict;

impl std::fmt::Display for RegionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "homeRegion conflicts with the control-plane region or existing actor ownership",
        )
    }
}

impl std::error::Error for RegionConflict {}

#[async_trait]
pub(crate) trait HostProvisioner: Send + Sync {
    async fn socket_credentials(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _lease: &HostLease,
    ) -> Result<crate::sandbox::SocketCredentials> {
        anyhow::bail!("host provider does not support direct sockets")
    }

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
    async fn socket_credentials(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        lease: &HostLease,
    ) -> Result<crate::sandbox::SocketCredentials> {
        self.provider
            .socket_credentials(&crate::sandbox::SocketCredentialsRequest {
                code_revision: spec.host_revision(),
                canonical_region: region.into(),
                host_id: lease.id.clone(),
                session_id: lease.session_id.clone(),
            })
            .await
    }

    async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease> {
        let started_at = Instant::now();
        let mut request = self.request(spec, region)?;
        if let Some(access) = &self.runtime_access {
            request.runtime_config = Some(access.bootstrap(region).await?);
        }
        let handle = match self.provider.ensure_host(&request).await {
            Ok(handle) => handle,
            Err(error) => {
                let command = error.downcast_ref::<ProviderCommandFailure>();
                warn!(
                    event = "actor_host_provisioning",
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
                code_revision: spec.host_revision(),
                canonical_regions: regions.to_vec(),
            })
            .await
    }
}

impl SandboxHostProvisioner {
    fn request(&self, spec: &HostLaunchSpec, region: &str) -> Result<EnsureHostRequest> {
        let revision = spec.host_revision();
        let host_id = HostId::new(format!("host.v3.{}.{}", revision, uuid::Uuid::new_v4()));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = self
            .issuer
            .issue_host(&host_id, &session_id, &revision, region)?
            .token;
        Ok(EnsureHostRequest {
            runtime_config: None,
            code_revision: revision,
            canonical_region: region.to_owned(),
            host_id,
            session_id,
            host_token,
            jwt_public_keys: self.issuer.verifier_keys_json()?,
            control_plane_url: self.runtime.control_plane_url.clone(),
            jwt_issuer: self.runtime.jwt_issuer.clone(),
            invocation_jwt_audience: self.runtime.invocation_jwt_audience.clone(),
            socket_jwt_audience: self.issuer.socket_audience(),
            image_ref: spec.image_ref.clone(),
            working_directory: spec.working_directory.clone(),
            actor_entrypoint: spec.actor_entrypoint.clone(),
            secret_refs: spec.secret_refs.clone(),
            actor_idle_timeout_seconds: self.runtime.actor_idle_timeout_seconds,
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

fn host_matches_revision(host: &HostId, revision: &str) -> bool {
    host.as_str().starts_with(&format!("host.v3.{revision}."))
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

fn failed_precondition(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
#[path = "../../tests/unit/control_plane/service.rs"]
mod tests;
