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
#[path = "streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex, time::Duration};

    use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
    use super::*;
    use crate::{
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
            peers: Arc<Mutex<std::collections::HashSet<std::net::SocketAddr>>>,
        }
        #[tonic::async_trait]
        impl ActorHostService for Host {
            async fn publish_socket_effects(
                &self,
                _: tonic::Request<crate::grpc::proto::PublishSocketEffectsRequest>,
            ) -> Result<tonic::Response<crate::grpc::proto::Empty>, tonic::Status> {
                Err(tonic::Status::unimplemented(
                    "fixture does not publish socket effects",
                ))
            }

            async fn activate(
                &self,
                request: Request<proto::ActivateActorRequest>,
            ) -> Result<Response<proto::ActivateActorReply>, Status> {
                self.peers
                    .lock()
                    .unwrap()
                    .insert(request.remote_addr().unwrap());
                let principal = self.auth.authenticate(&request).await?;
                assert!(principal.invocation.is_none());
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
        let peers = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let host_peers = peers.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ActorHostServiceServer::new(Host {
                    auth: invocation_auth,
                    peers: host_peers,
                }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .register_test_deployment(&HostLaunchSpec {
                code_revision: "revision".into(),
                image_ref: "image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
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
                id: HostId::new("host.v3.revision.host"),
                session_id: uuid::Uuid::new_v4().to_string(),
                route: route.clone(),
                expires_at_ms: u64::MAX,
            })),
        );
        let actor = ActorKey {
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        };
        let target = service.resolve_actor_route(&actor, None, None).await?;
        assert_eq!(target.route, route);
        assert_eq!(target.owner_epoch, 42);
        assert!(placements.get(&actor.storage_key()).await?.is_none());
        service.resolve_actor_route(&actor, None, None).await?;
        assert_eq!(
            peers.lock().unwrap().len(),
            2,
            "activations open fresh connections"
        );
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
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
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
        assert_eq!(admin.current_deployment().await?, Some(first.clone()));
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
        assert_eq!(admin.current_deployment().await?, Some(replacement.clone()));
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
        assert!(service.delete_deployment(&admin).await.is_err());
        assert_eq!(admin.current_deployment().await?, Some(secret_update));
        provisioner
            .fail
            .store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(service.delete_deployment(&admin).await?);
        assert_eq!(admin.current_deployment().await?, None);
        assert!(!service.delete_deployment(&admin).await?);
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
            code_revision: "revision-1".into(),
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
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
        assert_eq!(delivered["actorType"], "ChatRoom");
        assert_eq!(delivered["actorId"], "room-1");
        assert_eq!(delivered["triggerId"], "trigger-1");
        assert_eq!(delivered["connectionId"], "socket-1");
        assert_eq!(delivered["message"]["type"], "text");
        assert_eq!(delivered["message"]["data"], "hello");
        Ok(())
    }

    #[test]
    fn existing_actors_stay_pinned_to_the_assigned_region() -> Result<()> {
        let actor = ActorStorageKey::new("object.v1.project.Counter.one");
        let current = ObjectPlacement {
            object: actor,
            owner: HostId::new("host.v3.revision.host"),
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
        async fn socket_credentials(
            &self,
            _spec: &HostLaunchSpec,
            _region: &str,
            lease: &HostLease,
        ) -> Result<crate::sandbox::SocketCredentials> {
            Ok(crate::sandbox::SocketCredentials {
                url: lease.route.clone(),
                token: String::new(),
            })
        }

        async fn ensure_host(&self, spec: &HostLaunchSpec, region: &str) -> Result<HostLease> {
            self.calls.lock().unwrap().push(region.to_owned());
            ensure!(
                !self.failed_regions.contains(&region),
                "host unavailable in {region}"
            );
            Ok(HostLease {
                id: HostId::new(format!("host.v3.{}.{region}", spec.host_revision())),
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
    async fn provisioning_never_changes_the_assigned_region() -> Result<()> {
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
            (vec![south], false, "southcentralus", None, vec![south]),
            (vec![south], true, "eastus", None, vec![south]),
            (
                vec![south, central],
                false,
                "southcentralus",
                None,
                vec![south],
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
                .register_test_deployment(&HostLaunchSpec {
                    code_revision: "revision".into(),
                    image_ref: "image".into(),
                    working_directory: "/app".into(),
                    actor_entrypoint: None,
                    secret_refs: vec![],
                })
                .await?;
            let placements = Arc::new(LocalObjectPlacementStore::default());
            let actor = ActorKey {
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
            let spec = registry.launch_spec().await?.unwrap();
            let result = service
                .ensure_actor_host(
                    &spec,
                    before.as_ref(),
                    &select_target_region(before.as_ref(), reported)?,
                )
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
            .register_test_deployment(&HostLaunchSpec {
                code_revision: "v1".into(),
                image_ref: "image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
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
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::new()),
        });
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let host_id = HostId::new("host.v3.v1.fixture");
        leases
            .register(&HostLeaseRequest {
                id: host_id.clone(),
                session_id: "00000000-0000-4000-8000-000000000001".into(),
                route: "https://host.example.com".into(),
                duration_ms: 60_000,
            })
            .await?;
        let actor = ActorKey {
            actor_type: "Room".into(),
            actor_id: "lobby".into(),
        };
        placements
            .claim(&actor.storage_key(), None, &host_id, "north-america-east")
            .await?;
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
        let requests = [(
            "actors/Room/lobby/connect",
            serde_json::json!({"transport":"websocket", "metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000}),
        )];
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
            let mut url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
            let key = url
                .query_pairs()
                .find(|(name, _)| name == "key")
                .unwrap()
                .1
                .into_owned();
            url.set_query(None);
            assert_eq!(url.as_str(), "wss://host.example.com/v1/socket");
            let ticket = issuer.verify_socket(&key)?;
            assert_eq!(ticket.actor.actor_id, "lobby");
            assert_eq!(ticket.metadata, body["metadata"]);
            assert_eq!(issued["transport"], "websocket");
            assert_eq!(issued["homeRegion"], "north-america-east");
            assert!(issued.get("key").is_none());
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
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::new()),
        });
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let host_id = HostId::new("host.v3.v1.fixture");
        leases
            .register(&HostLeaseRequest {
                id: host_id.clone(),
                session_id: "00000000-0000-4000-8000-000000000001".into(),
                route: "https://host.example.com".into(),
                duration_ms: 60_000,
            })
            .await?;
        let actor = ActorKey {
            actor_type: "Room".into(),
            actor_id: "lobby".into(),
        };
        placements
            .claim(&actor.storage_key(), None, &host_id, "north-america-east")
            .await?;
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
        let url = format!("{origin}/v1/actors/Room/lobby/connect");
        let body = serde_json::json!({"transport":"websocket", "metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000,"homeRegion":"north-america-east"});
        let host_token = issuer
            .issue_host(
                &HostId::new("host.v3.v1.fixture"),
                &uuid::Uuid::new_v4().to_string(),
                "v1",
                "north-america-east",
            )?
            .token;
        for credential in ["", "wrong", &host_token] {
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
            .json(&serde_json::json!({"codeRevision":"v1","imageRef":"image","workingDirectory":"/app"}))
            .send().await?.error_for_status()?;
        for operation in ["websocket", "grpc"] {
            let response = client
                .post(&url)
                .bearer_auth("api-key")
                .json(&if operation == "websocket" {
                    serde_json::json!({"transport":"websocket", "metadata":{},"homeRegion":"north-america-west"})
                } else {
                    serde_json::json!({"transport":"grpc", "homeRegion":"north-america-west"})
                })
                .send()
                .await?;
            assert_eq!(
                response.status(),
                reqwest::StatusCode::CONFLICT,
                "{operation}"
            );
        }
        let issued = client
            .post(&url)
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(issued.headers().get("cache-control").unwrap(), "no-store");
        let issued: serde_json::Value = issued.json().await?;
        let socket_url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
        let key = socket_url
            .query_pairs()
            .find(|(name, _)| name == "key")
            .unwrap()
            .1
            .into_owned();
        assert_eq!(socket_url.scheme(), "wss");
        assert_eq!(socket_url.host_str(), Some("host.example.com"));
        assert_eq!(socket_url.path(), "/v1/socket");
        assert_eq!(
            socket_url.query_pairs().collect::<Vec<_>>(),
            vec![("key".into(), key.as_str().into())]
        );
        assert_ne!(key, "api-key");
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(&key)
                .json(&body)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .post(format!("{origin}/v1/actors/Room/lobby/connect"))
                .bearer_auth(&key)
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
    async fn api_key_access_connects_directly_without_an_http_socket_relay() -> Result<()> {
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
        {
            let host = HostId::new("host.v3.revision.test");
            leases
                .register(&HostLeaseRequest {
                    id: host.clone(),
                    session_id: "00000000-0000-4000-8000-000000000001".into(),
                    route: "https://host.example.com".into(),
                    duration_ms: 60_000,
                })
                .await?;
            let actor = ActorKey {
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
        for suffix in ["connect"] {
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
            .post(format!("{origin}/v1/actors/Counter/one/connect"))
            .bearer_auth("api-key")
            .json(&serde_json::json!({"transport":"grpc"}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(target["transport"], "grpc");
        assert_eq!(target["homeRegion"], "north-america-east");
        assert_eq!(target["route"], "https://host.example.com");
        for body in [
            serde_json::json!({}),
            serde_json::json!({"transport":"http"}),
            serde_json::json!({"transport":"grpc","metadata":{}}),
            serde_json::json!({"transport":"websocket"}),
            serde_json::json!({"transport":"websocket","metadata":{},"unknown":true}),
        ] {
            let reply = client
                .post(format!("{origin}/v1/actors/Counter/one/connect"))
                .bearer_auth("api-key")
                .json(&body)
                .send()
                .await?;
            assert_eq!(reply.status(), reqwest::StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(
                reply.json::<serde_json::Value>().await?["error"]["code"],
                "invalid_request"
            );
        }
        assert_ne!(target["token"], "api-key");
        assert_eq!(
            client
                .post(format!("{origin}/v1/actors/Counter/one/socket-effects"))
                .bearer_auth("api-key")
                .json(&serde_json::json!({"effects":[]}))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn deployment_reads_and_deletion_require_the_api_key() -> Result<()> {
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
            .register_test_deployment(&HostLaunchSpec {
                code_revision: "revision-1".into(),
                image_ref: "image-1".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
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
        let deployment_url = format!("{origin}/v1/deployment");
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
        assert_eq!(
            client
                .get(&deployment_url)
                .bearer_auth("api-key")
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn contract_api_publishes_with_deployments_and_reads_only_the_active_revision()
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
        let host_token = issuer
            .issue_host(
                &HostId::new("host.v3.r1.one"),
                &uuid::Uuid::new_v4().to_string(),
                "r1",
                "us-east",
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
        for path in ["/v1/deployment/contract"] {
            for credential in ["", "wrong", &host_token] {
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
            let response = client
                .get(format!("{origin}{path}"))
                .bearer_auth("api-key")
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
            assert_eq!(
                response.json::<serde_json::Value>().await?["error"]["code"],
                "not_found"
            );
        }
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../../sdk/fixtures/public-contract.json"))?;
        let mut deployment = serde_json::json!({"codeRevision":"r1", "imageRef":"image", "workingDirectory":"/app", "contract":document});
        for scope in ["/v1"] {
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
            let response = client
                .get(format!("{origin}{scope}/deployment/contract"))
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
        deployment["contract"] = serde_json::json!({"version":1,"actors":[]});
        let response = client
            .put(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        deployment["codeRevision"] = "r2".into();
        client
            .put(format!("{origin}/v1/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?
            .error_for_status()?;
        let active: serde_json::Value = client
            .get(format!("{origin}/v1/deployment/contract"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(active["codeRevision"], "r2");
        assert_eq!(active["contract"]["actors"], serde_json::json!([]));
        assert_eq!(
            client
                .get(format!("{origin}/v1/deployment/contract?revision=r1"))
                .bearer_auth("api-key")
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        let pinned: serde_json::Value = client
            .get(format!("{origin}/v1/deployment/contract?revision=r2"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(pinned, active);
        for suffix in ["?revision=bad%2Frevision", "?unknown=1"] {
            assert_eq!(
                client
                    .get(format!("{origin}/v1/deployment/contract{suffix}"))
                    .bearer_auth("api-key")
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::BAD_REQUEST
            );
        }
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
