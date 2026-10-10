pub(crate) mod transport;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::host::HostId;

mod local;
mod local_store;
pub(crate) mod substrate;

pub(crate) use local::LocalSandboxProvider;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    pub cpu_millis: u32,
    pub memory_mib: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            cpu_millis: 1000,
            memory_mib: 256,
        }
    }
}

pub struct RuntimeTemplateRequest {
    pub code_snapshot: Option<String>,
    pub image_ref: String,
    pub canonical_region: String,
    pub resources: Vec<ResourceLimits>,
    pub jwt_public_keys: String,
    pub jwt_issuer: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureHostRequest {
    pub actor_is_new: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_hint: Option<String>,
    pub actor: Option<crate::actor::ActorKey>,
    pub code_snapshot: Option<String>,
    pub resources: ResourceLimits,
    pub runtime_config: Option<String>,

    pub host_config_key: String,
    pub canonical_region: String,
    pub host_id: HostId,
    pub session_id: String,
    pub host_token: String,
    pub jwt_public_keys: String,
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub secret_refs: Vec<String>,
    pub host_idle_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostHandle {
    pub lease: Option<crate::host_leases::HostLease>,
    #[serde(default)]
    pub owner_epoch: u64,
    pub host_id: HostId,
    pub route: String,
    pub canonical_region: String,
    pub provisioning: Option<ActorHostProvisioning>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostProvisioning {
    pub provider: String,
    pub resource_id: String,
    pub reused: bool,
    pub started_at_ms: u64,
    pub input_parsed_at_ms: Option<u64>,
    pub sdk_loaded_at_ms: Option<u64>,
    pub resources_resolved_at_ms: Option<u64>,
    pub sandbox_scheduled_at_ms: Option<u64>,
    pub host_ready_observed_at_ms: Option<u64>,
    pub route_read_at_ms: Option<u64>,
    pub completed_at_ms: u64,
    #[serde(default)]
    pub command_spawned_at_ms: Option<u64>,
    #[serde(default)]
    pub request_written_at_ms: Option<u64>,
    #[serde(default)]
    pub process_completed_at_ms: Option<u64>,
    #[serde(default)]
    pub response_decoded_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminateHostsRequest {
    pub host_config_key: String,
    pub canonical_regions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostTermination {
    pub provider: String,
    pub resource_ids: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct HostNotReady;

impl std::fmt::Display for HostNotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actor host is not ready")
    }
}

impl std::error::Error for HostNotReady {}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    fn bootstraps_storage(&self) -> bool {
        false
    }

    async fn prepare_runtime(&self, request: &RuntimeTemplateRequest) -> Result<()>;
    async fn wait_ready(&self, _host: &HostId) -> Result<()> {
        Ok(())
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination>;
}

#[derive(Clone)]
pub struct HostSandboxRuntimeConfig {
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub host_idle_timeout_ms: u64,
}

#[cfg(test)]
#[path = "../tests/support/sandbox.rs"]
pub(crate) mod testing;
