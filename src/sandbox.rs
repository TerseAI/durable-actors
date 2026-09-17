use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::host::HostId;

mod command_process;
mod local;

pub(crate) use local::LocalSandboxProvider;

const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_PROVIDER_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureHostRequest {
    pub runtime_config: Option<String>,
    pub namespace_id: String,
    pub code_revision: String,
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
    pub socket_gateway_url: String,
    pub actor_idle_timeout_ms: u64,
    pub host_idle_timeout_ms: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EnsureReplicaRequest {
    pub installation_id: String,
    pub slot: usize,
    pub canonical_region: String,
    pub image_ref: String,
    pub host_id: String,
    pub secret: String,
    pub control_plane_url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostHandle {
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
    pub existing_host_checked_at_ms: Option<u64>,
    pub sandbox_scheduled_at_ms: Option<u64>,
    pub host_ready_observed_at_ms: Option<u64>,
    pub route_read_at_ms: Option<u64>,
    pub metadata_written_at_ms: Option<u64>,
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
pub struct WarmImageRequest {
    pub namespace_id: String,
    pub code_revision: String,
    pub canonical_region: String,
    pub image_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageWarmup {
    pub provider: String,
    pub resource_id: String,
    pub total_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminateHostsRequest {
    pub namespace_id: String,
    pub code_revision: String,
    pub canonical_regions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostTermination {
    pub provider: String,
    pub resource_ids: Vec<String>,
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
    async fn warm_image(&self, request: &WarmImageRequest) -> Result<ImageWarmup>;
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination>;
}

#[derive(Clone)]
pub struct HostSandboxRuntimeConfig {
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub actor_idle_timeout_ms: u64,
    pub host_idle_timeout_ms: u64,
}

pub struct CommandSandboxProvider {
    provider_name: String,
    command: String,
    environment: HashMap<String, String>,
}

impl CommandSandboxProvider {
    pub(crate) async fn ensure_replica(
        &self,
        request: &EnsureReplicaRequest,
    ) -> Result<ActorHostHandle> {
        let handle: ActorHostHandle = self.execute("ensure_replica", request).await?;
        ensure!(
            handle.canonical_region == request.canonical_region,
            "replica provider returned the wrong region"
        );
        ensure!(
            !handle.host_id.as_str().is_empty(),
            "replica provider returned no host identity"
        );
        let route = reqwest::Url::parse(&handle.route)?;
        ensure!(
            route.scheme() == "https" && route.host_str().is_some(),
            "replica route must be HTTPS"
        );
        Ok(handle)
    }

    pub fn new(
        provider_name: String,
        command: String,
        mut environment: HashMap<String, String>,
    ) -> Result<Self> {
        ensure!(
            !provider_name.is_empty() && provider_name.trim() == provider_name,
            "sandbox provider name must be non-empty without surrounding whitespace"
        );
        ensure!(
            !command.is_empty() && command.trim() == command,
            "DURABLE_OBJECT_SANDBOX_COMMAND must be non-empty without surrounding whitespace"
        );
        if let Ok(path) = std::env::var("PATH") {
            environment.entry("PATH".into()).or_insert(path);
        }
        Ok(Self {
            provider_name,
            command,
            environment,
        })
    }
}

#[async_trait]
impl SandboxProvider for CommandSandboxProvider {
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let (mut response, command): (ActorHostHandle, _) =
            self.execute_timed("ensure_host", request).await?;
        if let Some(provisioning) = &mut response.provisioning {
            provisioning.command_spawned_at_ms = command.spawned_at_ms;
            provisioning.request_written_at_ms = command.request_written_at_ms;
            provisioning.process_completed_at_ms = command.process_completed_at_ms;
            provisioning.response_decoded_at_ms = command.response_decoded_at_ms;
        }
        ensure!(
            response.canonical_region == request.canonical_region,
            "{} sandbox command returned a host in the wrong canonical region",
            self.provider_name
        );
        ensure!(
            !response.host_id.as_str().is_empty() && !response.route.is_empty(),
            "{} sandbox command returned an invalid host",
            self.provider_name
        );
        Ok(response)
    }

    async fn warm_image(&self, request: &WarmImageRequest) -> Result<ImageWarmup> {
        self.execute("warm_image", request).await
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        self.execute("terminate_hosts", request).await
    }
}

impl CommandSandboxProvider {
    async fn execute<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<Reply> {
        Ok(self.execute_timed(operation, request).await?.0)
    }

    async fn execute_timed<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<(Reply, ProviderCommandTimings)> {
        let started_at = Instant::now();
        let mut timings = ProviderCommandTimings::default();
        match self
            .execute_timed_inner(operation, request, started_at, &mut timings)
            .await
        {
            Ok(response) => Ok((response, timings)),
            Err(source) => Err(ProviderCommandFailure { source, timings }.into()),
        }
    }

    async fn execute_timed_inner<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
    ) -> Result<Reply> {
        let command = ProviderCommand { operation, request };
        let execution = command_process::exchange(
            &self.command,
            &self.environment,
            &command,
            started_at,
            timings,
        );
        tokio::time::timeout(PROVIDER_REQUEST_TIMEOUT, execution)
            .await
            .context("sandbox provider command timed out; outcome may be unknown")?
    }
}

#[derive(Debug, Default)]
struct ProviderCommandTimings {
    spawned_at_ms: Option<u64>,
    request_written_at_ms: Option<u64>,
    process_completed_at_ms: Option<u64>,
    response_decoded_at_ms: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct ProviderCommandFailure {
    source: anyhow::Error,
    timings: ProviderCommandTimings,
}

impl ProviderCommandFailure {
    pub(crate) fn spawned_at_ms(&self) -> Option<u64> {
        self.timings.spawned_at_ms
    }

    pub(crate) fn request_written_at_ms(&self) -> Option<u64> {
        self.timings.request_written_at_ms
    }

    pub(crate) fn process_completed_at_ms(&self) -> Option<u64> {
        self.timings.process_completed_at_ms
    }

    pub(crate) fn response_decoded_at_ms(&self) -> Option<u64> {
        self.timings.response_decoded_at_ms
    }
}

impl std::fmt::Display for ProviderCommandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for ProviderCommandFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[derive(Serialize)]
struct ProviderCommand<'a, Request> {
    operation: &'a str,
    request: &'a Request,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_provider_provisioning_timings() {
        let handle: ActorHostHandle = serde_json::from_value(serde_json::json!({
            "hostId": "host.v2.namespace:revision.session",
            "route": "https://host.example.com",
            "canonicalRegion": "north-america-east",
            "provisioning": {
                "provider": "modal",
                "resourceId": "sb-actor",
                "reused": false,
                "startedAtMs": 0,
                "resourcesResolvedAtMs": 12,
                "existingHostCheckedAtMs": 34,
                "sandboxScheduledAtMs": 56,
                "hostReadyObservedAtMs": 123,
                "routeReadAtMs": 125,
                "metadataWrittenAtMs": 129,
                "completedAtMs": 130
            }
        }))
        .expect("actor host handle");

        let provisioning = handle.provisioning.expect("provisioning timings");
        assert_eq!(provisioning.resource_id, "sb-actor");
        assert_eq!(provisioning.sandbox_scheduled_at_ms, Some(56));
        assert_eq!(provisioning.completed_at_ms, 130);
    }

    #[test]
    fn rejects_ambiguous_command_configuration() {
        assert!(CommandSandboxProvider::new("".into(), "modal".into(), HashMap::new()).is_err());
        assert!(CommandSandboxProvider::new("modal".into(), "".into(), HashMap::new()).is_err());
    }

    #[tokio::test]
    async fn provider_calls_use_independent_processes() -> Result<()> {
        let (directory, provider) = test_provider()?;
        let requests = (0..5)
            .map(|index| {
                serde_json::json!({
                    "index": index,
                    "barrier": directory.path(),
                })
            })
            .collect::<Vec<_>>();
        let replies = tokio::time::timeout(
            Duration::from_secs(3),
            futures_util::future::try_join_all(
                requests
                    .iter()
                    .map(|request| provider.execute::<_, serde_json::Value>("test", request)),
            ),
        )
        .await
        .expect("all five independent processes must start before any replies")?;
        let pids = replies
            .iter()
            .map(|reply| reply["pid"].as_u64().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(pids.len(), 5);
        for (index, reply) in replies.iter().enumerate() {
            assert_eq!(reply["index"], index);
        }
        Ok(())
    }

    #[tokio::test]
    async fn provider_failures_do_not_affect_other_calls() -> Result<()> {
        let (_directory, provider) = test_provider()?;
        for (request, message) in [
            (serde_json::json!({"fail": true}), "test failure"),
            (serde_json::json!({"oversized": true}), "stdout exceeds"),
            (
                serde_json::json!({"malformed": true}),
                "decode provider response",
            ),
            (serde_json::json!({"exit": true}), "exited"),
        ] {
            let healthy_request = serde_json::json!({"index": 42});
            let (failed, healthy) = tokio::join!(
                provider.execute::<_, serde_json::Value>("test", &request),
                provider.execute::<_, serde_json::Value>("test", &healthy_request),
            );
            assert!(failed.unwrap_err().to_string().contains(message));
            assert_eq!(healthy?["index"], 42);
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_one_provider_call_terminates_only_its_process() -> Result<()> {
        let (directory, provider) = test_provider()?;
        let marker = directory.path().join("cancelled.pid");
        let request = serde_json::json!({"marker": marker, "delay": 30_000});
        let mut cancelled = Box::pin(provider.execute::<_, serde_json::Value>("test", &request));
        let wait_for_start = async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            result = &mut cancelled => panic!("provider should still be waiting: {result:?}"),
            started = tokio::time::timeout(Duration::from_secs(3), wait_for_start) => started?,
        }
        let pid: u32 = std::fs::read_to_string(marker)?.parse()?;
        drop(cancelled);
        let healthy: serde_json::Value = provider
            .execute("test", &serde_json::json!({"index": 42}))
            .await?;
        assert_eq!(healthy["index"], 42);
        tokio::time::timeout(Duration::from_secs(3), async {
            while tokio::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stderr(std::process::Stdio::null())
                .status()
                .await?
                .success()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }

    fn test_provider() -> Result<(tempfile::TempDir, CommandSandboxProvider)> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("custom-provider");
        std::fs::write(
            &path,
            r#"#!/usr/bin/env node
const fs = require('node:fs');
const command = JSON.parse(fs.readFileSync(0, 'utf8'));
const request = command.request;
const reply = result => process.stdout.write(JSON.stringify({status: 'success', result}) + '\n');
if (request.barrier) {
  fs.writeFileSync(request.barrier + '/' + process.pid + '.started', '');
  const timer = setInterval(() => {
    if (fs.readdirSync(request.barrier).filter(name => name.endsWith('.started')).length === 5) {
      clearInterval(timer);
      reply({pid: process.pid, index: request.index});
    }
  }, 5);
} else if (request.fail) {
  process.stdout.write(JSON.stringify({status: 'failure', error: 'test failure'}) + '\n');
} else if (request.oversized) {
  process.stdout.write('x'.repeat(1024 * 1024 + 1));
} else if (request.malformed) {
  process.stdout.write('not json\n');
} else if (request.exit) {
  process.exitCode = 1;
} else {
  if (request.marker) fs.writeFileSync(request.marker, String(process.pid));
  setTimeout(() => reply({pid: process.pid, index: request.index}), request.delay ?? 0);
}
"#,
        )?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
        let provider = CommandSandboxProvider::new(
            "modal".into(),
            path.display().to_string(),
            HashMap::new(),
        )?;
        Ok((directory, provider))
    }
}
