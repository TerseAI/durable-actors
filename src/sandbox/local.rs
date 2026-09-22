use std::{
    collections::HashMap,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};

use crate::{
    clock::{Clock, SystemClock},
    placement::ObjectPlacementStore,
};

use super::{
    ActorHostHandle, EnsureHostRequest, HostTermination, SandboxProvider, TerminateHostsRequest,
};

pub(crate) struct LocalSandboxProvider {
    executable: PathBuf,
    project: PathBuf,
    sdk_host: Option<PathBuf>,
    placements: Arc<dyn ObjectPlacementStore>,
    hosts: Mutex<HashMap<String, LocalHost>>,
    stopping: AtomicBool,
}

impl LocalSandboxProvider {
    pub(crate) fn new(
        executable: PathBuf,
        project: PathBuf,
        placements: Arc<dyn ObjectPlacementStore>,
        sdk_host: Option<PathBuf>,
    ) -> Self {
        Self {
            executable,
            project,
            sdk_host,
            placements,
            hosts: Mutex::new(HashMap::new()),
            stopping: AtomicBool::new(false),
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let mut hosts = self.hosts.lock().await;
        for (_, host) in hosts.drain() {
            host.stop().await;
        }
    }

    async fn launch(&self, request: &EnsureHostRequest) -> Result<LocalHost> {
        let directory = tempfile::Builder::new().prefix("ldo-").tempdir_in("/tmp")?;
        let mut environment = host_environment(request, &directory);
        if let Some(module) = &self.sdk_host {
            environment.insert(
                "DURABLE_OBJECT_SDK_HOST".into(),
                module.display().to_string(),
            );
        }
        let child = Command::new(&self.executable)
            .current_dir(&self.project)
            .env_clear()
            .envs(environment)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("start local actor host")?;
        let mut host = LocalHost {
            child,
            directory,
            host_id: request.host_id.clone(),
            owner_epoch: 0,
            actor: request
                .actor
                .clone()
                .context("local actor identity missing")?,
            lease: None,
        };
        let ready =
            tokio::time::timeout(Duration::from_secs(30), self.wait_until_ready(&mut host)).await;
        match ready {
            Ok(Ok(())) => Ok(host),
            error => {
                host.stop().await;
                match error {
                    Ok(Err(error)) => Err(error),
                    _ => anyhow::bail!("local actor host did not become ready within 30 seconds"),
                }
            }
        }
    }

    async fn active_lease(
        &self,
        host: &LocalHost,
    ) -> Result<Option<crate::host_leases::HostLease>> {
        let placement = self.placements.get_owner(&host.actor.storage_key()).await?;
        Ok(placement.map(|placement| placement.lease).filter(|lease| {
            lease.id == host.host_id
                && lease.expires_at_ms > SystemClock.now_ms().unwrap_or(u64::MAX)
        }))
    }

    async fn wait_until_ready(&self, host: &mut LocalHost) -> Result<()> {
        loop {
            if let Some(status) = host.child.try_wait()? {
                anyhow::bail!("local actor host exited with {status}; check its logs above");
            }
            if host.directory.path().join("ready").exists() {
                let ready: serde_json::Value = serde_json::from_slice(
                    &tokio::fs::read(host.directory.path().join("ready")).await?,
                )?;
                host.owner_epoch = ready["ownerEpoch"]
                    .as_u64()
                    .context("host readiness omitted ownership epoch")?;
                ensure!(
                    host.owner_epoch > 0,
                    "host readiness returned no ownership epoch"
                );
                host.lease = Some(serde_json::from_value(ready["lease"].clone())?);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[async_trait]
impl SandboxProvider for LocalSandboxProvider {
    async fn build_code(&self, _: &super::BuildCodeRequest) -> Result<super::BuiltActorCode> {
        anyhow::bail!("local deployments load actor source directly")
    }
    async fn wait_ready(&self, host: &crate::host::HostId) -> Result<()> {
        let hosts = self.hosts.lock().await;
        ensure!(
            hosts.values().any(|running| running.host_id == *host
                && running.directory.path().join("ready").exists()),
            "local actor is not ready"
        );
        Ok(())
    }

    async fn socket_credentials(
        &self,
        request: &super::SocketCredentialsRequest,
    ) -> Result<super::SocketCredentials> {
        let hosts = self.hosts.lock().await;
        let host = hosts
            .values()
            .find(|host| host.host_id == request.host_id)
            .context("local host missing")?;
        let lease = self
            .active_lease(host)
            .await?
            .context("socket host lease expired")?;
        ensure!(
            lease.session_id == request.session_id,
            "socket host session replaced"
        );
        Ok(super::SocketCredentials {
            url: lease.route,
            token: String::new(),
        })
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        ensure!(
            PathBuf::from(&request.working_directory) == self.project,
            "local deployments must use the current project directory"
        );
        ensure!(
            request.secret_refs.is_empty(),
            "Modal secret references are unavailable in local mode"
        );
        let key = format!(
            "{}/{}/{}",
            request.host_config_key,
            request.canonical_region,
            request
                .actor
                .as_ref()
                .map(|actor| serde_json::to_string(actor).unwrap())
                .unwrap_or_default()
        );
        let mut hosts = self.hosts.lock().await;
        ensure!(
            !self.stopping.load(Ordering::SeqCst),
            "local runtime is shutting down"
        );
        if let Some(host) = hosts.get_mut(&key) {
            if host.child.try_wait()?.is_none()
                && let Some(lease) = self.active_lease(host).await?
            {
                return Ok(handle(lease, &request.canonical_region, host.owner_epoch));
            }
        }
        if let Some(host) = hosts.remove(&key) {
            host.stop().await;
        }
        let host = self.launch(request).await?;
        let lease = host
            .lease
            .clone()
            .context("local readiness omitted lease")?;
        let epoch = host.owner_epoch;
        hosts.insert(key, host);
        Ok(handle(lease, &request.canonical_region, epoch))
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        let prefix = format!("{}/", request.host_config_key);
        let mut hosts = self.hosts.lock().await;
        let keys = hosts
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        let mut resource_ids = Vec::new();
        for key in keys {
            if let Some(host) = hosts.remove(&key) {
                resource_ids.push(host.host_id.as_str().to_owned());
                host.stop().await;
            }
        }
        Ok(HostTermination {
            provider: "local".into(),
            resource_ids,
        })
    }
}

struct LocalHost {
    actor: crate::actor::ActorKey,
    lease: Option<crate::host_leases::HostLease>,
    child: Child,
    directory: TempDir,
    host_id: crate::host::HostId,
    owner_epoch: u64,
}

impl LocalHost {
    async fn stop(mut self) {
        drop(self.child.stdin.take());
        if tokio::time::timeout(Duration::from_secs(7), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.kill().await;
        }
    }
}

fn handle(lease: crate::host_leases::HostLease, region: &str, owner_epoch: u64) -> ActorHostHandle {
    ActorHostHandle {
        lease: Some(lease.clone()),
        owner_epoch,
        host_id: lease.id,
        route: lease.route,
        canonical_region: region.to_owned(),
        provisioning: None,
    }
}

fn host_environment(request: &EnsureHostRequest, directory: &TempDir) -> HashMap<String, String> {
    let mut environment = std::env::vars()
        .filter(|(key, _)| !key.starts_with("DURABLE_OBJECT_"))
        .collect::<HashMap<_, _>>();
    if let Some(actor) = &request.actor {
        environment.insert(
            "DURABLE_OBJECT_ACTOR".into(),
            serde_json::to_string(actor).expect("actor identity serializable"),
        );
    }
    environment.insert(
        "DURABLE_OBJECT_ACTOR_IS_NEW".into(),
        request.actor_is_new.to_string(),
    );
    for (key, value) in [
        ("DURABLE_OBJECT_PROCESS_ROLE", "host".to_owned()),
        ("DURABLE_OBJECT_LOG_MODE", "development".into()),
        ("DURABLE_OBJECT_PARENT_LIFETIME_STDIN", "1".into()),
        ("DURABLE_OBJECT_HOST_BIND", "127.0.0.1:0".into()),
        (
            "DURABLE_OBJECT_HOST_ID",
            request.host_id.as_str().to_owned(),
        ),
        ("DURABLE_OBJECT_SESSION_ID", request.session_id.clone()),
        ("DURABLE_OBJECT_HOST_TOKEN", request.host_token.clone()),
        (
            "DURABLE_OBJECT_JWT_PUBLIC_KEYS",
            request.jwt_public_keys.clone(),
        ),
        (
            "DURABLE_OBJECT_CONTROL_PLANE_URL",
            request.control_plane_url.clone(),
        ),
        ("DURABLE_OBJECT_JWT_ISSUER", request.jwt_issuer.clone()),
        (
            "DURABLE_OBJECT_SOCKET_JWT_AUDIENCE",
            request.socket_jwt_audience.clone(),
        ),
        (
            "DURABLE_OBJECT_INVOKE_JWT_AUDIENCE",
            request.invocation_jwt_audience.clone(),
        ),
        (
            "DURABLE_OBJECT_EXECUTOR_SOCKET",
            directory.path().join("executor.sock").display().to_string(),
        ),
        (
            "DURABLE_OBJECT_HOST_READY_FILE",
            directory.path().join("ready").display().to_string(),
        ),
        (
            "DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_SECONDS",
            request.actor_idle_timeout_seconds.to_string(),
        ),
        (
            "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS",
            request.host_idle_timeout_ms.to_string(),
        ),
    ] {
        environment.insert(key.into(), value);
    }
    if let Some(config) = &request.runtime_config {
        environment.insert("DURABLE_OBJECT_RUNTIME_CONFIG".into(), config.clone());
    }
    if let Some(entrypoint) = &request.actor_entrypoint {
        environment.insert("DURABLE_OBJECT_ENTRYPOINT".into(), entrypoint.clone());
    }
    environment
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/local.rs"]
mod tests;
