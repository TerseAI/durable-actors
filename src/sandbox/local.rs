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

use crate::host_leases::HostLeaseStore;

use super::{
    ActorHostHandle, EnsureHostRequest, HostTermination, ImageWarmup, SandboxProvider,
    TerminateHostsRequest, WarmImageRequest,
};

pub(crate) struct LocalSandboxProvider {
    executable: PathBuf,
    project: PathBuf,
    sdk_host: Option<PathBuf>,
    leases: Arc<dyn HostLeaseStore>,
    hosts: Mutex<HashMap<String, LocalHost>>,
    stopping: AtomicBool,
}

impl LocalSandboxProvider {
    pub(crate) fn new(
        executable: PathBuf,
        project: PathBuf,
        leases: Arc<dyn HostLeaseStore>,
        sdk_host: Option<PathBuf>,
    ) -> Self {
        Self {
            executable,
            project,
            sdk_host,
            leases,
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

    async fn wait_until_ready(&self, host: &mut LocalHost) -> Result<()> {
        loop {
            if let Some(status) = host.child.try_wait()? {
                anyhow::bail!("local actor host exited with {status}; check its logs above");
            }
            if host.directory.path().join("ready").exists()
                && self.leases.lease_status(&host.host_id).await?.is_active()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[async_trait]
impl SandboxProvider for LocalSandboxProvider {
    async fn socket_credentials(
        &self,
        request: &super::SocketCredentialsRequest,
    ) -> Result<super::SocketCredentials> {
        let status = self.leases.lease_status(&request.host_id).await?;
        ensure!(status.is_active(), "socket host lease expired");
        let lease = status.lease.context("socket host lease missing")?;
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
        let key = format!("{}/{}", request.code_revision, request.canonical_region);
        let mut hosts = self.hosts.lock().await;
        ensure!(
            !self.stopping.load(Ordering::SeqCst),
            "local runtime is shutting down"
        );
        if let Some(host) = hosts.get_mut(&key) {
            let status = self.leases.lease_status(&host.host_id).await?;
            if host.child.try_wait()?.is_none() && status.is_active() {
                return Ok(handle(
                    status.lease.context("active host lease missing")?,
                    &request.canonical_region,
                ));
            }
        }
        if let Some(host) = hosts.remove(&key) {
            host.stop().await;
        }
        let host = self.launch(request).await?;
        let lease = self
            .leases
            .lease_status(&host.host_id)
            .await?
            .lease
            .context("local host lease missing")?;
        hosts.insert(key, host);
        Ok(handle(lease, &request.canonical_region))
    }

    async fn warm_image(&self, _request: &WarmImageRequest) -> Result<ImageWarmup> {
        Ok(ImageWarmup {
            provider: "local".into(),
            resource_id: "local".into(),
            total_ms: 0,
        })
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        let prefix = format!("{}/", request.code_revision);
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
    child: Child,
    directory: TempDir,
    host_id: crate::host::HostId,
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

fn handle(lease: crate::host_leases::HostLease, region: &str) -> ActorHostHandle {
    ActorHostHandle {
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
mod tests {
    use super::*;
    use crate::{
        bucket::{BucketHostLeases, FileBucket},
        clock::SystemClock,
        host::HostId,
    };

    #[tokio::test]
    async fn shutdown_rejects_new_hosts_before_starting_a_process() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let project = directory.path().to_path_buf();
        let leases = Arc::new(BucketHostLeases::new(
            Arc::new(FileBucket::new(project.join("objects"))?),
            Arc::new(SystemClock),
        ));
        let provider = LocalSandboxProvider::new(
            project.join("unused-executable"),
            project.clone(),
            leases,
            None,
        );
        provider.shutdown().await;
        let request = EnsureHostRequest {
            runtime_config: None,

            code_revision: "local".into(),
            canonical_region: "north-america-east".into(),
            host_id: HostId::new("host"),
            session_id: "session".into(),
            host_token: "unused".into(),
            jwt_public_keys: "unused".into(),
            control_plane_url: "http://127.0.0.1:7100".into(),
            jwt_issuer: "local".into(),
            invocation_jwt_audience: "local".into(),
            socket_jwt_audience: "local:websocket".into(),
            image_ref: "local".into(),
            working_directory: project.display().to_string(),
            actor_entrypoint: None,
            secret_refs: vec![],
            actor_idle_timeout_seconds: 60,
            host_idle_timeout_ms: 300_000,
        };
        assert_eq!(
            host_environment(&request, &directory).get("DURABLE_OBJECT_LOG_MODE"),
            Some(&"development".to_owned())
        );
        assert_eq!(
            host_environment(&request, &directory).get("DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_SECONDS"),
            Some(&"60".to_owned())
        );
        let error = provider.ensure_host(&request).await.unwrap_err();
        assert!(error.to_string().contains("shutting down"), "{error:#}");
        Ok(())
    }
}
