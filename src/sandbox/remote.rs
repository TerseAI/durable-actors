use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio_util::task::AbortOnDropHandle;

use super::{bootstrap::HostBootstrap, *};
use crate::bucket::ActivationHandoff;

pub(crate) struct RemoteSandboxProvider {
    name: &'static str,
    cluster: Arc<dyn SandboxCluster>,
    assignment: Arc<dyn HostAssignment>,
    bootstrap: Arc<dyn HostBootstrap>,
}

impl RemoteSandboxProvider {
    pub(super) fn new(
        name: &'static str,
        cluster: Arc<dyn SandboxCluster>,
        bootstrap: Arc<dyn HostBootstrap>,
    ) -> Result<Self> {
        Ok(Self {
            name,
            cluster,
            bootstrap,
            assignment: Arc::new(HttpAssignment(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(120))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
            )),
        })
    }
}

#[async_trait]
impl SandboxProvider for RemoteSandboxProvider {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        validate_image(&request.image_ref)?;
        let spare = self.cluster.allocate_spare(request).await?;
        let mut guard = StartupGuard::new(self.cluster.clone(), spare.clone(), None);
        match self.cluster.wait_ready(&spare).await {
            Ok(()) => {
                guard.disarm();
                Ok(spare)
            }
            Err(error) => {
                guard.cleanup().await;
                Err(error)
            }
        }
    }

    async fn retire_spare(&self, request: &SpareHandle) -> Result<()> {
        self.cluster.retire_spare(request).await
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        self.cluster.stopped_spares(spares).await
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let bootstrap = self.bootstrap.clone();
        let configuration = request.clone();
        let credentials = AbortOnDropHandle::new(tokio::spawn(async move {
            let (result, elapsed) = timed(bootstrap.credentials(&configuration)).await;
            (result.map(Some), elapsed)
        }));
        self.ensure_host_with_credentials(request, credentials)
            .await
    }

    async fn ensure_host_with_credentials(
        &self,
        request: &EnsureHostRequest,
        credentials: HostCredentials,
    ) -> Result<ActorHostHandle> {
        let started_at_ms = now_ms()?;
        let spare = self.allocate(request).await?;
        let mut guard = StartupGuard::new(
            self.cluster.clone(),
            spare.clone(),
            Some((self.bootstrap.clone(), request.clone())),
        );
        let result = self.launch(request, &spare, credentials, &mut guard).await;
        match result {
            Ok(mut handle) => {
                let completed_at_ms = now_ms()?;
                handle.provisioning = Some(ActorHostProvisioning {
                    provider: self.name.into(),
                    resource_id: spare.resource_id,
                    reused: request.spare.is_some(),
                    started_at_ms,
                    completed_at_ms,
                    input_parsed_at_ms: None,
                    sdk_loaded_at_ms: None,
                    resources_resolved_at_ms: None,
                    sandbox_scheduled_at_ms: None,
                    host_ready_observed_at_ms: Some(completed_at_ms),
                    route_read_at_ms: None,
                    command_spawned_at_ms: None,
                    request_written_at_ms: None,
                    process_completed_at_ms: None,
                    response_decoded_at_ms: None,
                });
                guard.disarm();
                Ok(handle)
            }
            Err(error) => {
                guard.cleanup().await;
                Err(error)
            }
        }
    }

    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("remote hosts must be retired through the durable spare registry")
    }
}

impl RemoteSandboxProvider {
    async fn allocate(&self, request: &EnsureHostRequest) -> Result<SpareHandle> {
        if let Some(spare) = &request.spare {
            return Ok(spare.clone());
        }
        validate_image(&request.image_ref)?;
        self.cluster
            .allocate_spare(&CreateSpareRequest {
                control_plane_url: Some(request.control_plane_url.clone()),
                kind: SpareKind::Actor,
                name: format!("do-actor-{}", request.session_id),
                image_ref: request.image_ref.clone(),
                canonical_region: request.canonical_region.clone(),
                resources: request.resources.clone(),
            })
            .await
    }

    async fn launch(
        &self,
        request: &EnsureHostRequest,
        spare: &SpareHandle,
        credentials: HostCredentials,
        guard: &mut StartupGuard,
    ) -> Result<ActorHostHandle> {
        ensure!(
            spare.canonical_region == request.canonical_region,
            "spare region mismatch"
        );
        let setup = async {
            let ready = async {
                if request.spare.is_none() {
                    self.cluster.wait_ready(spare).await?;
                }
                anyhow::Ok(())
            };
            let (credentials, secrets, (ready, readiness_ms)) = tokio::join!(
                credentials,
                self.cluster.secrets(&request.secret_refs),
                timed(ready)
            );
            let (credentials, credentials_ms) = credentials?;
            ready?;
            Ok((
                credentials?.context("host storage configuration required")?,
                secrets?,
                credentials_ms,
                readiness_ms,
            ))
        };
        let (mut handoff, (runtime_config, secrets, credentials_ms, readiness_ms), ownership_ms) =
            self.prepare(request, spare, setup).await?;
        if handoff.lease().expires_at_ms <= now_ms()?.saturating_add(15_000) {
            handoff = self.bootstrap.renew(request, &spare.route).await?;
        }
        ensure!(
            handoff.lease().expires_at_ms > now_ms()?.saturating_add(15_000),
            "activation handoff has insufficient lease time"
        );
        let mut request = request.clone();
        request.runtime_config = Some(runtime_config);
        let mut environment = assignment_environment(&request, spare, secrets)?;
        environment.insert(
            "DURABLE_ACTORS_ACTIVATION_HANDOFF".into(),
            serde_json::to_string(&handoff)?,
        );
        guard.assigned = true;
        let (handle, assignment_ms) = timed(self.assignment.assign(spare, environment)).await;
        let handle = handle?;
        validate_assignment(&request, spare, &handoff, &handle)?;
        tracing::info!(event = "remote_host_bootstrap", provider = self.name,
            host_id = %request.host_id, credentials_ms, readiness_ms, ownership_ms, assignment_ms,
            reused = request.spare.is_some(), "remote host bootstrap completed");
        Ok(handle)
    }

    async fn prepare<T>(
        &self,
        request: &EnsureHostRequest,
        spare: &SpareHandle,
        setup: impl std::future::Future<Output = Result<T>>,
    ) -> Result<(ActivationHandoff, T, f64)> {
        let claim = timed(self.bootstrap.claim(request, &spare.route));
        tokio::pin!(claim, setup);
        let (mut handoff, ownership_ms) = tokio::select! {
            (handoff, elapsed) = &mut claim => (handoff?, elapsed),
            result = &mut setup => {
                let (handoff, elapsed) = claim.await;
                return Ok((handoff?, result?, elapsed));
            }
        };
        loop {
            tokio::select! {
                result = &mut setup => {
                    let result = result?;
                    return Ok((handoff, result, ownership_ms));
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    handoff = self.bootstrap.renew(request, &spare.route).await?;
                }
            }
        }
    }
}

fn validate_assignment(
    request: &EnsureHostRequest,
    spare: &SpareHandle,
    handoff: &ActivationHandoff,
    handle: &ActorHostHandle,
) -> Result<()> {
    let lease = handle
        .lease
        .as_ref()
        .context("assigned host did not acquire ownership")?;
    ensure!(
        handle.host_id == request.host_id
            && lease.id == request.host_id
            && lease.session_id == request.session_id
            && lease.expires_at_ms > now_ms()?
            && handle.owner_epoch == handoff.owner_epoch()
            && handle.route == spare.route
            && lease.route == spare.route
            && handle.canonical_region == request.canonical_region,
        "assigned host identity mismatch"
    );
    Ok(())
}

struct StartupGuard {
    cluster: Arc<dyn SandboxCluster>,
    spare: Option<SpareHandle>,
    bootstrap: Option<(Arc<dyn HostBootstrap>, EnsureHostRequest)>,
    assigned: bool,
}

impl StartupGuard {
    fn new(
        cluster: Arc<dyn SandboxCluster>,
        spare: SpareHandle,
        bootstrap: Option<(Arc<dyn HostBootstrap>, EnsureHostRequest)>,
    ) -> Self {
        Self {
            cluster,
            spare: Some(spare),
            bootstrap,
            assigned: false,
        }
    }

    fn disarm(&mut self) {
        self.spare = None;
    }

    async fn cleanup(&mut self) {
        let Some(spare) = self.spare.take() else {
            return;
        };
        let _ = tokio::spawn(cleanup(
            self.cluster.clone(),
            spare,
            self.bootstrap.take(),
            self.assigned,
        ))
        .await;
    }
}

impl Drop for StartupGuard {
    fn drop(&mut self) {
        let Some(spare) = self.spare.take() else {
            return;
        };
        tokio::spawn(cleanup(
            self.cluster.clone(),
            spare,
            self.bootstrap.take(),
            self.assigned,
        ));
    }
}

async fn cleanup(
    cluster: Arc<dyn SandboxCluster>,
    spare: SpareHandle,
    bootstrap: Option<(Arc<dyn HostBootstrap>, EnsureHostRequest)>,
    assigned: bool,
) {
    let result = tokio::time::timeout(Duration::from_secs(30), cluster.retire_spare(&spare)).await;
    if !matches!(result, Ok(Ok(()))) {
        tracing::warn!(?result, resource = %spare.resource_id, "failed to retire starting sandbox");
    }
    // An ambiguous assignment may have started a writer; let its lease expire.
    if !assigned && let Some((bootstrap, request)) = bootstrap {
        let result =
            tokio::time::timeout(Duration::from_secs(30), bootstrap.release(&request)).await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::warn!(?result, host_id = %request.host_id, "failed to release unassigned activation");
        }
    }
}

async fn timed<T>(future: impl std::future::Future<Output = T>) -> (T, f64) {
    let started = Instant::now();
    let result = future.await;
    (result, started.elapsed().as_secs_f64() * 1000.0)
}

fn assignment_environment(
    request: &EnsureHostRequest,
    spare: &SpareHandle,
    secrets: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let artifact = request
        .code_snapshot
        .as_deref()
        .context("compiled GCS code artifact required")?;
    let manifest = crate::bundle::ActorBundle::decode(artifact)?;
    let actor = request.actor.as_ref().context("actor identity required")?;
    actor.validate()?;
    let mut environment = HashMap::from([
        ("DURABLE_ACTORS_PROCESS_ROLE".into(), "host".into()),
        (
            "DURABLE_ACTORS_HOST_TOKEN".into(),
            request.host_token.clone(),
        ),
        (
            "DURABLE_ACTORS_JWT_PUBLIC_KEYS".into(),
            request.jwt_public_keys.clone(),
        ),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL".into(),
            request.control_plane_url.clone(),
        ),
        (
            "DURABLE_ACTORS_JWT_ISSUER".into(),
            request.jwt_issuer.clone(),
        ),
        (
            "DURABLE_ACTORS_INVOKE_JWT_AUDIENCE".into(),
            request.invocation_jwt_audience.clone(),
        ),
        ("DURABLE_ACTORS_HOST_ID".into(), request.host_id.to_string()),
        (
            "DURABLE_ACTORS_SESSION_ID".into(),
            request.session_id.clone(),
        ),
        (
            "DURABLE_ACTORS_REGION".into(),
            request.canonical_region.clone(),
        ),
        ("DURABLE_ACTORS_HOST_ROUTE".into(), spare.route.clone()),
        ("DURABLE_ACTORS_HOST_BIND".into(), "0.0.0.0:7101".into()),
        (
            "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS".into(),
            request.host_idle_timeout_ms.to_string(),
        ),
        (
            "DURABLE_ACTORS_EXECUTOR_SOCKET".into(),
            "/tmp/durable-actors-executor.sock".into(),
        ),
        (
            "DURABLE_ACTORS_ENTRYPOINT".into(),
            format!("/customer/{}", manifest.entrypoint()?),
        ),
        ("DURABLE_ACTORS_CODE_ARTIFACT".into(), artifact.into()),
        (
            "DURABLE_ACTORS_CUSTOMER_ENV".into(),
            serde_json::to_string(&secrets)?,
        ),
        ("DURABLE_ACTORS_ACTOR".into(), serde_json::to_string(actor)?),
        (
            "DURABLE_ACTORS_ACTOR_IS_NEW".into(),
            request.actor_is_new.to_string(),
        ),
        (
            "DURABLE_ACTORS_RUNTIME_CONFIG".into(),
            request
                .runtime_config
                .clone()
                .context("host storage configuration required")?,
        ),
    ]);
    if let Some(hint) = &request.owner_hint {
        environment.insert("DURABLE_ACTORS_OWNER_HINT".into(), hint.clone());
    }
    Ok(environment)
}

pub(crate) fn validate_image(image: &str) -> Result<()> {
    let (name, digest) = image
        .rsplit_once("@sha256:")
        .context("container image must be pinned to a sha256 digest")?;
    ensure!(
        !name.is_empty()
            && !name.chars().any(char::is_whitespace)
            && digest.len() == 64
            && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid container image digest"
    );
    Ok(())
}

fn now_ms() -> Result<u64> {
    crate::clock::Clock::now_ms(&crate::clock::SystemClock)
}

#[async_trait]
pub(super) trait SandboxCluster: Send + Sync {
    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>>;
    async fn allocate_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle>;
    async fn wait_ready(&self, spare: &SpareHandle) -> Result<()>;
    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()>;
    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>>;
}

#[async_trait]
trait HostAssignment: Send + Sync {
    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle>;
}

struct HttpAssignment(reqwest::Client);
#[async_trait]
impl HostAssignment for HttpAssignment {
    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        self.0
            .post(format!(
                "{}/assign",
                spare.control_route.trim_end_matches('/')
            ))
            .bearer_auth(&spare.control_token)
            .json(&environment)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("decode host assignment")
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/remote.rs"]
mod tests;
