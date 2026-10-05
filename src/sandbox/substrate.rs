use super::*;
use crate::{
    clock::{Clock, SystemClock},
    control_plane::ActorJwtIssuer,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

mod api;
mod assignment;
mod code;
mod launch;
pub(crate) use assignment::validate_image;
mod template;
use terse_substrate as proto;

#[derive(Clone)]
pub(crate) struct SubstrateConfig {
    pub endpoint: String,
    pub router: String,
    pub token_file: String,
    pub trust_bundle: String,
    pub atespace: String,
    pub worker_labels: BTreeMap<String, String>,
    pub regions: Vec<String>,
    pub snapshot_location: String,
    pub sandbox_config: String,
    pub secrets_namespace: String,
    pub egress_cidrs: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct SubstrateProvider {
    api: Arc<dyn SubstrateApi>,
    code: Arc<dyn code::CodeSource>,
    assignment: Arc<dyn HostAssignment>,
    bootstrap: Arc<dyn HostBootstrap>,
    config: SubstrateConfig,
    issuer: ActorJwtIssuer,
}

impl SubstrateProvider {
    pub async fn new(
        mut config: SubstrateConfig,
        issuer: ActorJwtIssuer,
        control_plane_url: &str,
        bootstrap: Arc<dyn HostBootstrap>,
    ) -> Result<Self> {
        let control = reqwest::Url::parse(control_plane_url)?;
        for address in tokio::net::lookup_host((
            control.host_str().context("control-plane host missing")?,
            control
                .port_or_known_default()
                .context("control-plane port missing")?,
        ))
        .await?
        {
            if address.is_ipv4() {
                config.egress_cidrs.push(format!("{}/32", address.ip()));
            }
        }
        let api = api::GrpcApi::connect(&config).await?;
        Ok(Self {
            api: Arc::new(api),
            code: Arc::new(code::GcsCodeSource(
                google_cloud_storage::client::Storage::builder()
                    .build()
                    .await?,
            )),
            config,
            issuer,
            bootstrap,
            assignment: Arc::new(HttpAssignment(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(120))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
            )),
        })
    }

    pub fn start(self: &Arc<Self>, stop: tokio_util::sync::CancellationToken) {
        let provider = self.clone();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_secs(2));
            let mut failures = HashMap::new();
            loop {
                tokio::select! { () = stop.cancelled() => return, _ = ticks.tick() => {} }
                if let Err(error) = provider.reap(&mut failures).await {
                    tracing::warn!(%error, "Substrate actor cleanup failed");
                }
            }
        });
    }

    async fn reap(&self, failures: &mut HashMap<String, u8>) -> Result<()> {
        let now = SystemClock.now_ms()? / 1000;
        let actors = self.api.actors(&self.config.atespace).await?;
        failures.retain(|uid, _| {
            actors
                .iter()
                .any(|a| a.metadata.as_ref().is_some_and(|m| &m.uid == uid))
        });
        let mut probes = stream::iter(actors)
            .map(|actor| self.probe_actor(actor))
            .buffer_unordered(16);
        while let Some(probe) = probes.next().await {
            let (actor, alive) = probe?;
            let Some(meta) = &actor.metadata else {
                continue;
            };
            if meta.name.starts_with("prep-") {
                if meta
                    .create_time
                    .as_ref()
                    .is_some_and(|t| now.saturating_sub(t.seconds as u64) > 600)
                {
                    self.api.delete(actor.clone(), Some(meta.version)).await?;
                }
                continue;
            }
            if !meta.name.starts_with("h-") {
                continue;
            }
            let state = actor.status.as_ref().map(|s| s.state());
            let abandoned = state == Some(proto::ActorState::Suspended)
                && meta
                    .create_time
                    .as_ref()
                    .is_some_and(|t| now.saturating_sub(t.seconds as u64) > 180);
            let exited = match alive {
                Some(false) => {
                    let count = failures.entry(meta.uid.clone()).or_default();
                    *count = count.saturating_add(1);
                    *count >= 3
                }
                _ => {
                    failures.remove(&meta.uid);
                    false
                }
            };
            if state == Some(proto::ActorState::Crashed) || abandoned || exited {
                let version = meta.version;
                self.api.delete(actor, Some(version)).await?;
            }
        }
        Ok(())
    }

    async fn probe_actor(&self, actor: proto::Actor) -> Result<(proto::Actor, Option<bool>)> {
        let running = actor
            .status
            .as_ref()
            .is_some_and(|s| s.state() == proto::ActorState::Running);
        let meta = actor.metadata.as_ref().filter(|m| m.name.starts_with("h-"));
        let alive = match (running, meta) {
            (true, Some(meta)) => Some(self.assignment.alive(&self.route(&meta.name)).await?),
            _ => None,
        };
        Ok((actor, alive))
    }

    async fn allocate(
        &self,
        request: &EnsureHostRequest,
        timings: &mut ProvisioningTimings,
    ) -> Result<proto::Actor> {
        let started = std::time::Instant::now();
        let template = self
            .api
            .template(template::build(
                &self.config,
                &runtime_template(request),
                &request.resources,
            )?)
            .await?;
        timings.template_ms = millis(started);
        let started = std::time::Instant::now();
        let metadata = template
            .metadata
            .as_ref()
            .context("template identity missing")?;
        let artifact = request
            .code_snapshot
            .as_deref()
            .context("code artifact required")?;
        let source_tag = code::code_tag(&template, artifact)?;
        let tag = self
            .api
            .tag(source_tag.clone())
            .await?
            .context("deployment code snapshot has not been prepared")?;
        proto::validate_tag(&tag, &metadata.uid)?;
        timings.code_tag_ms = millis(started);
        let started = std::time::Instant::now();
        let result = self
            .api
            .create(proto::Actor {
                metadata: Some(proto::ResourceMetadata {
                    atespace: self.config.atespace.clone(),
                    name: actor_name(&request.host_id)?,
                    ..Default::default()
                }),
                actor_template: Some(reference(&metadata.atespace, &metadata.name)),
                source_tag: Some(source_tag),
                ..Default::default()
            })
            .await;
        timings.create_ms = millis(started);
        result
    }

    async fn restore(
        &self,
        request: &EnsureHostRequest,
        actor: &proto::Actor,
        timings: &mut ProvisioningTimings,
    ) -> Result<HashMap<String, String>> {
        let meta = actor.metadata.as_ref().context("actor identity missing")?;
        let target = reference(&meta.atespace, &meta.name);
        let started = std::time::Instant::now();
        self.api
            .egress(
                target.clone(),
                vec![proto::EgressRule {
                    cidrs: Some(proto::CidrRule {
                        cidrs: self.config.egress_cidrs.clone(),
                    }),
                    ..Default::default()
                }],
            )
            .await?;
        timings.egress_ms = millis(started);
        let started = std::time::Instant::now();
        let secrets = self.api.secrets(&request.secret_refs).await?;
        timings.secrets_ms = millis(started);
        let started = std::time::Instant::now();
        self.api.resume(target).await?;
        timings.resume_ms = millis(started);
        Ok(secrets)
    }

    async fn assign(
        &self,
        request: &EnsureHostRequest,
        actor: &proto::Actor,
        secrets: HashMap<String, String>,
        handoff: &crate::bucket::ActivationHandoff,
        timings: &mut ProvisioningTimings,
    ) -> Result<ActorHostHandle> {
        let meta = actor.metadata.as_ref().context("actor identity missing")?;
        let started = std::time::Instant::now();
        let route = self.route(&meta.name);
        let mut env = assignment::environment(request, &route, secrets)?;
        env.insert(
            "DURABLE_ACTORS_ACTIVATION_HANDOFF".into(),
            serde_json::to_string(handoff)?,
        );
        let handle = self
            .assignment
            .assign(&route, &self.issuer.issue_assignment(&meta.uid)?, env)
            .await?;
        timings.assign_ms = millis(started);
        let lease = handle
            .lease
            .as_ref()
            .context("assigned host did not acquire ownership")?;
        ensure!(
            handle.host_id == request.host_id
                && lease.id == request.host_id
                && lease.session_id == request.session_id
                && lease.expires_at_ms > SystemClock.now_ms()?
                && handle.owner_epoch > 0
                && handle.route == route
                && lease.route == route
                && handle.canonical_region == request.canonical_region,
            "assigned host identity mismatch"
        );
        Ok(handle)
    }

    fn route(&self, name: &str) -> String {
        format!(
            "{}/substrate/{}/{name}",
            self.config.router.trim_end_matches('/'),
            self.config.atespace
        )
    }
}

#[async_trait]
impl SandboxProvider for SubstrateProvider {
    fn bootstraps_storage(&self) -> bool {
        true
    }

    async fn prepare_runtime(&self, request: &RuntimeTemplateRequest) -> Result<()> {
        ensure!(
            self.config.regions.contains(&request.canonical_region),
            "Substrate region is not configured"
        );
        assignment::validate_image(&request.image_ref)?;
        for resources in &request.resources {
            let template = self
                .api
                .template(template::build(&self.config, request, resources)?)
                .await?;
            self.prepare_code(
                &template,
                request
                    .code_snapshot
                    .as_deref()
                    .context("code artifact required")?,
            )
            .await?;
        }
        Ok(())
    }

    async fn wait_ready(&self, host: &HostId) -> Result<()> {
        let route = self.route(&actor_name(host)?);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        let result = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Ok(response) = super::transport::host_request(&client, &route, "/readyz")?
                    .send()
                    .await
                    && response.status().is_success()
                {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        result.unwrap_or_else(|_| Err(HostNotReady.into()))
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        ensure!(
            self.config.regions.contains(&request.canonical_region),
            "Substrate region is not configured"
        );
        assignment::validate_image(&request.image_ref)?;
        let provider = self.clone();
        let request = request.clone();
        // Complete allocation or cleanup even if the requesting client disconnects.
        tokio::spawn(async move { provider.launch(request).await }).await?
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        let prefix = format!("h-{}-", &digest(&request.host_config_key)[..30]);
        let mut resource_ids = Vec::new();
        for actor in self.api.actors(&self.config.atespace).await? {
            let meta = actor.metadata.as_ref().context("actor identity missing")?;
            if meta.name.starts_with(&prefix) {
                resource_ids.push(format!("{}/{}/{}", meta.atespace, meta.name, meta.uid));
                self.api.delete(actor, None).await?;
            }
        }
        Ok(HostTermination {
            provider: "substrate".into(),
            resource_ids,
        })
    }
}

fn runtime_template(request: &EnsureHostRequest) -> RuntimeTemplateRequest {
    RuntimeTemplateRequest {
        code_snapshot: request.code_snapshot.clone(),
        image_ref: request.image_ref.clone(),
        canonical_region: request.canonical_region.clone(),
        resources: vec![request.resources.clone()],
        jwt_public_keys: request.jwt_public_keys.clone(),
        jwt_issuer: request.jwt_issuer.clone(),
    }
}

fn actor_name(host: &HostId) -> Result<String> {
    let (key, _) = host
        .as_str()
        .strip_prefix("host.v3.")
        .context("invalid host identity")?
        .rsplit_once('.')
        .context("host session identity missing")?;
    Ok(format!(
        "h-{}-{}",
        &digest(key)[..30],
        &digest(host.as_str())[..30]
    ))
}

fn digest(value: &str) -> String {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, value.as_bytes()).as_ref()[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn reference(atespace: &str, name: &str) -> proto::ObjectRef {
    proto::ObjectRef {
        atespace: atespace.into(),
        name: name.into(),
    }
}

#[async_trait]
trait SubstrateApi: Send + Sync {
    async fn template(&self, template: proto::ActorTemplate) -> Result<proto::ActorTemplate>;
    async fn tag(&self, tag: proto::ObjectRef) -> Result<Option<proto::Tag>>;
    async fn suspend(&self, actor: proto::ObjectRef) -> Result<()>;
    async fn create_tag(&self, tag: proto::Tag) -> Result<()>;
    async fn create(&self, actor: proto::Actor) -> Result<proto::Actor>;
    async fn egress(&self, actor: proto::ObjectRef, rules: Vec<proto::EgressRule>) -> Result<()>;
    async fn resume(&self, actor: proto::ObjectRef) -> Result<()>;
    async fn delete(&self, actor: proto::Actor, version: Option<i64>) -> Result<()>;
    async fn actors(&self, atespace: &str) -> Result<Vec<proto::Actor>>;
    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>>;
}

#[async_trait]
trait HostAssignment: Send + Sync {
    async fn alive(&self, route: &str) -> Result<bool>;
    async fn prepare_code(
        &self,
        route: &str,
        token: &str,
        artifact: &crate::artifacts::ArtifactFile,
        chunks: code::CodeStream,
    ) -> Result<()>;
    async fn assign(
        &self,
        route: &str,
        token: &str,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle>;
}
struct HttpAssignment(reqwest::Client);
#[async_trait]
impl HostAssignment for HttpAssignment {
    async fn prepare_code(
        &self,
        route: &str,
        token: &str,
        artifact: &crate::artifacts::ArtifactFile,
        chunks: code::CodeStream,
    ) -> Result<()> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        super::transport::host_request(&self.0, route, "/prepare-code")?
            .bearer_auth(token)
            .header("connection", "close")
            .header(
                "terse-code-artifact",
                URL_SAFE_NO_PAD.encode(serde_json::to_vec(artifact)?),
            )
            .body(reqwest::Body::wrap_stream(chunks))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(())
    }

    async fn alive(&self, route: &str) -> Result<bool> {
        let mut request = super::transport::host_request(&self.0, route, "/warmz")?
            .timeout(Duration::from_secs(2))
            .build()?;
        *request.method_mut() = reqwest::Method::GET;
        Ok(self
            .0
            .execute(request)
            .await
            .is_ok_and(|r| r.status().is_success()))
    }

    async fn assign(
        &self,
        route: &str,
        token: &str,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        super::transport::host_request(&self.0, route, "/assign")?
            .bearer_auth(token)
            .json(&environment)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("decode host assignment")
    }
}

#[derive(Default, serde::Serialize)]
struct ProvisioningTimings {
    credentials_ms: f64,
    ownership_ms: f64,
    ready_to_assign_ms: f64,
    template_ms: f64,
    code_tag_ms: f64,
    create_ms: f64,
    egress_ms: f64,
    secrets_ms: f64,
    resume_ms: f64,
    assign_ms: f64,
}

fn millis(started: std::time::Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/substrate.rs"]
mod tests;

#[async_trait]
pub(crate) trait HostBootstrap: Send + Sync {
    async fn credentials(&self, request: &EnsureHostRequest) -> Result<String>;
    async fn claim(
        &self,
        request: &EnsureHostRequest,
        route: &str,
    ) -> Result<crate::bucket::ActivationHandoff>;
    async fn release(&self, request: &EnsureHostRequest) -> Result<()>;
}
