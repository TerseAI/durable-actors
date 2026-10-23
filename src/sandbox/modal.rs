use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use ::modal::{
    App, AppLookupParams, Client, Image, Probe, Sandbox, SandboxCreateParams, SandboxListParams,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    remote::{RemoteSandboxProvider, SandboxCluster},
    secrets::SecretResolver,
    *,
};

mod images;
mod snapshots;
use futures_util::{StreamExt, TryStreamExt};
use moka::future::Cache;

const READY_FILE: &str = "/tmp/durable-actors-spare-ready";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Debug)]
pub(crate) struct ModalConfig {
    pub app_name: String,
    pub regions: BTreeMap<String, String>,
    pub timeout: Duration,
    pub registry_secrets: BTreeMap<String, String>,
}

pub(crate) async fn provider(
    config: ModalConfig,
    database: crate::postgres::PostgresDatabase,
    images: &[&str],
    secrets: Arc<dyn SecretResolver>,
) -> Result<RemoteSandboxProvider> {
    let client = Client::from_env()?;
    let app = client
        .app_from_name(
            &config.app_name,
            AppLookupParams {
                create_if_missing: true,
                ..Default::default()
            },
        )
        .await?;
    let imported = Arc::new(images::Images::new(Arc::new(images::ModalImages::new(
        client.clone(),
        app.clone(),
        config.clone(),
    ))));
    for reference in images {
        imported.image(reference).await?;
    }
    let snapshots = snapshots::Snapshots::new(
        app.app_id.clone(),
        Arc::new(snapshots::ModalSnapshots {
            client: client.clone(),
            app: app.clone(),
            config: config.clone(),
            images: imported.clone(),
            storage: Default::default(),
        }),
        Arc::new(snapshots::PostgresSnapshots(database)),
    );
    RemoteSandboxProvider::new(
        "modal",
        Arc::new(ModalCluster {
            client,
            app,
            config,
            images: imported,
            snapshots,
            sandboxes: Cache::builder()
                .max_capacity(10000)
                .time_to_idle(Duration::from_secs(3600))
                .build(),
        }),
        secrets,
    )
}

struct ModalCluster {
    client: Client,
    app: App,
    config: ModalConfig,
    images: Arc<images::Images>,
    snapshots: snapshots::Snapshots,
    sandboxes: Cache<String, Sandbox>,
}

#[async_trait]
impl SandboxCluster for ModalCluster {
    async fn prepare_registration(&self, deployment: &SandboxDeployment) -> Result<()> {
        self.snapshots.prepare(deployment).await?;
        Ok(())
    }

    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        let image = self.images.image(&request.image_ref).await?;
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let params = spare_params(request, &self.config, &token)?;
        let sandbox = tokio::time::timeout(
            STARTUP_TIMEOUT,
            self.client.create_sandbox(&self.app, &image, params),
        )
        .await
        .context("Modal sandbox creation timed out")??;
        let mut cleanup = SandboxCleanup(Some(sandbox.clone()));
        let handle = tokio::time::timeout(STARTUP_TIMEOUT, ready_handle(&sandbox, request, token))
            .await
            .context("Modal spare readiness timed out")??;
        self.sandboxes
            .insert(handle.resource_id.clone(), sandbox)
            .await;
        cleanup.0 = None;
        Ok(handle)
    }

    async fn prepare_spares(&self, spares: &[SpareHandle]) -> Result<()> {
        futures_util::stream::iter(spares.to_vec())
            .map(|spare: SpareHandle| async move {
                if self.sandboxes.contains_key(&spare.resource_id) {
                    return Ok(());
                }
                let sandbox = self.client.sandbox_from_id(&spare.resource_id);
                // A claim may race this refresh; only cache successfully prepared connections.
                if sandbox
                    .wait_until_ready(Duration::from_secs(2))
                    .await
                    .is_ok()
                {
                    self.sandboxes
                        .insert(spare.resource_id.clone(), sandbox)
                        .await;
                }
                anyhow::Ok(())
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    async fn prepare_assignment(
        &self,
        request: &EnsureHostRequest,
        spare: &SpareHandle,
    ) -> Result<bool> {
        if request.code_snapshot.is_none() {
            return Ok(false);
        }
        let deployment = SandboxDeployment {
            project_id: request
                .actor
                .as_ref()
                .context("actor identity required")?
                .project_id
                .clone(),
            image_ref: request.image_ref.clone(),
            canonical_region: request.canonical_region.clone(),
            code_snapshot: request.code_snapshot.clone(),
        };
        let image = match self.snapshots.get(&deployment).await {
            Ok(Some(image)) => image,
            Ok(None) => return Ok(false),
            Err(error) => {
                tracing::warn!(%error, "snapshot unavailable; using verified GCS code delivery");
                return Ok(false);
            }
        };
        let sandbox = self
            .sandboxes
            .get(&spare.resource_id)
            .await
            .unwrap_or_else(|| self.client.sandbox_from_id(&spare.resource_id));
        match sandbox.mount_image("/customer", &image).await {
            Ok(()) => Ok(true),
            Err(::modal::Error::NotFound(_)) => {
                self.snapshots.invalidate(&deployment).await?;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        self.sandboxes.invalidate(&spare.resource_id).await;
        let sandbox = if spare.resource_id.is_empty() {
            match self
                .client
                .sandbox_from_name(&self.config.app_name, &spare.name, None)
                .await
            {
                Ok(sandbox) => sandbox,
                Err(::modal::Error::NotFound(_)) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        } else {
            self.client.sandbox_from_id(&spare.resource_id)
        };
        match sandbox.terminate().await {
            Ok(()) | Err(::modal::Error::NotFound(_)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        let running: HashSet<_> = self
            .client
            .list_sandboxes(SandboxListParams {
                app_id: Some(self.app.app_id.clone()),
                tags: HashMap::from([("managed-by".into(), "durable-actors".into())]),
                ..Default::default()
            })
            .await?
            .into_iter()
            .map(|sandbox| sandbox.sandbox_id)
            .collect();
        Ok(spares
            .iter()
            .filter(|spare| !running.contains(&spare.resource_id))
            .map(|spare| spare.resource_id.clone())
            .collect())
    }
}

fn spare_params(
    request: &CreateSpareRequest,
    config: &ModalConfig,
    token: &str,
) -> Result<SandboxCreateParams> {
    let region = config
        .regions
        .get(&request.canonical_region)
        .context("no Modal placement for actor region")?;
    let control_plane = request
        .control_plane_url
        .as_ref()
        .context("spare control-plane URL required")?;
    Ok(SandboxCreateParams {
        name: Some(request.name.clone()),
        command: vec!["/usr/local/bin/durable-actors".into()],
        env: HashMap::from([
            ("DURABLE_ACTORS_PROCESS_ROLE".into(), "spare".into()),
            ("DURABLE_ACTORS_SPARE_TOKEN".into(), token.into()),
            (
                "DURABLE_ACTORS_CONTROL_PLANE_URL".into(),
                control_plane.clone(),
            ),
            ("DURABLE_ACTORS_SPARE_READY_FILE".into(), READY_FILE.into()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
        ]),
        cpu: Some(f64::from(request.resources.cpu_millis) / 1000.0),
        cpu_limit: request
            .resources
            .cpu_limit_millis
            .map(|cpu| f64::from(cpu) / 1000.0),
        memory_mib: Some(request.resources.memory_mib),
        memory_limit_mib: request.resources.memory_limit_mib,
        timeout: Some(config.timeout),
        cloud: Some("gcp".into()),
        regions: vec![region.clone()],
        h2_ports: vec![7101, 7102],
        readiness_probe: Some(
            Probe::exec(["/usr/bin/test", "-f", READY_FILE]).interval(Duration::from_millis(250)),
        ),
        tags: HashMap::from([("managed-by".into(), "durable-actors".into())]),
        ..Default::default()
    })
}

async fn ready_handle(
    sandbox: &Sandbox,
    request: &CreateSpareRequest,
    token: String,
) -> Result<SpareHandle> {
    let (_, tunnels) = tokio::try_join!(
        sandbox.wait_until_ready(STARTUP_TIMEOUT),
        sandbox.tunnels(STARTUP_TIMEOUT)
    )?;
    Ok(SpareHandle {
        name: request.name.clone(),
        resource_id: sandbox.id().into(),
        route: tunnels
            .get(&7101)
            .context("Modal invocation tunnel missing")?
            .url(),
        control_route: tunnels
            .get(&7102)
            .context("Modal assignment tunnel missing")?
            .url(),
        control_token: token,
        canonical_region: request.canonical_region.clone(),
    })
}

struct SandboxCleanup(Option<Sandbox>);

impl Drop for SandboxCleanup {
    fn drop(&mut self) {
        if let Some(sandbox) = self.0.take() {
            tokio::spawn(async move {
                let result =
                    tokio::time::timeout(Duration::from_secs(30), sandbox.terminate()).await;
                if !matches!(result, Ok(Ok(()))) {
                    tracing::warn!(
                        resource = sandbox.id(),
                        ?result,
                        "failed to retire Modal sandbox after startup failure"
                    );
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/modal.rs"]
mod tests;
