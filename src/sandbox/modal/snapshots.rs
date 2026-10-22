use super::*;
use crate::{bundle::ActorBundle, postgres::PostgresDatabase};
use ::modal::ExecParams;
use moka::future::Cache;
use tokio::io::AsyncReadExt;

const SNAPSHOT_TTL: Duration = Duration::from_secs(30 * 86400);

pub(super) struct Snapshots {
    backend: Arc<dyn SnapshotBackend>,
    store: Arc<dyn SnapshotStore>,
    cached: Cache<String, Image>,
    slots: tokio::sync::Semaphore,
    namespace: String,
}

impl Snapshots {
    pub fn new(
        namespace: String,
        backend: Arc<dyn SnapshotBackend>,
        store: Arc<dyn SnapshotStore>,
    ) -> Self {
        Self {
            namespace,
            backend,
            store,
            cached: Cache::builder()
                .max_capacity(1024)
                .time_to_live(Duration::from_secs(3600))
                .build(),
            slots: tokio::sync::Semaphore::new(4),
        }
    }

    pub async fn prepare(&self, deployment: &SandboxDeployment) -> Result<Image> {
        let key = snapshot_key(&self.namespace, deployment)?;
        self.cached
            .try_get_with(key.clone(), async {
                if let Some(id) = self.store.get(&key).await? {
                    return Ok(Image::from_id(id));
                }
                let _slot = self.slots.acquire().await?;
                let image = self.backend.create(deployment).await?;
                self.store
                    .put(
                        &key,
                        image.image_id().context("snapshot image is not built")?,
                    )
                    .await?;
                anyhow::Ok(image)
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }

    pub async fn invalidate(&self, deployment: &SandboxDeployment) -> Result<()> {
        let key = snapshot_key(&self.namespace, deployment)?;
        self.cached.invalidate(&key).await;
        self.store.remove(&key).await
    }
}

fn snapshot_key(namespace: &str, deployment: &SandboxDeployment) -> Result<String> {
    let code = deployment
        .code_snapshot
        .as_deref()
        .context("snapshot requires a code bundle")?;
    ActorBundle::decode(code)?;
    let bytes = serde_json::to_vec(&(
        namespace,
        &deployment.project_id,
        &deployment.image_ref,
        code,
    ))?;
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes).as_ref(),
    ))
}

#[async_trait]
pub(super) trait SnapshotBackend: Send + Sync {
    async fn create(&self, deployment: &SandboxDeployment) -> Result<Image>;
}

#[async_trait]
pub(super) trait SnapshotStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn put(&self, key: &str, image: &str) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
}

pub(super) struct PostgresSnapshots(pub PostgresDatabase);
#[async_trait]
impl SnapshotStore for PostgresSnapshots {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.0.query_opt("SELECT image_id FROM durable_actors_prepared_code WHERE cache_key = $1 AND expires_at > clock_timestamp()", &[&key]).await?.map(|row| row.get(0)))
    }
    async fn put(&self, key: &str, image: &str) -> Result<()> {
        // Retire the reference a day before Modal's hard expiry, including local cache time.
        self.0.execute("INSERT INTO durable_actors_prepared_code (cache_key, image_id, expires_at) VALUES ($1, $2, clock_timestamp() + interval '29 days') ON CONFLICT (cache_key) DO UPDATE SET image_id = EXCLUDED.image_id, expires_at = EXCLUDED.expires_at", &[&key, &image]).await?;
        self.0
            .execute(
                "DELETE FROM durable_actors_prepared_code WHERE expires_at <= clock_timestamp()",
                &[],
            )
            .await?;
        Ok(())
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.0
            .execute(
                "DELETE FROM durable_actors_prepared_code WHERE cache_key = $1",
                &[&key],
            )
            .await?;
        Ok(())
    }
}

pub(super) struct ModalSnapshots {
    pub client: Client,
    pub app: App,
    pub config: ModalConfig,
    pub images: Arc<images::Images>,
    pub storage: tokio::sync::OnceCell<google_cloud_storage::client::Storage>,
}

#[async_trait]
impl SnapshotBackend for ModalSnapshots {
    async fn create(&self, deployment: &SandboxDeployment) -> Result<Image> {
        let bundle = ActorBundle::decode(
            deployment
                .code_snapshot
                .as_deref()
                .context("snapshot requires a code bundle")?,
        )?;
        let image = self.images.image(&deployment.image_ref).await?;
        let root = tempfile::tempdir()?;
        let storage = self
            .storage
            .get_or_try_init(|| async {
                google_cloud_storage::client::Storage::builder()
                    .build()
                    .await
            })
            .await?;
        bundle.install(root.path(), storage).await?;
        let archive = archive(root.path(), &bundle)?;
        let region = self
            .config
            .regions
            .get(&deployment.canonical_region)
            .context("no Modal placement for deployment region")?;
        let sandbox = tokio::time::timeout(
            STARTUP_TIMEOUT,
            self.client.create_sandbox(
                &self.app,
                &image,
                SandboxCreateParams {
                    command: vec!["/bin/sleep".into(), "600".into()],
                    cpu: Some(0.5),
                    memory_mib: Some(512),
                    timeout: Some(Duration::from_secs(600)),
                    cloud: Some("gcp".into()),
                    regions: vec![region.clone()],
                    block_network: true,
                    tags: HashMap::from([
                        ("managed-by".into(), "durable-actors".into()),
                        ("purpose".into(), "code-snapshot".into()),
                    ]),
                    ..Default::default()
                },
            ),
        )
        .await
        .context("Modal staging sandbox creation timed out")??;
        let mut cleanup = SandboxCleanup(Some(sandbox.clone()));
        let snapshot = tokio::time::timeout(Duration::from_secs(180), stage(&sandbox, archive))
            .await
            .context("Modal code staging timed out")??;
        sandbox.terminate().await?;
        cleanup.0 = None;
        Ok(snapshot)
    }
}

fn archive(root: &std::path::Path, bundle: &ActorBundle) -> Result<tempfile::NamedTempFile> {
    let file = tempfile::NamedTempFile::new()?;
    let mut archive = tar::Builder::new(file.reopen()?);
    for file in &bundle.files {
        archive.append_path_with_name(root.join(&file.path), &file.path)?;
    }
    archive.finish()?;
    Ok(file)
}

async fn stage(sandbox: &Sandbox, archive: tempfile::NamedTempFile) -> Result<Image> {
    let mkdir = sandbox
        .exec(["/bin/mkdir", "-p", "/customer"], ExecParams::default())
        .await?;
    ensure!(
        mkdir.wait().await? == 0,
        "creating customer directory failed"
    );
    let mut extract = sandbox
        .exec(
            ["/bin/tar", "--no-same-owner", "-xf", "-", "-C", "/customer"],
            ExecParams::default(),
        )
        .await?;
    let mut archive = tokio::fs::File::from_std(archive.reopen()?);
    let mut chunk = vec![0; 1024 * 1024];
    loop {
        let size = archive.read(&mut chunk).await?;
        if size == 0 {
            break;
        }
        extract.stdin.write(chunk[..size].to_vec()).await?;
    }
    extract.stdin.close().await?;
    ensure!(extract.wait().await? == 0, "staging customer files failed");
    Ok(sandbox
        .snapshot_directory("/customer", Some(SNAPSHOT_TTL))
        .await?)
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/modal_snapshots.rs"]
mod tests;
