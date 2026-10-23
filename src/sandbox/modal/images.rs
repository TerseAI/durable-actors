use super::*;
use ::modal::Secret;
use moka::future::Cache;

pub(super) struct Images {
    backend: Arc<dyn ImageBackend>,
    imported: Cache<String, Image>,
}

impl Images {
    pub fn new(backend: Arc<dyn ImageBackend>) -> Self {
        Self {
            backend,
            imported: Cache::builder()
                .max_capacity(1024)
                .time_to_idle(Duration::from_secs(86400))
                .build(),
        }
    }

    pub async fn image(&self, reference: &str) -> Result<Image> {
        validate_image(reference)?;
        self.imported
            .try_get_with(reference.to_owned(), self.backend.import(reference))
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }
}

#[async_trait]
pub(super) trait ImageBackend: Send + Sync {
    async fn import(&self, reference: &str) -> Result<Image>;
}

pub(super) struct ModalImages {
    client: Client,
    app: App,
    registry_secrets: BTreeMap<String, Secret>,
}

impl ModalImages {
    pub fn new(client: Client, app: App, config: ModalConfig) -> Self {
        Self {
            client,
            app,
            registry_secrets: config
                .registry_secrets
                .into_iter()
                .map(|(registry, name)| (registry, Secret::from_name(name)))
                .collect(),
        }
    }
}

#[async_trait]
impl ImageBackend for ModalImages {
    async fn import(&self, reference: &str) -> Result<Image> {
        let registry = reference.split('/').next().unwrap_or_default();
        let image = match self.registry_secrets.get(registry) {
            Some(secret) => Image::from_private_registry(reference, secret.clone()),
            None => Image::from_registry(reference),
        }
        .dockerfile_commands(["ENTRYPOINT []"]);
        tokio::time::timeout(
            Duration::from_secs(600),
            image.build(&self.client, &self.app),
        )
        .await
        .context("Modal image import timed out")??;
        Ok(image)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/modal_images.rs"]
mod tests;
