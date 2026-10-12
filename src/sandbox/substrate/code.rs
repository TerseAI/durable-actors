use super::*;
use crate::artifacts::{ArtifactFile, ArtifactManifest};
use bytes::Bytes;
use futures_util::stream::BoxStream;
use google_cloud_storage::client::Storage;

pub(super) type CodeStream = BoxStream<'static, Result<Bytes>>;

#[async_trait]
pub(super) trait CodeSource: Send + Sync {
    async fn open(&self, bucket: &str, file: &ArtifactFile) -> Result<CodeStream>;
}

pub(super) struct GcsCodeSource(pub Storage);

#[async_trait]
impl CodeSource for GcsCodeSource {
    async fn open(&self, bucket: &str, file: &ArtifactFile) -> Result<CodeStream> {
        let response = self
            .0
            .read_object(format!("projects/_/buckets/{bucket}"), &file.object)
            .set_generation(file.generation)
            .send()
            .await?;
        Ok(stream::try_unfold(response, |mut response| async {
            match response.next().await {
                Some(chunk) => Ok(Some((chunk?, response))),
                None => anyhow::Ok(None),
            }
        })
        .boxed())
    }
}

impl SubstrateProvider {
    pub(super) async fn prepare_code(
        &self,
        template: &proto::ActorTemplate,
        artifact: &str,
    ) -> Result<()> {
        let meta = template
            .metadata
            .as_ref()
            .context("template identity missing")?;
        let manifest = ArtifactManifest::decode(artifact)?;
        let target = code_tag(template, artifact)?;
        if let Some(tag) = self.api.tag(target.clone()).await? {
            proto::validate_tag(&tag, &meta.uid)?;
            return Ok(());
        }
        let actor = self
            .api
            .create(proto::Actor {
                metadata: Some(proto::ResourceMetadata {
                    atespace: meta.atespace.clone(),
                    name: format!("prep-{}", uuid::Uuid::new_v4()),
                    ..Default::default()
                }),
                actor_template: Some(reference(&meta.atespace, &meta.name)),
                source_tag: Some(proto::golden_tag(template)?),
                ..Default::default()
            })
            .await?;
        let result = self.capture_code(&actor, &manifest, target).await;
        let cleanup = self.api.delete(actor, None).await;
        if let Err(error) = &cleanup {
            tracing::warn!(%error, "failed to delete code preparation actor");
        }
        result?;
        cleanup
    }

    async fn capture_code(
        &self,
        actor: &proto::Actor,
        manifest: &ArtifactManifest,
        target: proto::ObjectRef,
    ) -> Result<()> {
        let meta = actor
            .metadata
            .as_ref()
            .context("preparation actor identity missing")?;
        let actor_ref = reference(&meta.atespace, &meta.name);
        self.api.resume(actor_ref.clone()).await?;
        let token = self.issuer.issue_assignment(&meta.uid)?;
        for file in &manifest.files {
            let chunks = self.code.open(&manifest.bucket, file).await?;
            self.assignment
                .prepare_code(&self.route(&meta.name), &token, file, chunks)
                .await?;
        }
        self.api.suspend(actor_ref.clone()).await?;
        self.api
            .create_tag(proto::Tag {
                metadata: Some(proto::ResourceMetadata {
                    atespace: target.atespace,
                    name: target.name,
                    ..Default::default()
                }),
                source_actor: Some(actor_ref),
                scope: proto::TagScope::Atespace.into(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }
}

pub(super) fn code_tag(
    template: &proto::ActorTemplate,
    artifact: &str,
) -> Result<proto::ObjectRef> {
    let meta = template
        .metadata
        .as_ref()
        .context("template identity missing")?;
    ensure!(!meta.uid.is_empty(), "template UID missing");
    let manifest = ArtifactManifest::decode(artifact)?;
    let identity = serde_json::to_string(&(&meta.uid, manifest))?;
    Ok(reference(
        &meta.atespace,
        &format!("code-{}", digest(&identity)),
    ))
}
