use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_storage::client::Storage;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::sandbox::{
    BuiltActorCode,
    source::{SourceArchive, dependency_prefix},
};

pub(super) struct SourceBuilds {
    bucket: String,
    store: Arc<dyn BuildStore>,
    workers: Arc<dyn BuildWorkers>,
}

#[async_trait]
pub(super) trait SourceBuilder: Send + Sync {
    async fn cached(&self, project: &str, image: &str, source: &SourceArchive) -> Result<bool>;
    async fn build(
        &self,
        project: &str,
        image: &str,
        region: &str,
        source: &SourceArchive,
    ) -> Result<BuiltActorCode>;
}

impl SourceBuilds {
    pub fn new(bucket: String, storage: Storage, workers: Arc<dyn BuildWorkers>) -> Self {
        Self {
            store: Arc::new(GcsBuildStore {
                storage,
                bucket: bucket.clone(),
            }),
            bucket,
            workers,
        }
    }
}

#[async_trait]
impl SourceBuilder for SourceBuilds {
    async fn cached(&self, project: &str, image: &str, source: &SourceArchive) -> Result<bool> {
        source.validate()?;
        Ok(self
            .store
            .get(&source.cache_key(project, image))
            .await?
            .is_some())
    }

    async fn build(
        &self,
        project: &str,
        image: &str,
        region: &str,
        source: &SourceArchive,
    ) -> Result<BuiltActorCode> {
        source.validate()?;
        let key = source.cache_key(project, image);
        if let Some(build) = self.store.get(&key).await? {
            tracing::info!(
                project_id = project,
                cache_hit = true,
                "actor source build reused"
            );
            return Ok(build);
        }
        source
            .object
            .as_ref()
            .context("compiled build is missing; upload the source archive again")?;
        let artifact_prefix = format!(
            "{}artifacts/{}/",
            crate::storage_paths::ROOT,
            uuid::Uuid::new_v4()
        );
        let dependency_prefix = dependency_prefix(project, image);
        let access_token = self
            .store
            .token(source, &artifact_prefix, &dependency_prefix)
            .await?;
        let started = std::time::Instant::now();
        let reply = self
            .workers
            .build(
                region,
                &WorkerRequest {
                    source: source.clone(),
                    bucket: self.bucket.clone(),
                    artifact_prefix: artifact_prefix.clone(),
                    dependency_prefix,
                    access_token,
                },
            )
            .await?;
        ensure!(
            reply.manifest.bucket == self.bucket
                && reply
                    .manifest
                    .files
                    .iter()
                    .all(|file| file.object == format!("{artifact_prefix}{}", file.path)),
            "build worker returned an artifact outside its deployment"
        );
        ensure!(
            reply.manifest.entrypoint()?
                == if source.entrypoint.ends_with(".py") {
                    "actors.pyz"
                } else {
                    "actors.mjs"
                },
            "compiled entrypoint does not match the source language"
        );
        crate::control_plane::contracts::PublicActorContract::new(reply.contract.clone())?;
        let build = BuiltActorCode {
            code_snapshot: reply.manifest.encode()?,
            contract: reply.contract,
            source_archive: Some(source.clone()),
        };
        self.store.put(&key, &build).await?;
        tracing::info!(project_id = project, total_ms = started.elapsed().as_millis() as u64, dependency_cache_hit = reply.dependency_cache_hit, timings = ?reply.timings, "actor source build completed");
        Ok(build)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WorkerRequest {
    pub source: SourceArchive,
    pub bucket: String,
    pub artifact_prefix: String,
    pub dependency_prefix: String,
    pub access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WorkerReply {
    pub manifest: crate::artifacts::ArtifactManifest,
    pub contract: Value,
    pub timings: std::collections::BTreeMap<String, u64>,
    pub dependency_cache_hit: bool,
}

#[async_trait]
pub(super) trait BuildWorkers: Send + Sync {
    async fn build(&self, region: &str, request: &WorkerRequest) -> Result<WorkerReply>;
}

#[async_trait]
trait BuildStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BuiltActorCode>>;
    async fn put(&self, key: &str, build: &BuiltActorCode) -> Result<()>;
    async fn token(
        &self,
        source: &SourceArchive,
        artifacts: &str,
        dependencies: &str,
    ) -> Result<String>;
}

struct GcsBuildStore {
    storage: Storage,
    bucket: String,
}

#[async_trait]
impl BuildStore for GcsBuildStore {
    async fn get(&self, key: &str) -> Result<Option<BuiltActorCode>> {
        let mut response = match self
            .storage
            .read_object(format!("projects/_/buckets/{}", self.bucket), key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error)
                if error.http_status_code() == Some(404)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "NOT_FOUND") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            bytes.extend_from_slice(&chunk?);
            ensure!(
                bytes.len() <= 4 * 1024 * 1024,
                "cached build metadata exceeds the size limit"
            );
        }
        let build: BuiltActorCode = serde_json::from_slice(&bytes)?;
        let manifest = crate::artifacts::ArtifactManifest::decode(&build.code_snapshot)?;
        ensure!(
            manifest.bucket == self.bucket,
            "cached build belongs to another artifact bucket"
        );
        crate::control_plane::contracts::PublicActorContract::new(build.contract.clone())?;
        Ok(Some(build))
    }

    async fn put(&self, key: &str, build: &BuiltActorCode) -> Result<()> {
        match self
            .storage
            .write_object(
                format!("projects/_/buckets/{}", self.bucket),
                key,
                bytes::Bytes::from(serde_json::to_vec(build)?),
            )
            .set_if_generation_match(0)
            .send_unbuffered()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error.http_status_code() == Some(412)
                    || error
                        .status()
                        .is_some_and(|status| status.code.name() == "FAILED_PRECONDITION") =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn token(
        &self,
        source: &SourceArchive,
        artifacts: &str,
        dependencies: &str,
    ) -> Result<String> {
        Ok(crate::bucket::access::scoped_storage_token(&build_boundary(
            &self.bucket,
            source,
            artifacts,
            dependencies,
        )?)
        .await?
        .access_token)
    }
}

fn build_boundary(
    bucket: &str,
    source: &SourceArchive,
    artifacts: &str,
    dependencies: &str,
) -> Result<Value> {
    let source = source.object.as_ref().context("source object missing")?;
    let rule = |bucket: &str, roles: &[&str], expression: String| {
        json!({
            "availableResource": format!("//storage.googleapis.com/projects/_/buckets/{bucket}"),
            "availablePermissions": roles.iter().map(|role| format!("inRole:roles/storage.{role}")).collect::<Vec<_>>(),
            "availabilityCondition": { "expression": expression }
        })
    };
    let object = |bucket: &str, path: &str| {
        serde_json::to_string(&format!("projects/_/buckets/{bucket}/objects/{path}"))
    };
    Ok(json!({"accessBoundary": {"accessBoundaryRules": [
        rule(&source.bucket, &["objectViewer"], format!("resource.name == {}", object(&source.bucket, &source.name)?)),
        rule(bucket, &["objectCreator"], format!("resource.name.startsWith({})", object(bucket, artifacts)?)),
        rule(bucket, &["objectViewer", "objectCreator"], format!("resource.name.startsWith({})", object(bucket, dependencies)?))
    ]}}))
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/source_builds.rs"]
mod tests;
