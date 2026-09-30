use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use google_cloud_storage::client::Storage;
use reqwest::Method;
use serde_json::{Value, json};

use super::source_builds::{BuildExecutor, BuildReply, BuildRequest};

pub(crate) struct CloudBuildConfig {
    pub project: String,
    pub region: String,
    pub service_account: String,
    pub machine_type: String,
}

impl CloudBuildConfig {
    pub fn validate(&self) -> Result<()> {
        for value in [&self.project, &self.region] {
            ensure!(
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                "invalid Cloud Build project or region"
            );
        }
        ensure!(
            self.service_account.ends_with(".iam.gserviceaccount.com")
                && self.service_account.contains('@')
                && !self.service_account.contains('/'),
            "Cloud Build service account must be an email address"
        );
        ensure!(
            [
                "E2_MEDIUM",
                "E2_STANDARD_2",
                "E2_HIGHCPU_8",
                "E2_HIGHCPU_32"
            ]
            .contains(&self.machine_type.as_str()),
            "unsupported Cloud Build machine type"
        );
        Ok(())
    }
}

pub(super) struct CloudBuild {
    config: CloudBuildConfig,
    image: String,
    api: Arc<dyn BuildApi>,
    results: Arc<dyn BuildResults>,
}

impl CloudBuild {
    pub fn new(config: CloudBuildConfig, image: String, storage: Storage) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            image,
            api: Arc::new(GoogleBuildApi {
                http: reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
                credentials: crate::bucket::access::StorageCredentials::new()?,
            }),
            results: Arc::new(GcsBuildResults(storage)),
        })
    }

    fn specification(&self, request: &BuildRequest) -> Result<Value> {
        // Cloud Build expands substitutions even in environment values.
        let request = serde_json::to_string(request)?.replace('$', "$$");
        Ok(json!({
            "steps": [{"name": self.image, "entrypoint": "python3", "args": ["/opt/durable-actors/source-build.py"], "env": [format!("DURABLE_ACTORS_BUILD_REQUEST={request}")]}],
            "serviceAccount": format!("projects/{}/serviceAccounts/{}", self.config.project, self.config.service_account),
            "options": {"machineType": self.config.machine_type, "logging": "CLOUD_LOGGING_ONLY"},
            "timeout": "900s", "queueTtl": "300s", "tags": ["actor-source-build"]
        }))
    }

    async fn finish(&self, path: &str, request: &BuildRequest) -> Result<BuildReply> {
        loop {
            let build = self.api.send(Method::GET, path, None).await?;
            let status = build["status"]
                .as_str()
                .context("Cloud Build status missing")?;
            match status {
                "QUEUED" | "PENDING" | "WORKING" => {
                    tokio::time::sleep(Duration::from_secs(2)).await
                }
                "SUCCESS" => {
                    return serde_json::from_value(self.result(request).await?)
                        .context("invalid Cloud Build result");
                }
                _ => {
                    let result = self.result(request).await.ok();
                    let detail = result
                        .as_ref()
                        .and_then(|value| value["error"].as_str())
                        .unwrap_or("see Cloud Build logs");
                    bail!("Cloud Build {path} {status}: {detail}");
                }
            }
        }
    }

    async fn result(&self, request: &BuildRequest) -> Result<Value> {
        self.results
            .read(
                &request.bucket,
                &format!("{}result.json", request.artifact_prefix),
            )
            .await
    }
}

#[async_trait]
impl BuildExecutor for CloudBuild {
    async fn build(&self, _region: &str, request: &BuildRequest) -> Result<BuildReply> {
        let parent = format!(
            "projects/{}/locations/{}/builds",
            self.config.project, self.config.region
        );
        let operation = self
            .api
            .send(Method::POST, &parent, Some(self.specification(request)?))
            .await?;
        let id = operation["metadata"]["build"]["id"]
            .as_str()
            .context("Cloud Build did not return a build ID")?;
        ensure!(
            !id.is_empty() && id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'),
            "invalid Cloud Build ID"
        );
        let path = format!("{parent}/{id}");
        tracing::info!(build = %path, machine_type = %self.config.machine_type, "actor Cloud Build submitted");
        match tokio::time::timeout(Duration::from_secs(1230), self.finish(&path, request)).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self
                    .api
                    .send(Method::POST, &format!("{path}:cancel"), Some(json!({})))
                    .await;
                bail!("Cloud Build {path} exceeded its deadline");
            }
        }
    }
}

#[async_trait]
trait BuildApi: Send + Sync {
    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value>;
}
struct GoogleBuildApi {
    http: reqwest::Client,
    credentials: crate::bucket::access::StorageCredentials,
}
#[async_trait]
impl BuildApi for GoogleBuildApi {
    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self
            .http
            .request(
                method,
                format!("https://cloudbuild.googleapis.com/v1/{path}"),
            )
            .bearer_auth(self.credentials.token().await?);
        if let Some(body) = body {
            request = request.json(&body);
        }
        Ok(request.send().await?.error_for_status()?.json().await?)
    }
}

#[async_trait]
trait BuildResults: Send + Sync {
    async fn read(&self, bucket: &str, object: &str) -> Result<Value>;
}
struct GcsBuildResults(Storage);
#[async_trait]
impl BuildResults for GcsBuildResults {
    async fn read(&self, bucket: &str, object: &str) -> Result<Value> {
        let mut response = self
            .0
            .read_object(format!("projects/_/buckets/{bucket}"), object)
            .send()
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            bytes.extend_from_slice(&chunk?);
            ensure!(
                bytes.len() <= 4 * 1024 * 1024,
                "Cloud Build result exceeds the size limit"
            );
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/cloud_build.rs"]
mod tests;
