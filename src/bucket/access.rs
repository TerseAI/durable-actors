use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use gcp_auth::TokenProvider;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    clock::{Clock, SystemClock},
    replication::{ReplicaAccess, ReplicaProvisioner, ReplicaScope, ReplicaTarget},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub region: String,
    pub replica_secret: String,
    pub replica_regions: Vec<String>,
    pub token: Option<StorageToken>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BucketLocation {
    Gcs { bucket: String },
    File { directory: PathBuf },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageToken {
    pub access_token: String,
    pub expires_at_ms: u64,
}

impl std::fmt::Debug for StorageToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageToken")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

pub(crate) struct RuntimeAccess {
    credentials: Option<StorageCredentials>,
    http: reqwest::Client,
    location: BucketLocation,
    fleet: Arc<dyn ReplicaProvisioner>,
    replicas: ReplicaAccess,
    tokens: moka::future::Cache<(), StorageToken>,
    storage: Arc<super::RuntimeStorage>,
    initial: moka::future::Cache<String, super::ReplicaMembership>,
    targets: moka::future::Cache<String, Vec<ReplicaTarget>>,
}

impl RuntimeAccess {
    pub fn new(
        location: BucketLocation,
        fleet: Arc<dyn ReplicaProvisioner>,
        replicas: ReplicaAccess,
        storage: Arc<super::RuntimeStorage>,
    ) -> Result<Self> {
        Ok(Self {
            credentials: match &location {
                BucketLocation::Gcs { .. } => Some(StorageCredentials::new()?),
                BucketLocation::File { .. } => None,
            },
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()?,
            location,
            fleet,
            replicas,
            storage,
            initial: moka::future::Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            targets: moka::future::Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            tokens: moka::future::Cache::builder()
                .max_capacity(1)
                .time_to_live(Duration::from_secs(300))
                .build(),
        })
    }

    pub async fn bootstrap(&self, region: &str) -> Result<String> {
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.location.clone(),
            region: region.into(),
            replica_secret: self.replicas.secret().to_owned(),
            replica_regions: self.fleet.replica_regions(),
            token: self.issue().await?,
        })?)
    }

    pub fn prewarm(self: &Arc<Self>, scope: ReplicaScope) {
        if self.fleet.replica_regions().is_empty() {
            return;
        }
        let access = self.clone();
        tokio::spawn(async move {
            if let Err(error) = access.initial_replicas(&scope).await {
                tracing::warn!(%error, actor = %scope.actor.storage_key(), "actor replica provisioning failed");
            }
        });
    }

    pub async fn initial_replicas(&self, scope: &ReplicaScope) -> Result<super::ReplicaMembership> {
        self.initial
            .try_get_with(scope.identity(), async {
                let replicas = self.initial_replica_targets(scope).await?;
                self.storage
                    .register_initial_replicas(scope, replicas)
                    .await
            })
            .await
            .map_err(|error| anyhow::anyhow!("initial replica registration failed: {error:#}"))
    }

    pub async fn initial_replica_targets(
        &self,
        scope: &ReplicaScope,
    ) -> Result<Vec<ReplicaTarget>> {
        self.targets
            .try_get_with(scope.identity(), self.fleet.ensure(scope))
            .await
            .map_err(|error| anyhow::anyhow!("initial replica assignment failed: {error:#}"))
    }

    pub async fn replicas(
        &self,
        scope: &ReplicaScope,
        failed: &[String],
    ) -> Result<Vec<ReplicaTarget>> {
        self.fleet.repair(scope, failed).await
    }

    pub async fn issue(&self) -> Result<Option<StorageToken>> {
        if matches!(self.location, BucketLocation::File { .. }) {
            return Ok(None);
        }
        if let Some(token) = self.tokens.get(&()).await {
            if token.expires_at_ms > SystemClock.now_ms()? + 60_000 {
                return Ok(Some(token));
            }
            self.tokens.invalidate(&()).await;
        }
        self.tokens
            .try_get_with((), self.exchange())
            .await
            .map(Some)
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }

    async fn exchange(&self) -> Result<StorageToken> {
        let BucketLocation::Gcs { bucket } = &self.location else {
            anyhow::bail!("local buckets do not need credentials")
        };
        let boundary = boundary(bucket);
        let source = self
            .credentials
            .as_ref()
            .context("GCS credentials missing")?
            .token()
            .await?;
        let mut form = reqwest::Url::parse("https://sts.googleapis.com/")?;
        form.query_pairs_mut().extend_pairs([
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", &source),
            ("options", &boundary.to_string()),
        ]);
        let started = SystemClock.now_ms()?;
        let response = self
            .http
            .post("https://sts.googleapis.com/v1/token")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form.query().unwrap().to_owned())
            .send()
            .await?
            .error_for_status()?;
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
            expires_in: u64,
        }
        let token: Token = response
            .json()
            .await
            .context("exchange scoped GCS credentials (service-account credentials required)")?;
        ensure!(
            token.expires_in > 60,
            "GCS credential lifetime is too short"
        );
        Ok(StorageToken {
            access_token: token.access_token,
            expires_at_ms: started + (token.expires_in - 30) * 1000,
        })
    }
}

enum StorageCredentials {
    ServiceAccount(gcp_auth::CustomServiceAccount),
    ApplicationDefault(AccessTokenCredentials),
}

impl StorageCredentials {
    fn new() -> Result<Self> {
        if let Some(path) = std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
            let document = std::fs::read_to_string(path)?;
            let value: Value = serde_json::from_str(&document)?;
            if value["type"] == "service_account" {
                return Ok(Self::ServiceAccount(
                    gcp_auth::CustomServiceAccount::from_json(&document)?,
                ));
            }
        }
        Ok(Self::ApplicationDefault(
            Builder::default().build_access_token_credentials()?,
        ))
    }

    async fn token(&self) -> Result<String> {
        match self {
            // STS requires an OAuth access token, not the default library's self-signed JWT.
            Self::ServiceAccount(credentials) => Ok(credentials
                .token(&["https://www.googleapis.com/auth/cloud-platform"])
                .await?
                .as_str()
                .to_owned()),
            Self::ApplicationDefault(credentials) => Ok(credentials.access_token().await?.token),
        }
    }
}

fn boundary(bucket: &str) -> Value {
    let prefix = crate::storage_paths::ROOT;
    json!({"accessBoundary": {"accessBoundaryRules": [
        rule(bucket, &[format!("{prefix}owners/"), format!("{prefix}hosts/")], &["storage.objectUser"]),
        rule(bucket, &[format!("{prefix}snapshots/")], &["storage.objectViewer", "storage.objectCreator"])
    ]}})
}

fn rule(bucket: &str, prefixes: &[String], roles: &[&str]) -> Value {
    let expression = prefixes.iter().map(|prefix| format!(
        "resource.name.startsWith('projects/_/buckets/{bucket}/objects/{prefix}') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('{prefix}')"
    )).collect::<Vec<_>>().join(" || ");
    json!({"availableResource": format!("//storage.googleapis.com/projects/_/buckets/{bucket}"),
        "availablePermissions": roles.iter().map(|role| format!("inRole:roles/{role}")).collect::<Vec<_>>(),
        "availabilityCondition": {"expression": expression}})
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/access.rs"]
mod tests;
