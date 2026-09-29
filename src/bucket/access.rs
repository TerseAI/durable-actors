use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use gcp_auth::TokenProvider;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_util::task::AbortOnDropHandle;

use crate::clock::{Clock, SystemClock};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub persistence: super::PersistenceConfig,
    pub region: String,
    pub replica_token: Option<String>,
    pub token: Option<StorageToken>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BucketLocation {
    Gcs {
        bucket: String,
        artifact_bucket: String,
    },
    File {
        directory: PathBuf,
    },
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
    location: BucketLocation,
    replicas: Option<crate::replicas::access::Access>,
    tokens: Option<StorageTokens>,
    storage: Arc<super::RuntimeStorage>,
}

impl RuntimeAccess {
    pub fn new(location: BucketLocation, storage: Arc<super::RuntimeStorage>) -> Result<Self> {
        Ok(Self {
            replicas: None,
            tokens: match &location {
                BucketLocation::Gcs {
                    bucket,
                    artifact_bucket,
                } => Some(StorageTokens::new(
                    Arc::new(GcsTokenSource {
                        bucket: bucket.clone(),
                        persistence: storage.persistence.clone(),
                        artifact_bucket: artifact_bucket.clone(),
                        credentials: StorageCredentials::new()?,
                        http: reqwest::Client::builder()
                            .timeout(Duration::from_secs(20))
                            .build()?,
                    }),
                    Arc::new(SystemClock),
                )),
                BucketLocation::File { .. } => None,
            },
            location,
            storage,
        })
    }

    pub fn with_replica_secret(mut self, secret: String) -> Result<Self> {
        self.replicas = Some(crate::replicas::access::Access::new(secret)?);
        Ok(self)
    }

    pub async fn bootstrap(&self, region: &str, actor: &crate::actor::ActorKey) -> Result<String> {
        Ok(serde_json::to_string(&HostStorageConfig {
            replica_token: self
                .replicas
                .as_ref()
                .map(|access| access.scoped(actor))
                .transpose()?,
            bucket: self.location.clone(),
            persistence: self.storage.persistence.clone(),
            region: region.into(),
            token: self.issue().await?,
        })?)
    }

    pub async fn issue(&self) -> Result<Option<StorageToken>> {
        match &self.tokens {
            Some(tokens) => tokens.issue().await.map(Some),
            None => Ok(None),
        }
    }
}

const TOKEN_MIN_LIFETIME_MS: u64 = 60_000;
const TOKEN_REFRESH_MARGIN_MS: u64 = 120_000;
const TOKEN_REFRESH_RETRY: Duration = Duration::from_secs(10);

struct StorageTokens {
    cache: Arc<TokenCache>,
    _refresh: AbortOnDropHandle<()>,
}

impl StorageTokens {
    fn new(source: Arc<dyn StorageTokenSource>, clock: Arc<dyn Clock>) -> Self {
        let cache = Arc::new(TokenCache {
            source,
            clock,
            token: RwLock::new(None),
            exchange: Mutex::new(()),
        });
        Self {
            _refresh: AbortOnDropHandle::new(tokio::spawn(cache.clone().refresh())),
            cache,
        }
    }

    async fn issue(&self) -> Result<StorageToken> {
        self.cache.get(TOKEN_MIN_LIFETIME_MS).await
    }
}

struct TokenCache {
    source: Arc<dyn StorageTokenSource>,
    clock: Arc<dyn Clock>,
    token: RwLock<Option<StorageToken>>,
    exchange: Mutex<()>,
}

impl TokenCache {
    async fn refresh(self: Arc<Self>) {
        loop {
            let delay = match self.refresh_delay().await {
                Ok(delay) => delay,
                Err(error) => {
                    tracing::warn!(%error, "could not refresh scoped GCS credentials");
                    TOKEN_REFRESH_RETRY
                }
            };
            tokio::time::sleep(delay).await;
        }
    }

    async fn refresh_delay(&self) -> Result<Duration> {
        let token = self.get(TOKEN_REFRESH_MARGIN_MS).await?;
        let remaining = token
            .expires_at_ms
            .saturating_sub(self.clock.now_ms()?.saturating_add(TOKEN_REFRESH_MARGIN_MS));
        Ok(Duration::from_millis(remaining).max(TOKEN_REFRESH_RETRY))
    }

    async fn get(&self, minimum_lifetime_ms: u64) -> Result<StorageToken> {
        if let Some(token) = self.cached(minimum_lifetime_ms)? {
            return Ok(token);
        }
        let _exchange = self.exchange.lock().await;
        if let Some(token) = self.cached(minimum_lifetime_ms)? {
            return Ok(token);
        }
        let token = self.source.exchange().await?;
        *self.token.write().unwrap() = Some(token.clone());
        Ok(token)
    }

    fn cached(&self, minimum_lifetime_ms: u64) -> Result<Option<StorageToken>> {
        let deadline = self.clock.now_ms()?.saturating_add(minimum_lifetime_ms);
        Ok(self
            .token
            .read()
            .unwrap()
            .as_ref()
            .filter(|token| token.expires_at_ms > deadline)
            .cloned())
    }
}

#[async_trait]
trait StorageTokenSource: Send + Sync {
    async fn exchange(&self) -> Result<StorageToken>;
}

struct GcsTokenSource {
    bucket: String,
    persistence: super::PersistenceConfig,
    artifact_bucket: String,
    credentials: StorageCredentials,
    http: reqwest::Client,
}

#[async_trait]
impl StorageTokenSource for GcsTokenSource {
    async fn exchange(&self) -> Result<StorageToken> {
        let boundary = boundary(&self.bucket, &self.persistence, Some(&self.artifact_bucket));
        let source = self.credentials.token().await?;
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

fn boundary(
    bucket: &str,
    persistence: &super::PersistenceConfig,
    artifact_bucket: Option<&str>,
) -> Value {
    let prefix = crate::storage_paths::ROOT;
    let mut rules = vec![rule(
        bucket,
        &[format!("{prefix}owners/"), format!("{prefix}hosts/")],
        &["storage.objectUser"],
    )];
    match persistence {
        super::PersistenceConfig::Replicated { .. } => {}
        super::PersistenceConfig::Local => rules.push(rule(
            bucket,
            &[format!("{prefix}snapshots/")],
            &["storage.objectViewer", "storage.objectCreator"],
        )),
    }
    if let Some(bucket) = artifact_bucket {
        rules.push(rule(
            bucket,
            &[format!("{prefix}artifacts/")],
            &["storage.objectViewer"],
        ));
    }
    json!({"accessBoundary": {"accessBoundaryRules": rules}})
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
