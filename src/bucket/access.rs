use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use gcp_auth::TokenProvider;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

use crate::clock::{Clock, SystemClock};

mod client_side;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub persistence: super::SnapshotStoreConfig,
    pub region: String,
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

// Scoped tokens are credentials to customer data, so each request mints a fresh one and nothing
// keeps it. They are minted locally (client-side Credential Access Boundaries) from an
// intermediary credential held only in this process's memory, so a launch costs no STS call.
pub(crate) struct RuntimeAccess {
    location: BucketLocation,
    token_source: Option<Box<dyn StorageTokenSource>>,
    persistence: super::SnapshotStoreConfig,
}

impl RuntimeAccess {
    pub fn new(location: BucketLocation, persistence: super::SnapshotStoreConfig) -> Result<Self> {
        Ok(Self {
            token_source: match &location {
                BucketLocation::Gcs { .. } => Some(Box::new(client_side::ClientSideTokens::new(
                    Box::new(GcsIntermediaryExchange {
                        credentials: StorageCredentials::new()?,
                        http: reqwest::Client::builder()
                            .timeout(Duration::from_secs(20))
                            .build()?,
                    }),
                ))),
                BucketLocation::File { .. } => None,
            },
            location,
            persistence,
        })
    }

    pub(crate) fn validate_code(&self, code: &crate::bundle::ActorBundle) -> Result<()> {
        let BucketLocation::Gcs {
            artifact_bucket, ..
        } = &self.location
        else {
            anyhow::bail!("compiled GCS bundles require GCS storage");
        };
        ensure!(
            code.bucket == *artifact_bucket,
            "code artifact belongs to another bucket"
        );
        artifact_prefix(code)?;
        Ok(())
    }

    pub async fn bootstrap(
        &self,
        region: &str,
        actor: &crate::actor::ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<String> {
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.location.clone(),
            persistence: self.persistence.clone(),
            region: region.into(),
            token: self.issue(actor, code_snapshot).await?,
        })?)
    }

    pub async fn issue(
        &self,
        actor: &crate::actor::ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<Option<StorageToken>> {
        let BucketLocation::Gcs {
            bucket,
            artifact_bucket,
        } = &self.location
        else {
            return Ok(None);
        };
        let code = code_snapshot
            .map(crate::bundle::ActorBundle::decode)
            .transpose()?;
        let rules = rules(
            bucket,
            &self.persistence,
            Some(artifact_bucket),
            actor,
            code.as_ref(),
        )?;
        let source = self
            .token_source
            .as_ref()
            .context("storage token issuer missing")?;
        Ok(Some(source.issue(&rules).await?))
    }
}

#[async_trait]
trait StorageTokenSource: Send + Sync {
    async fn issue(&self, rules: &[Rule]) -> Result<StorageToken>;
}

struct GcsIntermediaryExchange {
    credentials: StorageCredentials,
    http: reqwest::Client,
}

#[async_trait]
impl client_side::IntermediaryExchange for GcsIntermediaryExchange {
    async fn exchange(&self) -> Result<client_side::IntermediaryToken> {
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
                "urn:ietf:params:oauth:token-type:access_boundary_intermediary_token",
            ),
            ("subject_token", &source),
        ]);
        let started = SystemClock.now_ms()?;
        let response = self.exchange_token(form.query().unwrap()).await?;
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
            expires_in: u64,
            access_boundary_session_key: String,
        }
        let token: Token = response.json().await.context(
            "exchange intermediary GCS credentials (service-account credentials required)",
        )?;
        ensure!(
            token.expires_in > 60,
            "GCS credential lifetime is too short"
        );
        Ok(client_side::IntermediaryToken {
            access_token: token.access_token,
            expires_at_ms: started + (token.expires_in - 30) * 1000,
            session_key: token.access_boundary_session_key,
        })
    }
}

impl GcsIntermediaryExchange {
    async fn exchange_token(&self, form: &str) -> reqwest::Result<reqwest::Response> {
        let mut attempt = 1;
        (|| async {
            self.http
                .post("https://sts.googleapis.com/v1/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(form.to_owned())
                .timeout(Duration::from_secs(5))
                .send()
                .await?
                .error_for_status()
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(250))
                .with_max_times(3)
                .with_jitter(),
        )
        .when(|error: &reqwest::Error| {
            error.is_connect()
                || error.is_timeout()
                || matches!(
                    error.status().map(|status| status.as_u16()),
                    Some(429 | 500 | 502 | 503 | 504)
                )
        })
        .adjust(|error, delay| throttled_delay(error.status(), delay))
        .notify(|error, delay| {
            attempt += 1;
            tracing::warn!(attempt, status = ?error.status(), delay_ms = delay.as_millis(),
                "retrying intermediary GCS credential exchange");
        })
        .await
    }
}

/// A 429 means the project-wide STS quota is exhausted, so retrying on the transient
/// schedule only adds to the next rejection.
fn throttled_delay(
    status: Option<reqwest::StatusCode>,
    delay: Option<Duration>,
) -> Option<Duration> {
    delay.map(|delay| match status {
        Some(reqwest::StatusCode::TOO_MANY_REQUESTS) => delay + Duration::from_secs(1),
        _ => delay,
    })
}

enum StorageCredentials {
    ServiceAccount(String),
    ApplicationDefault(AccessTokenCredentials),
}

impl StorageCredentials {
    fn new() -> Result<Self> {
        if let Some(path) = std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
            let document = std::fs::read_to_string(path)?;
            let value: Value = serde_json::from_str(&document)?;
            if value["type"] == "service_account" {
                gcp_auth::CustomServiceAccount::from_json(&document)?;
                return Ok(Self::ServiceAccount(document));
            }
        }
        Ok(Self::ApplicationDefault(
            Builder::default().build_access_token_credentials()?,
        ))
    }

    async fn token(&self) -> Result<String> {
        match self {
            // STS requires an OAuth access token, not the default library's self-signed JWT. The
            // intermediary expires with this token, and gcp_auth reuses a cached one until 20 s
            // before expiry, so each exchange signs in afresh.
            Self::ServiceAccount(document) => {
                Ok(gcp_auth::CustomServiceAccount::from_json(document)?
                    .token(&["https://www.googleapis.com/auth/cloud-platform"])
                    .await?
                    .as_str()
                    .to_owned())
            }
            Self::ApplicationDefault(credentials) => Ok(credentials.access_token().await?.token),
        }
    }
}

/// One Credential Access Boundary rule. Its condition is an OR of terms, rendered as CEL text for
/// server-side boundaries and compiled to an expression tree for client-side ones.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Rule {
    pub bucket: String,
    pub roles: &'static [&'static str],
    pub terms: Vec<Term>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Term {
    /// `resource.name == <object resource>`
    Equals(String),
    /// `resource.name.startsWith(<object resource prefix>)`
    StartsWith(String),
    /// `api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith(<prefix>)`
    ListPrefix(String),
}

const LIST_PREFIX_ATTRIBUTE: &str = "storage.googleapis.com/objectListPrefix";

impl Rule {
    fn resource(&self) -> String {
        format!(
            "//storage.googleapis.com/projects/_/buckets/{}",
            self.bucket
        )
    }

    fn permissions(&self) -> Vec<String> {
        self.roles
            .iter()
            .map(|role| format!("inRole:roles/{role}"))
            .collect()
    }

    #[cfg(test)]
    fn expression(&self) -> Result<String> {
        let terms = self
            .terms
            .iter()
            .map(|term| {
                Ok(match term {
                    Term::Equals(name) => {
                        format!("resource.name == {}", serde_json::to_string(name)?)
                    }
                    Term::StartsWith(prefix) => {
                        format!(
                            "resource.name.startsWith({})",
                            serde_json::to_string(prefix)?
                        )
                    }
                    Term::ListPrefix(prefix) => format!(
                        "api.getAttribute('{LIST_PREFIX_ATTRIBUTE}', '').startsWith({})",
                        serde_json::to_string(prefix)?
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(terms.join(" || "))
    }
}

/// The server-side form of the boundary, as STS `options` would take it.
#[cfg(test)]
fn boundary(
    bucket: &str,
    persistence: &super::SnapshotStoreConfig,
    artifact_bucket: Option<&str>,
    actor: &crate::actor::ActorKey,
    code: Option<&crate::bundle::ActorBundle>,
) -> Result<Value> {
    let rules = rules(bucket, persistence, artifact_bucket, actor, code)?
        .iter()
        .map(|rule| {
            Ok(json!({"availableResource": rule.resource(),
                "availablePermissions": rule.permissions(),
                "availabilityCondition": {"expression": rule.expression()?}}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"accessBoundary": {"accessBoundaryRules": rules}}))
}

fn rules(
    bucket: &str,
    persistence: &super::SnapshotStoreConfig,
    artifact_bucket: Option<&str>,
    actor: &crate::actor::ActorKey,
    code: Option<&crate::bundle::ActorBundle>,
) -> Result<Vec<Rule>> {
    let owner = format!(
        "projects/_/buckets/{bucket}/objects/{}",
        crate::storage_paths::owner(&actor.storage_key())?
    );
    let mut rules = vec![Rule {
        bucket: bucket.into(),
        roles: &["storage.objectUser"],
        terms: vec![Term::Equals(owner)],
    }];
    let prefix = crate::storage_paths::snapshots(actor)?;
    match persistence {
        super::SnapshotStoreConfig::Local => rules.push(snapshot_rule(bucket, &[&prefix], false)),
        super::SnapshotStoreConfig::Rapid {
            archive_bucket,
            buckets,
            ..
        } => {
            let prefix = super::rapid::object_name(&prefix)?;
            rules.push(snapshot_rule(archive_bucket, &[&prefix], false));
            let logs = prefix.replacen("snapshots-", "logs-", 1);
            for placement in buckets {
                rules.push(snapshot_rule(&placement.bucket, &[&logs], true));
            }
        }
    }

    if let Some(code) = code {
        ensure!(
            Some(code.bucket.as_str()) == artifact_bucket,
            "code artifact belongs to another bucket"
        );
        rules.push(Rule {
            bucket: code.bucket.clone(),
            roles: &["storage.objectViewer"],
            terms: vec![Term::StartsWith(format!(
                "projects/_/buckets/{}/objects/{}",
                code.bucket,
                artifact_prefix(code)?,
            ))],
        });
    }
    Ok(rules)
}

fn snapshot_rule(bucket: &str, prefixes: &[&str], mutable: bool) -> Rule {
    let terms = prefixes
        .iter()
        .flat_map(|prefix| {
            [
                Term::StartsWith(format!("projects/_/buckets/{bucket}/objects/{prefix}")),
                Term::ListPrefix((*prefix).to_owned()),
            ]
        })
        .collect();
    Rule {
        bucket: bucket.into(),
        roles: if mutable {
            &["storage.objectUser"]
        } else {
            &["storage.objectViewer", "storage.objectCreator"]
        },
        terms,
    }
}

fn artifact_prefix(code: &crate::bundle::ActorBundle) -> Result<String> {
    let root = format!("{}artifacts/", crate::storage_paths::ROOT);
    let first = code.files.first().context("code artifact is empty")?;
    let relative = first
        .object
        .strip_prefix(&root)
        .context("invalid artifact location")?;
    let (deployment, _) = relative
        .split_once('/')
        .context("artifact deployment missing")?;
    uuid::Uuid::parse_str(deployment).context("invalid artifact deployment")?;
    let prefix = format!("{root}{deployment}/");
    ensure!(
        code.files
            .iter()
            .all(|file| file.object == format!("{prefix}{}", file.path)),
        "code files must belong to one artifact deployment"
    );
    Ok(prefix)
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/access.rs"]
mod tests;
