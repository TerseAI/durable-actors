use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    replication::{ReplicaAccess, ReplicaProvisioner, ReplicaTarget},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub region: String,
    pub replica_secret: String,
    pub replicas: Vec<ReplicaTarget>,
    pub replica_count: usize,
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
    credentials: Option<AccessTokenCredentials>,
    http: reqwest::Client,
    location: BucketLocation,
    fleet: Arc<dyn ReplicaProvisioner>,
    replicas: ReplicaAccess,
    count: usize,
    tokens: moka::future::Cache<String, StorageToken>,
}

impl RuntimeAccess {
    pub fn new(
        location: BucketLocation,
        fleet: Arc<dyn ReplicaProvisioner>,
        replicas: ReplicaAccess,
        count: usize,
    ) -> Result<Self> {
        Ok(Self {
            credentials: match &location {
                BucketLocation::Gcs { .. } => {
                    Some(Builder::default().build_access_token_credentials()?)
                }
                BucketLocation::File { .. } => None,
            },
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()?,
            location,
            fleet,
            replicas,
            count,
            tokens: moka::future::Cache::builder()
                .max_capacity(512)
                .time_to_live(Duration::from_secs(300))
                .build(),
        })
    }

    pub async fn bootstrap(&self, namespace: &str, region: &str) -> Result<String> {
        let actor = ActorKey {
            namespace_id: namespace.into(),
            actor_type: "bootstrap".into(),
            actor_id: "bootstrap".into(),
        };
        let (token, replicas) = tokio::try_join!(
            self.issue(namespace),
            self.fleet.ensure(&actor, region, self.count)
        )?;
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.location.clone(),
            region: region.into(),
            replica_secret: self.replicas.delegate_secret(namespace)?,
            replicas,
            replica_count: self.count,
            token,
        })?)
    }

    pub async fn issue(&self, namespace: &str) -> Result<Option<StorageToken>> {
        if matches!(self.location, BucketLocation::File { .. }) {
            return Ok(None);
        }
        if let Some(token) = self.tokens.get(namespace).await {
            if token.expires_at_ms > SystemClock.now_ms()? + 60_000 {
                return Ok(Some(token));
            }
            self.tokens.invalidate(namespace).await;
        }
        self.tokens
            .try_get_with(namespace.to_owned(), self.exchange(namespace))
            .await
            .map(Some)
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }

    async fn exchange(&self, namespace: &str) -> Result<StorageToken> {
        let BucketLocation::Gcs { bucket } = &self.location else {
            anyhow::bail!("local buckets do not need credentials")
        };
        let boundary = boundary(bucket, namespace)?;
        let source = self
            .credentials
            .as_ref()
            .context("GCS credentials missing")?
            .access_token()
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
            ("subject_token", &source.token),
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

fn boundary(bucket: &str, namespace: &str) -> Result<Value> {
    crate::actor::ActorScope {
        namespace_id: namespace.into(),
    }
    .validate()?;
    let prefix = crate::storage_paths::namespace(namespace);
    Ok(json!({"accessBoundary": {"accessBoundaryRules": [
        rule(bucket, &[format!("{prefix}owners/"), format!("{prefix}hosts/")], &["storage.objectUser"]),
        rule(bucket, &[format!("{prefix}snapshots/")], &["storage.objectViewer", "storage.objectCreator"])
    ]}}))
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
mod tests {
    use super::*;
    #[test]
    fn one_bucket_scopes_mutable_metadata_and_immutable_snapshots_separately() -> Result<()> {
        let boundary = boundary("actors", "project.a")?;
        let rules = boundary["accessBoundary"]["accessBoundaryRules"]
            .as_array()
            .unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["availableResource"], rules[1]["availableResource"]);
        assert!(
            rules[0]["availabilityCondition"]["expression"]
                .as_str()
                .unwrap()
                .contains("little-actors/v1/namespaces/cHJvamVjdC5h/owners/")
        );
        assert_eq!(
            rules[1]["availablePermissions"],
            json!([
                "inRole:roles/storage.objectViewer",
                "inRole:roles/storage.objectCreator"
            ])
        );
        assert!(
            rules[1]["availabilityCondition"]["expression"]
                .as_str()
                .unwrap()
                .contains("little-actors/v1/namespaces/cHJvamVjdC5h/snapshots/")
        );
        assert!(super::boundary("actors", "../escape").is_err());
        Ok(())
    }
}
