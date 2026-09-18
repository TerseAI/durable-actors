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
    tokens: moka::future::Cache<(), StorageToken>,
}

impl RuntimeAccess {
    pub fn new(
        location: BucketLocation,
        fleet: Arc<dyn ReplicaProvisioner>,
        replicas: ReplicaAccess,
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
            tokens: moka::future::Cache::builder()
                .max_capacity(1)
                .time_to_live(Duration::from_secs(300))
                .build(),
        })
    }

    pub async fn bootstrap(&self, region: &str) -> Result<String> {
        let actor = ActorKey {
            actor_type: "bootstrap".into(),
            actor_id: "bootstrap".into(),
        };
        let (token, replicas) = tokio::try_join!(self.issue(), self.fleet.ensure(&actor, region))?;
        ensure!(
            replicas.len() == self.fleet.replica_regions().len(),
            "incomplete replica set"
        );
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.location.clone(),
            region: region.into(),
            replica_secret: self.replicas.secret().to_owned(),
            replicas,
            token,
        })?)
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn host_bootstrap_carries_replica_membership_without_a_separate_count() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let replicas: Vec<_> = ["first", "second"]
            .into_iter()
            .map(|id| ReplicaTarget {
                host_id: id.into(),
                url: format!("https://{id}.example"),
                region: "north-america-east".into(),
            })
            .collect();
        let access = RuntimeAccess::new(
            BucketLocation::File {
                directory: directory.path().into(),
            },
            Arc::new(crate::replication::ReplicaSet(replicas.clone())),
            ReplicaAccess::new("secret", Arc::new(SystemClock)),
        )?;
        let document = access.bootstrap("north-america-east").await?;
        let value: Value = serde_json::from_str(&document)?;
        assert!(value.get("replicaCount").is_none());
        let config: HostStorageConfig = serde_json::from_str(&document)?;
        assert_eq!(config.replicas, replicas);
        let partial = RuntimeAccess::new(
            BucketLocation::File {
                directory: directory.path().into(),
            },
            Arc::new(PartialFleet(crate::replication::ReplicaSet(replicas))),
            ReplicaAccess::new("secret", Arc::new(SystemClock)),
        )?;
        assert!(partial.bootstrap("north-america-east").await.is_err());
        Ok(())
    }

    struct PartialFleet(crate::replication::ReplicaSet);

    #[async_trait::async_trait]
    impl ReplicaProvisioner for PartialFleet {
        fn replica_regions(&self) -> Vec<String> {
            self.0.replica_regions()
        }

        async fn ensure(&self, actor: &ActorKey, region: &str) -> Result<Vec<ReplicaTarget>> {
            let mut replicas = self.0.ensure(actor, region).await?;
            replicas.pop();
            Ok(replicas)
        }
    }

    #[test]
    fn one_bucket_scopes_mutable_metadata_and_immutable_snapshots_separately() -> Result<()> {
        let boundary = boundary("actors");
        let rules = boundary["accessBoundary"]["accessBoundaryRules"]
            .as_array()
            .unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["availableResource"], rules[1]["availableResource"]);
        assert!(
            rules[0]["availabilityCondition"]["expression"]
                .as_str()
                .unwrap()
                .contains("little-actors/v2/owners/")
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
                .contains("little-actors/v2/snapshots/")
        );
        Ok(())
    }
}
