use std::{collections::HashMap, sync::Arc, time::Duration};

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
    pub authority_bucket: String,
    pub state_buckets: HashMap<String, String>,
    pub region: String,
    pub replica_secret: String,
    pub replicas: Vec<ReplicaTarget>,
    pub replica_count: usize,
    pub token: StorageToken,
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
    credentials: AccessTokenCredentials,
    http: reqwest::Client,
    authority_bucket: String,
    state_buckets: HashMap<String, String>,
    fleet: Arc<dyn ReplicaProvisioner>,
    replicas: ReplicaAccess,
    count: usize,
    tokens: moka::future::Cache<String, StorageToken>,
}

impl RuntimeAccess {
    pub fn new(
        authority_bucket: String,
        state_buckets: HashMap<String, String>,
        fleet: Arc<dyn ReplicaProvisioner>,
        replicas: ReplicaAccess,
        count: usize,
    ) -> Result<Self> {
        Ok(Self {
            credentials: Builder::default().build_access_token_credentials()?,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()?,
            authority_bucket,
            state_buckets,
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
            authority_bucket: self.authority_bucket.clone(),
            state_buckets: self.state_buckets.clone(),
            region: region.into(),
            replica_secret: self.replicas.delegate_secret(namespace)?,
            replicas,
            replica_count: self.count,
            token,
        })?)
    }

    pub async fn issue(&self, namespace: &str) -> Result<StorageToken> {
        if let Some(token) = self.tokens.get(namespace).await {
            if token.expires_at_ms > SystemClock.now_ms()? + 60_000 {
                return Ok(token);
            }
            self.tokens.invalidate(namespace).await;
        }
        self.tokens
            .try_get_with(namespace.to_owned(), self.exchange(namespace))
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }

    async fn exchange(&self, namespace: &str) -> Result<StorageToken> {
        let boundary = boundary(&self.authority_bucket, &self.state_buckets, namespace)?;
        let source = self.credentials.access_token().await?;
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

fn boundary(authority: &str, states: &HashMap<String, String>, namespace: &str) -> Result<Value> {
    crate::actor::ActorScope {
        namespace_id: namespace.into(),
    }
    .validate()?;
    let namespace_key = super::component(namespace);
    let mut rules = vec![rule(
        authority,
        &[
            format!("runtime/owners/{namespace_key}/"),
            format!("runtime/hosts/{namespace_key}/"),
            format!("runtime/sessions/{namespace_key}/"),
        ],
        &["storage.objectUser"],
    )];
    let mut buckets: Vec<_> = states.values().collect();
    buckets.sort();
    buckets.dedup();
    for bucket in buckets {
        rules.push(rule(
            bucket,
            &[format!("snapshots/{namespace}/")],
            &["storage.objectViewer", "storage.objectCreator"],
        ));
    }
    ensure!(
        rules.len() <= 10,
        "GCS credential boundary exceeds ten buckets"
    );
    Ok(json!({"accessBoundary": {"accessBoundaryRules": rules}}))
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
    fn boundary_scopes_coordination_and_immutable_snapshots() -> Result<()> {
        let rules = boundary(
            "authority",
            &HashMap::from([("us-east".into(), "states".into())]),
            "project.a",
        )?;
        let rules = rules["accessBoundary"]["accessBoundaryRules"]
            .as_array()
            .unwrap();
        assert_eq!(rules.len(), 2);
        assert!(
            rules[0]["availabilityCondition"]["expression"]
                .as_str()
                .unwrap()
                .contains("runtime/owners/cHJvamVjdC5h/")
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
                .contains(".startsWith('snapshots/project.a/')")
        );
        assert!(boundary("authority", &HashMap::new(), "../escape").is_err());
        Ok(())
    }
}
