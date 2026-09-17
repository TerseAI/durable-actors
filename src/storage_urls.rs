use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::signer::Signer;
use google_cloud_storage::{builder::storage::SignedUrlBuilder, http::Method};
use serde::{Deserialize, Serialize};

use crate::{actor::ActorKey, placement::validate_region};

mod iam;
mod local;

pub use local::LocalStorage;

const SIGNED_URL_TTL: Duration = Duration::from_secs(60);
pub const STATE_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateWriteTicket {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<crate::replication::ReplicaStream>,
    pub state_version: u64,
    pub object_name: String,
    pub url: String,
    pub expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<crate::replication::ReplicationTicket>,
}

#[async_trait]
pub trait StorageUrlSigner: Send + Sync {
    fn uses_epoch_streams(&self) -> bool {
        false
    }
    fn durability(&self) -> crate::replication::DurabilityPolicy {
        crate::replication::DurabilityPolicy::new(0)
    }
    async fn read_url(&self, region: &str, object_name: &str) -> Result<String>;

    async fn archive_write_url(&self, _region: &str, _object_name: &str) -> Result<String> {
        anyhow::bail!("archive URL renewal is not supported")
    }

    async fn write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        state_version: u64,
    ) -> Result<StateWriteTicket>;

    fn regions(&self) -> Vec<String>;
}

#[derive(Clone)]
pub struct GcsStorageUrlSigner {
    buckets: HashMap<String, String>,
    signer: Signer,
}

impl GcsStorageUrlSigner {
    pub async fn from_adc(buckets: HashMap<String, String>) -> Result<Self> {
        Self::new(buckets, iam::from_adc().await?)
    }

    pub fn new(buckets: HashMap<String, String>, signer: Signer) -> Result<Self> {
        validate_buckets(&buckets)?;
        Ok(Self { buckets, signer })
    }
}

#[async_trait]
impl StorageUrlSigner for GcsStorageUrlSigner {
    async fn archive_write_url(&self, region: &str, object_name: &str) -> Result<String> {
        validate_object_name(object_name)?;
        SignedUrlBuilder::for_object(self.bucket(region)?, object_name)
            .with_method(Method::PUT)
            .with_expiration(SIGNED_URL_TTL)
            .with_header("content-type", STATE_CONTENT_TYPE)
            .with_query_param("ifGenerationMatch", "0")
            .sign_with(&self.signer)
            .await
            .context("sign GCS archive write URL")
    }
    async fn read_url(&self, region: &str, object_name: &str) -> Result<String> {
        validate_object_name(object_name)?;
        SignedUrlBuilder::for_object(self.bucket(region)?, object_name)
            .with_method(Method::GET)
            .with_expiration(SIGNED_URL_TTL)
            .sign_with(&self.signer)
            .await
            .context("sign GCS actor-state read URL")
    }

    async fn write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        state_version: u64,
    ) -> Result<StateWriteTicket> {
        actor.validate()?;
        ensure!(state_version > 0, "actor state version must be positive");
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let object_name = snapshot_object_name(actor, state_version, &nonce)?;
        let expires_at_ms = unix_millis()?
            .checked_add(i64::try_from(SIGNED_URL_TTL.as_millis())?)
            .context("signed state-write URL expiration overflow")?;
        let url = self.archive_write_url(region, &object_name).await?;
        Ok(StateWriteTicket {
            stream: None,
            replication: None,
            state_version,
            object_name,
            url,
            expires_at_ms,
        })
    }

    fn regions(&self) -> Vec<String> {
        let mut regions = self.buckets.keys().cloned().collect::<Vec<_>>();
        regions.sort();
        regions
    }
}

impl GcsStorageUrlSigner {
    fn bucket(&self, region: &str) -> Result<String> {
        validate_region(region)?;
        self.buckets
            .get(region)
            .map(|bucket| format!("projects/_/buckets/{bucket}"))
            .with_context(|| format!("sandbox region {region:?} has no Standard bucket"))
    }
}

pub fn snapshot_object_name(actor: &ActorKey, state_version: u64, nonce: &str) -> Result<String> {
    actor.validate()?;
    ensure!(state_version > 0, "actor state version must be positive");
    ensure!(
        nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "actor state object nonce is invalid"
    );
    Ok(format!(
        "{}{nonce}/{state_version}.json",
        snapshot_prefix(actor)?
    ))
}

pub(crate) fn snapshot_prefix(actor: &ActorKey) -> Result<String> {
    actor.validate()?;
    let hash = aws_lc_rs::digest::digest(
        &aws_lc_rs::digest::SHA256,
        actor.storage_key().as_str().as_bytes(),
    );
    Ok(format!(
        "snapshots/{}/{:02x}/{}/{}/",
        actor.namespace_id,
        hash.as_ref()[0],
        actor.actor_type,
        actor.actor_id
    ))
}

pub fn validate_snapshot_object_name(
    actor: &ActorKey,
    state_version: u64,
    object_name: &str,
) -> Result<()> {
    validate_object_name(object_name)?;
    let prefix = snapshot_prefix(actor)?;
    let mut parts = object_name.strip_prefix(&prefix).unwrap_or("").split('/');
    let valid = matches!(parts.next(), Some(nonce) if nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit()))
        && parts.next() == Some(format!("{state_version}.json").as_str())
        && parts.next().is_none();
    ensure!(valid, "actor state object name does not match its commit");
    Ok(())
}

pub fn validate_buckets(buckets: &HashMap<String, String>) -> Result<()> {
    ensure!(!buckets.is_empty(), "Standard bucket map must not be empty");
    for (region, bucket) in buckets {
        validate_region(region)?;
        ensure!(
            !bucket.is_empty()
                && bucket.trim() == bucket
                && bucket.len() <= 222
                && !bucket.contains('/'),
            "Standard bucket for region {region:?} is invalid"
        );
    }
    Ok(())
}

fn validate_object_name(object_name: &str) -> Result<()> {
    ensure!(
        object_name.starts_with("snapshots/")
            && object_name.len() <= 1024
            && !object_name.chars().any(char::is_control),
        "actor state object name is invalid"
    );
    Ok(())
}

fn unix_millis() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis(),
    )
    .context("system clock exceeds supported state-write timestamp range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_paths_group_an_actor_and_validate_its_identity() -> Result<()> {
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "account.42".into(),
        };
        let object = snapshot_object_name(&actor, 7, "0123456789abcdef0123456789abcdef")?;
        assert!(object.starts_with("snapshots/project-1/"));
        assert!(object.ends_with("/Counter/account.42/0123456789abcdef0123456789abcdef/7.json"));
        validate_snapshot_object_name(&actor, 7, &object)?;
        assert!(validate_snapshot_object_name(&actor, 8, &object).is_err());
        assert!(
            validate_snapshot_object_name(
                &ActorKey {
                    actor_id: "other".into(),
                    ..actor
                },
                7,
                &object
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn validates_the_only_storage_configuration() {
        assert!(
            validate_buckets(&HashMap::from([(
                "us-east".into(),
                "objects-us-east".into()
            )]))
            .is_ok()
        );
        assert!(validate_buckets(&HashMap::new()).is_err());
        assert!(
            validate_buckets(&HashMap::from([("bad/region".into(), "objects".into())])).is_err()
        );
    }
}
