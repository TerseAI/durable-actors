use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::actor::ActorKey;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WritePlan {
    pub stream: crate::replication::ReplicaStream,
    pub state_version: u64,
    pub object_name: String,
    pub expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<crate::replication::ReplicationTicket>,
}

#[async_trait]
pub trait SnapshotReader: Send + Sync {
    async fn read_snapshot(&self, region: &str, object: &str) -> Result<bytes::Bytes>;
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
    crate::storage_paths::snapshots(actor)
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

pub fn validate_bucket(bucket: &str) -> Result<()> {
    ensure!(
        !bucket.is_empty()
            && bucket.trim() == bucket
            && bucket.len() <= 222
            && !bucket.contains('/'),
        "invalid storage bucket"
    );
    Ok(())
}

fn validate_object_name(object_name: &str) -> Result<()> {
    ensure!(
        object_name.starts_with(crate::storage_paths::ROOT)
            && object_name.len() <= 1024
            && !object_name.chars().any(char::is_control),
        "actor state object name is invalid"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_paths_group_an_actor_and_validate_its_identity() -> Result<()> {
        let actor = ActorKey {
            actor_type: "Counter".into(),
            actor_id: "account.42".into(),
        };
        let object = snapshot_object_name(&actor, 7, "0123456789abcdef0123456789abcdef")?;
        assert!(object.starts_with("little-actors/v2/snapshots/"));
        assert!(
            object.ends_with("/Q291bnRlcg/YWNjb3VudC40Mg/0123456789abcdef0123456789abcdef/7.json")
        );
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
        assert!(validate_bucket("actors").is_ok());
        for bucket in ["", "gs://actors", " actors"] {
            assert!(validate_bucket(bucket).is_err());
        }
    }
}
