#[cfg(test)]
pub(crate) mod testing;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{actor_state::ActorStorageKey, host::HostId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub owner_epoch: u64,
    pub home_region: String,
    pub state_version: u64,
    pub state_object: Option<String>,
    pub last_request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementClaim {
    Acquired(ObjectPlacement),
    Current(ObjectPlacement),
}

#[async_trait]
pub trait ObjectPlacementStore: Send + Sync {
    async fn get_owner(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        self.get(object).await
    }

    async fn matches_lease(
        &self,
        _placement: &ObjectPlacement,
        _lease: &crate::host_leases::HostLease,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>>;

    async fn list_committed(
        &self,
        namespace: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>>;
}

pub fn validate_region(region: &str) -> Result<()> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            }),
        "sandbox region is invalid"
    );
    Ok(())
}
