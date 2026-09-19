use crate::{actor::ActorKey, host::HostId};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub const MAX_HOST_LEASE_DURATION_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLease {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLeaseRequest {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub duration_ms: u64,
}

impl HostLeaseRequest {
    pub fn validate_duration(&self) -> Result<()> {
        ensure!(self.duration_ms > 0, "host lease duration must be positive");
        ensure!(
            self.duration_ms <= MAX_HOST_LEASE_DURATION_MS,
            "host lease duration must not exceed {MAX_HOST_LEASE_DURATION_MS}ms"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLeaseStatus {
    pub lease: Option<HostLease>,
    #[serde(rename = "registry_now_ms")]
    pub store_now_ms: u64,
}

impl HostLeaseStatus {
    pub fn is_active(&self) -> bool {
        self.lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at_ms > self.store_now_ms)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActorSocketInventory {
    pub actor: ActorKey,
    pub connections: Vec<crate::actor::ActorSocketConnection>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActorQueueInventory {
    pub actor: ActorKey,
    pub waiting: Vec<WaitingOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitingOperation {
    pub id: String,
    pub operation: String,
}

#[async_trait]
pub trait HostLeaseRegistry: Send + Sync {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease>;
    async fn register_with_residents(
        &self,
        request: &HostLeaseRequest,
        _residents: Option<&[ActorKey]>,
    ) -> Result<HostLease> {
        self.register(request).await
    }

    async fn register_with_inventory(
        &self,
        request: &HostLeaseRequest,
        residents: Option<&[ActorKey]>,
        _sockets: &[ActorSocketInventory],
        _queues: Option<&[ActorQueueInventory]>,
    ) -> Result<HostLease> {
        self.register_with_residents(request, residents).await
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()>;
}

#[async_trait]
pub trait HostLeaseStore: HostLeaseRegistry {
    async fn inventory_status(
        &self,
        id: &HostId,
    ) -> Result<(
        HostLeaseStatus,
        Option<Vec<ActorKey>>,
        Vec<ActorSocketInventory>,
        Option<Vec<ActorQueueInventory>>,
    )> {
        let (status, residents) = self.residency_status(id).await?;
        Ok((status, residents, vec![], None))
    }
    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus>;
    async fn residency_status(
        &self,
        id: &HostId,
    ) -> Result<(HostLeaseStatus, Option<Vec<ActorKey>>)> {
        Ok((self.lease_status(id).await?, None))
    }
}

#[cfg(test)]
#[path = "../tests/unit/host_leases.rs"]
mod tests;
