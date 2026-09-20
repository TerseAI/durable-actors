use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::{
    control_plane::ControlPlaneClient,
    replication::{ReplicaProvisioner, ReplicaScope, ReplicaTarget},
};

pub(super) struct HostReplicaProvisioner {
    pub client: Arc<ControlPlaneClient>,
    pub regions: Vec<String>,
}

#[async_trait]
impl ReplicaProvisioner for HostReplicaProvisioner {
    fn replica_regions(&self) -> Vec<String> {
        self.regions.clone()
    }

    async fn ensure(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        self.repair(scope, &[]).await
    }

    async fn repair(&self, _: &ReplicaScope, failed: &[String]) -> Result<Vec<ReplicaTarget>> {
        if self.regions.is_empty() {
            return Ok(vec![]);
        }
        self.client.ensure_replicas(failed.to_vec()).await
    }
}
