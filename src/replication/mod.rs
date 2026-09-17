pub(crate) mod access;
mod archive;
mod process;
mod server;
mod store;
mod stream;
mod transport;

use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub use access::{ReplicaAccess, ReplicaGrant};
pub use archive::{ArchiveTicket, archive_pending, start_archiver};
pub use process::serve_replica_host;
pub use server::replica_router;
pub use store::{FileReplicaStore, PendingSnapshot, ReplicaStore};
pub use stream::{ReplicaStream, SessionHead, SnapshotRef, StreamHead};
pub use transport::ReplicatedStateTransport;

pub const MAX_REPLICAS: usize = 8;
pub const DEFAULT_SPOOL_BYTES: u64 = 1024 * 1024 * 1024;

#[async_trait::async_trait]
pub trait ReplicaProvisioner: Send + Sync {
    fn replica_regions(&self) -> Vec<String> {
        Vec::new()
    }

    async fn ensure(
        &self,
        actor: &crate::actor::ActorKey,
        region: &str,
        count: usize,
    ) -> Result<Vec<ReplicaTarget>>;
}

pub fn replica_regions(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Vec<String>> {
    let regions: Vec<String> = serde_json::from_str(
        &get("DURABLE_OBJECT_REPLICA_REGIONS").unwrap_or_else(|| "[]".into()),
    )?;
    ensure!(
        regions.len() <= MAX_REPLICAS,
        "at most {MAX_REPLICAS} replicas are supported"
    );
    for region in &regions {
        crate::placement::validate_region(region)?;
    }
    Ok(regions)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurabilityPolicy {
    pub mode: String,
    pub replica_count: usize,
    pub runtime_version: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_regions: Vec<String>,
}

impl DurabilityPolicy {
    pub fn new(replica_count: usize) -> Self {
        Self {
            mode: if replica_count == 0 {
                "object_storage"
            } else {
                "replicated"
            }
            .into(),
            replica_count,
            runtime_version: env!("CARGO_PKG_VERSION").into(),
            replica_regions: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationTicket {
    pub required_replicas: usize,
    pub replicas: Vec<ReplicaTarget>,
    pub archive_url: String,
}

impl ReplicationTicket {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_REPLICAS).contains(&self.required_replicas),
            "invalid replica count"
        );
        ensure!(
            self.replicas.len() == self.required_replicas,
            "incomplete replica set"
        );
        let hosts: HashSet<_> = self.replicas.iter().map(|peer| &peer.host_id).collect();
        let urls: HashSet<_> = self.replicas.iter().map(|peer| &peer.url).collect();
        ensure!(
            hosts.len() == self.required_replicas && urls.len() == self.required_replicas,
            "replica hosts must be distinct"
        );
        ensure!(
            !self.archive_url.is_empty(),
            "replica archive capability is missing"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaTarget {
    pub host_id: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub region: String,
}

#[derive(Default)]
pub(crate) struct ReplicaSet(pub Vec<ReplicaTarget>);
#[async_trait::async_trait]
impl ReplicaProvisioner for ReplicaSet {
    async fn ensure(
        &self,
        _: &crate::actor::ActorKey,
        _: &str,
        count: usize,
    ) -> Result<Vec<ReplicaTarget>> {
        ensure!(
            self.0.len() == count,
            "replica membership does not match configuration"
        );
        Ok(self.0.clone())
    }
}
