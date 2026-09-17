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
    fn replica_regions(&self) -> Vec<String>;

    async fn ensure(
        &self,
        actor: &crate::actor::ActorKey,
        region: &str,
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
    pub fn new(replica_regions: Vec<String>) -> Self {
        let replica_count = replica_regions.len();
        Self {
            mode: if replica_count == 0 {
                "object_storage"
            } else {
                "replicated"
            }
            .into(),
            replica_count,
            runtime_version: env!("CARGO_PKG_VERSION").into(),
            replica_regions,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationTicket {
    pub replicas: Vec<ReplicaTarget>,
    pub archive_url: String,
}

impl ReplicationTicket {
    pub(crate) fn validate(&self) -> Result<()> {
        let count = self.replicas.len();
        ensure!((1..=MAX_REPLICAS).contains(&count), "invalid replica count");
        let hosts: HashSet<_> = self.replicas.iter().map(|peer| &peer.host_id).collect();
        let urls: HashSet<_> = self.replicas.iter().map(|peer| &peer.url).collect();
        ensure!(
            hosts.len() == count && urls.len() == count,
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
    fn replica_regions(&self) -> Vec<String> {
        self.0
            .iter()
            .map(|replica| replica.region.clone())
            .collect()
    }

    async fn ensure(&self, _: &crate::actor::ActorKey, _: &str) -> Result<Vec<ReplicaTarget>> {
        Ok(self.0.clone())
    }
}
