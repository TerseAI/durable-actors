pub(crate) mod access;
mod process;
mod server;
mod spare;
pub(crate) use spare::ReplicaAssignment;
mod store;
mod stream;
mod transport;
mod wire;

use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub use access::{ReplicaAccess, ReplicaGrant};
pub use process::serve_replica_host;
pub use server::replica_routes;
pub use store::{FileReplicaStore, ReplicaStore};
pub use stream::{ReplicaStream, SessionHead, SnapshotRef, StreamHead};
pub use transport::ReplicatedStateTransport;

pub const MAX_REPLICAS: usize = 8;
pub const DEFAULT_REPLICA_BYTES: u64 = 1024 * 1024 * 1024;

#[async_trait::async_trait]
pub trait ReplicaProvisioner: Send + Sync {
    fn replica_regions(&self) -> Vec<String>;

    async fn ensure(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>>;

    async fn repair(&self, scope: &ReplicaScope, _failed: &[String]) -> Result<Vec<ReplicaTarget>> {
        self.ensure(scope).await
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaScope {
    pub actor: crate::actor::ActorKey,
    pub host: crate::host::HostId,
    pub session: String,
    pub region: String,
}

impl ReplicaScope {
    pub fn identity(&self) -> String {
        format!(
            "{}/",
            crate::storage_paths::session(&self.host, &self.session)
        )
    }
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
pub struct ReplicationTicket {
    pub replicas: Vec<ReplicaTarget>,
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

    async fn ensure(&self, _: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        Ok(self.0.clone())
    }
}
