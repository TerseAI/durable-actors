mod access;
mod archive;
mod coordinator;
mod process;
mod server;
mod store;
mod transport;

use std::collections::HashSet;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub use access::{ReplicaAccess, ReplicaGrant};
pub use archive::{ArchiveTicket, archive_pending, start_archiver};
pub use coordinator::{ReplicaCatalog, ReplicaCoordinator, ReplicaManifest, ReplicaProvisioner};
pub use process::serve_replica_host;
pub use server::replica_router;
pub use store::ReplicaStore;
pub use transport::ReplicatedStateTransport;

pub const MAX_REPLICAS: usize = 8;
pub const DEFAULT_SPOOL_BYTES: u64 = 1024 * 1024 * 1024;

pub fn replica_count(get: &mut impl FnMut(&str) -> Option<String>) -> Result<usize> {
    let mode = get("DURABLE_OBJECT_DURABILITY").unwrap_or_else(|| "object_storage".into());
    let configured = get("DURABLE_OBJECT_REPLICA_COUNT");
    match mode.as_str() {
        "object_storage" => {
            ensure!(
                configured.as_deref().is_none_or(|count| count == "0"),
                "object_storage durability does not use replica hosts"
            );
            Ok(0)
        }
        "zonal" | "cross_region_preview" => {
            let count = configured.as_deref().unwrap_or("2").parse()?;
            ensure!(
                (1..=MAX_REPLICAS).contains(&count),
                "DURABLE_OBJECT_REPLICA_COUNT must be between 1 and {MAX_REPLICAS}"
            );
            Ok(count)
        }
        _ => anyhow::bail!(
            "DURABLE_OBJECT_DURABILITY supports object_storage, zonal, or cross_region_preview; strict regional modes are not implemented"
        ),
    }
}

pub fn replica_regions(
    get: &mut impl FnMut(&str) -> Option<String>,
    count: usize,
) -> Result<Vec<String>> {
    let configured = get("DURABLE_OBJECT_REPLICA_REGIONS");
    if get("DURABLE_OBJECT_DURABILITY").as_deref() != Some("cross_region_preview") {
        ensure!(
            configured.is_none_or(|value| value.is_empty() || value == "[]"),
            "explicit replica regions require cross_region_preview"
        );
        return Ok(Vec::new());
    }
    let regions: Vec<String> = serde_json::from_str(&configured.ok_or_else(|| {
        anyhow::anyhow!("cross_region_preview requires DURABLE_OBJECT_REPLICA_REGIONS")
    })?)?;
    ensure!(
        !regions.is_empty(),
        "cross_region_preview requires replica regions"
    );
    replica_destinations("", count, &regions)
}

pub fn replica_destinations(
    home: &str,
    count: usize,
    configured: &[String],
) -> Result<Vec<String>> {
    if configured.is_empty() {
        return Ok(vec![home.into(); count]);
    }
    ensure!(
        configured.len() == count,
        "replica region count must match replica host count"
    );
    let distinct: HashSet<_> = configured.iter().collect();
    ensure!(
        distinct.len() == count,
        "cross-region replica destinations must be distinct"
    );
    for region in configured {
        crate::placement::validate_region(region)?;
        ensure!(
            region != home,
            "cross-region replicas must be outside the actor home region"
        );
    }
    Ok(configured.to_vec())
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
                "zonal"
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
    fn validate(&self) -> Result<()> {
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
