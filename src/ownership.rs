//! Which host may run each actor. Every actor has one record, which the control plane claims
//! before it assigns a host and replaces by compare-and-swap in an [`OwnershipStore`].

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    bucket::{SnapshotStoreConfig, recovery::CheckpointProgress},
    host_leases::HostLease,
    placement::ObjectPlacement,
    storage::{SnapshotRef, StateStream, snapshot_object_name},
};

mod abandoned;
mod bucket;
mod claims;
mod moving;
mod postgres;

pub(crate) use bucket::BucketOwnership;
pub(crate) use claims::{Claim, Ownerships};
pub(crate) use moving::MovingOwnership;
pub(crate) use postgres::PostgresOwnership;

#[cfg(test)]
#[path = "../tests/support/ownership.rs"]
pub(crate) mod testing;

#[cfg(test)]
#[path = "../tests/unit/ownership.rs"]
mod tests;

#[async_trait]
pub(crate) trait OwnershipStore: Send + Sync {
    async fn load(&self, actor: &ActorKey) -> Result<Option<Stored>>;
    /// Writes `record` if its actor's record is still at `expected`, or still absent for `None`.
    async fn replace(&self, record: &Ownership, expected: Option<Version>) -> Result<bool>;
    /// Unsealed records, which include every live lease.
    async fn unsealed(&self) -> Result<Vec<Stored>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    Bucket(i64),
    Postgres(i64),
}

#[derive(Clone)]
pub(crate) struct Stored {
    pub version: Version,
    pub record: Ownership,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Ownership {
    pub persistence: SnapshotStoreConfig,
    #[serde(default)]
    pub sealed: bool,
    pub lease: HostLease,
    pub mutation: String,
    pub actor: ActorKey,
    pub epoch: u64,
    pub region: String,
    pub base: Option<SnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_progress: Option<CheckpointProgress>,
}

impl Ownership {
    pub(crate) fn placement(&self) -> ObjectPlacement {
        let mut placement = ObjectPlacement {
            lease: self.lease.clone(),
            object: self.actor.storage_key(),
            owner: self.lease.id.clone(),
            owner_epoch: self.epoch,
            home_region: self.region.clone(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        };
        apply_snapshot(&mut placement, self.base.as_ref());
        placement
    }

    pub(crate) fn stream(&self) -> Result<StateStream> {
        let name = snapshot_object_name(&self.actor, 1, &format!("{:032x}", self.epoch))?;
        Ok(StateStream {
            session: self.lease.session_id.clone(),
            prefix: name.strip_suffix("1.json").unwrap().into(),
            owner_epoch: self.epoch,
            base_version: self.base.as_ref().map_or(0, |base| base.state_version),
        })
    }
}

/// A record the control plane read, which a later claim can replace without reading it again.
#[derive(Clone, Serialize, Deserialize)]
pub struct OwnershipHint {
    version: Version,
    record: Ownership,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    node: Option<String>,
}

impl OwnershipHint {
    pub(crate) fn sealed(&self) -> bool {
        self.record.sealed
    }

    pub(crate) fn placement(&self) -> ObjectPlacement {
        self.record.placement()
    }

    pub(crate) fn node(&self) -> Option<&str> {
        self.node.as_deref()
    }

    pub(crate) fn on_node(self, node: Option<String>) -> Self {
        Self { node, ..self }
    }
}

impl From<Stored> for OwnershipHint {
    fn from(stored: Stored) -> Self {
        Self {
            version: stored.version,
            record: stored.record,
            node: None,
        }
    }
}

/// The ownership a control plane claimed for a host before assigning it the actor.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ActorClaim {
    pub record: Ownership,
    /// The sealed predecessor's checkpoint progress, which this session's checkpoints continue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<CheckpointProgress>,
}

impl ActorClaim {
    pub(crate) fn is_new(&self) -> bool {
        self.record.base.is_none()
    }
}

/// What a host reports as it ends its session: the state it last acknowledged.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SessionCheckpoint {
    pub snapshot: Option<SnapshotRef>,
    pub progress: Option<CheckpointProgress>,
}

impl SessionCheckpoint {
    /// Whether the sealed session found a new base due.
    pub(crate) fn base_due(&self) -> bool {
        self.progress.is_some_and(|progress| progress.base_due)
    }
}

pub(crate) fn apply_snapshot(placement: &mut ObjectPlacement, snapshot: Option<&SnapshotRef>) {
    if let Some(snapshot) = snapshot {
        placement.state_version = snapshot.state_version;
        placement.state_object = Some(snapshot.object.clone());
        placement.last_request_id = Some(snapshot.request_id.clone());
    }
}
