mod actor_host;
mod actor_runtime;
mod lease_maintenance;
mod process;
mod queues;
pub(crate) mod sockets;
pub(crate) mod storage;

use serde::{Deserialize, Serialize};
use std::fmt;

pub use self::process::{ActorHostConfig, serve_actor_host};
pub(crate) use self::{
    actor_host::ActorHost,
    lease_maintenance::{HostLeaseMaintainer, LeaseRenewalTask},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HostId(String);

impl HostId {
    pub fn new<S>(id: S) -> Self
    where
        S: Into<String>,
    {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostEndpoint {
    pub id: HostId,
    pub route: String,
}
