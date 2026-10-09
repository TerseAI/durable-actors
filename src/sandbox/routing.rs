use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ResourceLimits;
use crate::{control_plane::contracts::SandboxOptions, host::HostId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RuntimeBackend {
    Gke,
    Substrate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimePlan {
    pub default_resources: ResourceLimits,
    pub actors: BTreeMap<String, RuntimeBackend>,
}

impl RuntimePlan {
    pub fn resolve(
        default_resources: ResourceLimits,
        options: &BTreeMap<String, SandboxOptions>,
        hybrid: bool,
    ) -> Self {
        let actors = options
            .iter()
            .map(|(actor, options)| {
                let backend = if hybrid
                    && options.resources(default_resources.clone()) != default_resources
                {
                    RuntimeBackend::Substrate
                } else {
                    RuntimeBackend::Gke
                };
                (actor.clone(), backend)
            })
            .collect();
        Self {
            default_resources,
            actors,
        }
    }

    pub fn backend(&self, actor: &str) -> RuntimeBackend {
        self.actors
            .get(actor)
            .copied()
            .unwrap_or(RuntimeBackend::Gke)
    }

    pub fn uses_substrate(&self) -> bool {
        self.actors
            .values()
            .any(|backend| *backend == RuntimeBackend::Substrate)
    }
}

impl RuntimeBackend {
    pub fn host_id(self, config: &str) -> HostId {
        let prefix = match self {
            Self::Gke => "",
            Self::Substrate => "substrate-",
        };
        HostId::new(format!("host.v3.{config}.{prefix}{}", uuid::Uuid::new_v4()))
    }

    pub fn for_host(host: &HostId) -> Self {
        match host.as_str().rsplit('.').next() {
            Some(suffix) if suffix.starts_with("substrate-") => Self::Substrate,
            _ => Self::Gke,
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/routing.rs"]
mod tests;
