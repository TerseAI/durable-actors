use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    admin::{AdminRegistry, HostLaunchSpec},
    contracts::{PublicActorContract, PublishedContract},
};

/// Stores opaque deployment documents; the runtime owns their validation and schema.
#[async_trait]
pub trait DeploymentCatalog: Send + Sync {
    async fn get(&self, project: &str) -> Result<Option<Value>>;
    async fn list(&self) -> Result<Vec<Value>>;
    async fn publish(&self, project: &str, document: Value) -> Result<bool>;
    async fn remove(&self, project: &str) -> Result<()>;
}

pub(super) struct CatalogRegistry(pub Arc<dyn DeploymentCatalog>);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    spec: HostLaunchSpec,
    contract: PublishedContract,
}

#[async_trait]
impl AdminRegistry for CatalogRegistry {
    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        let contract = contract.context("hosted deployments require a compiled public contract")?;
        let manifest = Manifest {
            spec: spec.clone(),
            contract: PublishedContract::new(contract),
        };
        manifest.validate()?;
        self.0
            .publish(&spec.project_id, serde_json::to_value(manifest)?)
            .await
    }

    async fn deployment_contract(&self, project: &str) -> Result<Option<PublishedContract>> {
        Ok(self.load(project).await?.map(|manifest| manifest.contract))
    }

    async fn launch_spec(&self, project: &str) -> Result<Option<HostLaunchSpec>> {
        Ok(self.load(project).await?.map(|manifest| manifest.spec))
    }

    async fn launch_specs(&self) -> Result<Vec<HostLaunchSpec>> {
        self.0
            .list()
            .await?
            .into_iter()
            .map(|value| Ok(Manifest::decode(value)?.spec))
            .collect()
    }

    async fn remove_deployment(&self, project: &str) -> Result<()> {
        self.0.remove(project).await
    }
}

impl CatalogRegistry {
    async fn load(&self, project: &str) -> Result<Option<Manifest>> {
        self.0
            .get(project)
            .await?
            .map(|value| {
                let manifest = Manifest::decode(value)?;
                ensure!(
                    manifest.spec.project_id == project,
                    "catalog returned another project"
                );
                Ok(manifest)
            })
            .transpose()
    }
}

impl Manifest {
    fn decode(value: Value) -> Result<Self> {
        let manifest: Self = serde_json::from_value(value)?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        ensure!(
            self.spec.code_snapshot.is_some(),
            "deployment must contain a published code snapshot"
        );
        let contract = PublicActorContract::new(self.contract.contract.clone())?;
        ensure!(
            contract.hash() == self.contract.contract_hash,
            "deployment contract hash mismatch"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/catalog.rs"]
mod tests;
