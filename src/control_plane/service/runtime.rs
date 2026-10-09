use super::*;
use crate::sandbox::{
    ResourceLimits, RuntimeTemplateRequest,
    routing::{RuntimeBackend, RuntimePlan},
};

impl SandboxHostProvisioner {
    pub(crate) fn with_substrate(
        mut self,
        provider: Arc<dyn crate::sandbox::PreparedSandboxProvider>,
    ) -> Self {
        self.substrate = Some(provider);
        self
    }

    pub(super) async fn prepare_runtime_deployment(
        &self,
        prepared: &mut HostLaunchSpec,
        previous: Option<&HostLaunchSpec>,
        region: &str,
    ) -> Result<()> {
        let plan = self.resolve_runtime_plan(prepared, previous);
        let profiles = runtime_profiles(prepared, &plan, region);
        prepared.runtime = Some(plan);
        if profiles.is_empty() {
            return Ok(());
        }
        let substrate = self
            .substrate
            .as_ref()
            .context("deployment requires the Substrate runtime")?;
        for (canonical_region, resources) in profiles {
            substrate
                .prepare_runtime(&RuntimeTemplateRequest {
                    code_snapshot: prepared.code_snapshot.clone(),
                    image_ref: prepared.image_ref.clone(),
                    canonical_region,
                    resources,
                    jwt_public_keys: self.issuer.verifier_keys_json()?,
                    jwt_issuer: self.runtime.jwt_issuer.clone(),
                })
                .await?;
        }
        Ok(())
    }

    fn resolve_runtime_plan(
        &self,
        prepared: &HostLaunchSpec,
        previous: Option<&HostLaunchSpec>,
    ) -> RuntimePlan {
        previous
            .filter(|previous| {
                previous.code_snapshot == prepared.code_snapshot
                    && previous.sandboxes == prepared.sandboxes
            })
            .and_then(|previous| previous.runtime.clone())
            .unwrap_or_else(|| {
                RuntimePlan::resolve(
                    self.default_resources(),
                    &prepared.sandboxes,
                    self.substrate.is_some(),
                )
            })
    }

    pub(super) fn default_resources(&self) -> ResourceLimits {
        self.pool
            .as_ref()
            .map(|pool| pool.config.resources.clone())
            .unwrap_or_default()
    }

    pub(super) fn backend(&self, spec: &HostLaunchSpec, actor: &str) -> RuntimeBackend {
        spec.runtime
            .as_ref()
            .map(|runtime| runtime.backend(actor))
            .unwrap_or(RuntimeBackend::Gke)
    }

    pub(super) fn provider_for(&self, backend: RuntimeBackend) -> Result<&dyn SandboxProvider> {
        match backend {
            RuntimeBackend::Gke => Ok(self.provider.as_ref()),
            RuntimeBackend::Substrate => Ok(self
                .substrate
                .as_ref()
                .context("deployment requires the Substrate runtime")?
                .as_ref()),
        }
    }
}

fn runtime_profiles(
    spec: &HostLaunchSpec,
    plan: &RuntimePlan,
    region: &str,
) -> std::collections::BTreeMap<String, Vec<ResourceLimits>> {
    let mut profiles = std::collections::BTreeMap::<String, Vec<ResourceLimits>>::new();
    for (actor, options) in &spec.sandboxes {
        if plan.backend(actor) != RuntimeBackend::Substrate {
            continue;
        }
        let resources = options.resources(plan.default_resources.clone());
        for target in options
            .regions
            .clone()
            .unwrap_or_else(|| vec![region.into()])
        {
            let shapes = profiles.entry(target).or_default();
            if !shapes.contains(&resources) {
                shapes.push(resources.clone());
            }
        }
    }
    profiles
}
