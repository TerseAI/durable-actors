use super::*;
use crate::sandbox::{
    PreparedSandboxProvider, ResourceLimits, RuntimeTemplateRequest, routing::RuntimeBackend,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn mixed_deployment_prepares_only_custom_shapes_and_persists_routing() -> Result<()> {
    let substrate = Arc::new(PreparedProvider::default());
    let (service, admin, _) = fixture_with_substrate(10_000, Some(substrate.clone()))?;
    service
        .deploy_source(&admin, &source(), Some(&mixed_contract()?))
        .await?;
    let active = admin.current_deployment("default").await?.unwrap();
    let plan = active.runtime.as_ref().unwrap();
    assert_eq!(plan.backend("ChatRoom"), RuntimeBackend::Substrate);
    assert_eq!(plan.backend("Default"), RuntimeBackend::Gke);
    assert_eq!(plan.backend("Implicit"), RuntimeBackend::Gke);
    assert_eq!(
        substrate.prepared.lock().unwrap().as_slice(),
        &[(
            "north-america-central".into(),
            vec![ResourceLimits {
                cpu_millis: 2000,
                memory_mib: 512
            }]
        )]
    );
    Ok(())
}

#[tokio::test]
async fn default_only_deployment_does_not_prepare_substrate_snapshots() -> Result<()> {
    let substrate = Arc::new(PreparedProvider::default());
    substrate.fail.store(true, Ordering::SeqCst);
    let (service, admin, _) = fixture_with_substrate(10_000, Some(substrate.clone()))?;
    let contract = super::super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &source(), Some(&contract))
        .await?;
    assert!(substrate.prepared.lock().unwrap().is_empty());
    assert!(admin.current_deployment("default").await?.is_some());
    Ok(())
}

#[tokio::test]
async fn failed_custom_preparation_preserves_the_current_deployment_and_hosts() -> Result<()> {
    let substrate = Arc::new(PreparedProvider::default());
    let (service, admin, pods) = fixture_with_substrate(10_000, Some(substrate.clone()))?;
    service
        .deploy_source(&admin, &source(), Some(&mixed_contract()?))
        .await?;
    let active = admin.current_deployment("default").await?;
    substrate.fail.store(true, Ordering::SeqCst);
    let mut changed = source();
    changed.code_snapshot = Some(crate::sandbox::testing::code_artifact(2));
    assert!(
        service
            .deploy_source(&admin, &changed, Some(&mixed_contract()?))
            .await
            .is_err()
    );
    assert_eq!(admin.current_deployment("default").await?, active);
    assert!(pods.retired.lock().unwrap().is_empty());
    assert!(substrate.retired.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn replacing_a_mixed_deployment_retires_hosts_from_both_backends() -> Result<()> {
    let substrate = Arc::new(PreparedProvider::default());
    let (service, admin, pods) = fixture_with_substrate(10_000, Some(substrate.clone()))?;
    service
        .deploy_source(&admin, &source(), Some(&mixed_contract()?))
        .await?;
    let active = admin.current_deployment("default").await?.unwrap();
    let mut changed = source();
    changed.code_snapshot = Some(crate::sandbox::testing::code_artifact(2));
    service
        .deploy_source(&admin, &changed, Some(&mixed_contract()?))
        .await?;
    assert_eq!(
        pods.retired.lock().unwrap().as_slice(),
        &[active.host_config_key()]
    );
    assert_eq!(
        substrate.retired.lock().unwrap().as_slice(),
        &[active.host_config_key()]
    );
    Ok(())
}

#[tokio::test]
async fn secret_rotation_preserves_the_deployments_resolved_defaults() -> Result<()> {
    let substrate = Arc::new(PreparedProvider::default());
    let (service, admin, _) = fixture_with_substrate(10_000, Some(substrate))?;
    service
        .deploy_source(&admin, &source(), Some(&mixed_contract()?))
        .await?;
    let mut active = admin.current_deployment("default").await?.unwrap();
    active
        .runtime
        .as_mut()
        .unwrap()
        .default_resources
        .cpu_millis = 500;
    admin.register_deployment(&active, None).await?;
    let mut changed = active.clone();
    changed.secret_refs = vec!["rotated".into()];
    service
        .deploy_source(&admin, &changed, Some(&mixed_contract()?))
        .await?;
    assert_eq!(
        admin.current_deployment("default").await?.unwrap().runtime,
        active.runtime
    );
    Ok(())
}

#[tokio::test]
async fn host_readiness_uses_its_recorded_backend() -> Result<()> {
    let fixed = Arc::new(PreparedProvider::default());
    let substrate = Arc::new(PreparedProvider::default());
    let provisioner = SandboxHostProvisioner::new(
        fixed.clone(),
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invoke".into(),
            host_idle_timeout_ms: 10000,
        },
        super::super::tests::test_issuer()?,
        None,
    )
    .with_substrate(substrate.clone());
    let pod = RuntimeBackend::Gke.host_id("cfg.test");
    let sandbox = RuntimeBackend::Substrate.host_id("cfg.test");
    provisioner.wait_ready(&pod).await?;
    provisioner.wait_ready(&sandbox).await?;
    assert_eq!(fixed.waited.lock().unwrap().as_slice(), &[pod]);
    assert_eq!(substrate.waited.lock().unwrap().as_slice(), &[sandbox]);
    Ok(())
}

fn mixed_contract() -> Result<super::super::super::contracts::PublicActorContract> {
    let mut document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    document["actors"][0]["sandbox"] = serde_json::json!({"cpu":2,"memoryMiB":512});
    let mut default_actor = document["actors"][0].clone();
    default_actor["actorName"] = "Default".into();
    default_actor["socket"]["actorName"] = "Default".into();
    default_actor["sandbox"] =
        serde_json::json!({"cpu":0.25,"memoryMiB":128,"idleTimeoutMs":60000});
    document["actors"]
        .as_array_mut()
        .unwrap()
        .push(default_actor);
    super::super::super::contracts::PublicActorContract::new(document)
}

#[derive(Default)]
struct PreparedProvider {
    prepared: Mutex<Vec<(String, Vec<ResourceLimits>)>>,
    retired: Mutex<Vec<String>>,
    waited: Mutex<Vec<HostId>>,
    ensured: Mutex<Vec<EnsureHostRequest>>,
    fail: AtomicBool,
}

#[async_trait]
impl PreparedSandboxProvider for PreparedProvider {
    async fn prepare_runtime(&self, request: &RuntimeTemplateRequest) -> Result<()> {
        self.prepared
            .lock()
            .unwrap()
            .push((request.canonical_region.clone(), request.resources.clone()));
        ensure!(
            !self.fail.load(Ordering::SeqCst),
            "snapshot preparation failed"
        );
        Ok(())
    }
}

#[async_trait]
impl SandboxProvider for PreparedProvider {
    async fn wait_ready(&self, host: &HostId) -> Result<()> {
        self.waited.lock().unwrap().push(host.clone());
        Ok(())
    }
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("unexpected spare inspection")
    }
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        use crate::clock::{Clock, SystemClock};
        self.ensured.lock().unwrap().push(request.clone());
        let route = "http://host".to_owned();
        Ok(ActorHostHandle {
            host_id: request.host_id.clone(),
            route: route.clone(),
            canonical_region: request.canonical_region.clone(),
            owner_epoch: 1,
            provisioning: None,
            lease: Some(HostLease {
                id: request.host_id.clone(),
                session_id: request.session_id.clone(),
                route,
                expires_at_ms: SystemClock.now_ms()? + 30000,
            }),
        })
    }
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        self.retired
            .lock()
            .unwrap()
            .push(request.host_config_key.clone());
        Ok(HostTermination {
            provider: "test".into(),
            resource_ids: vec![],
        })
    }
}

#[tokio::test]
async fn mixed_activations_use_the_recorded_backend_and_exact_resources() -> Result<()> {
    let fixed = Arc::new(PreparedProvider::default());
    let substrate = Arc::new(PreparedProvider::default());
    let provisioner = provisioner(fixed.clone())?.with_substrate(substrate.clone());
    let mut deployment = source();
    deployment.sandboxes = mixed_contract()?.sandboxes()?;
    deployment.runtime = Some(crate::sandbox::routing::RuntimePlan::resolve(
        ResourceLimits::default(),
        &deployment.sandboxes,
        true,
    ));
    for actor_name in ["Default", "ChatRoom"] {
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: actor_name.into(),
            actor_id: "one".into(),
        };
        provisioner
            .ensure_actor_host(&deployment, "north-america-central", &actor, true, None)
            .await?;
    }
    for (provider, backend, cpu, memory) in [
        (&fixed, RuntimeBackend::Gke, 250, 128),
        (&substrate, RuntimeBackend::Substrate, 2000, 512),
    ] {
        let requests = provider.ensured.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].resources,
            ResourceLimits {
                cpu_millis: cpu,
                memory_mib: memory
            }
        );
        assert_eq!(RuntimeBackend::for_host(&requests[0].host_id), backend);
    }
    Ok(())
}

#[tokio::test]
async fn missing_substrate_prevents_partial_retirement_of_a_mixed_deployment() -> Result<()> {
    let fixed = Arc::new(PreparedProvider::default());
    let provisioner = provisioner(fixed.clone())?;
    let mut deployment = source();
    deployment.sandboxes = mixed_contract()?.sandboxes()?;
    deployment.runtime = Some(crate::sandbox::routing::RuntimePlan::resolve(
        ResourceLimits::default(),
        &deployment.sandboxes,
        true,
    ));
    assert!(
        provisioner
            .terminate_hosts(&deployment, &["north-america-central".into()])
            .await
            .is_err()
    );
    assert!(fixed.retired.lock().unwrap().is_empty());
    assert!(fixed.ensured.lock().unwrap().is_empty());
    Ok(())
}

fn provisioner(fixed: Arc<PreparedProvider>) -> Result<SandboxHostProvisioner> {
    Ok(SandboxHostProvisioner::new(
        fixed,
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invoke".into(),
            host_idle_timeout_ms: 10000,
        },
        super::super::tests::test_issuer()?,
        None,
    ))
}
