use super::*;
use crate::{
    control_plane::{ActorTokenPurpose, admin::LocalAdminRegistry},
    placement::testing::LocalObjectPlacementStore,
    sandbox::ActorHostHandle,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[tokio::test]
async fn openapi_is_available_without_credentials_or_a_deployment() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/openapi.yaml", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let response = reqwest::get(url).await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/yaml");
    let expected = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/reference/openapi.yaml"
    ))?;
    assert_eq!(response.text().await?, expected);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn bundle_registration_preserves_code_and_contract_during_secret_rotation() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let bundle = crate::bundle::ActorBundle::decode(&crate::sandbox::testing::code_artifact(1))?;
    let dependency_image = format!("registry/runtime@sha256:{}", "a".repeat(64));
    let mut document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    document["actors"][0]["sandbox"] = serde_json::json!({"cpu":2,"memoryMiB":4096});
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&serde_json::json!({"bundle":bundle,"dependencyImage":dependency_image,"contract":document}))
        .send()
        .await?
        .error_for_status()?;
    let active = admin.current_deployment("default").await?.unwrap();
    assert_eq!(active.image_ref, dependency_image);
    assert_eq!(active.actor_entrypoint.as_deref(), Some("actors.mjs"));
    assert_eq!(active.code_snapshot, Some(bundle.encode()?));
    assert!(active.sandboxes.contains_key("ChatRoom"));
    let mut current: serde_json::Value = client
        .get(&url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(current["bundle"], serde_json::to_value(&bundle)?);
    assert_eq!(current["dependencyImage"], dependency_image);
    current["secretRefs"] = serde_json::json!(["rotated"]);
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    let rotated = admin.current_deployment("default").await?.unwrap();
    assert_eq!(rotated.code_snapshot, active.code_snapshot);
    assert_eq!(rotated.sandboxes, active.sandboxes);
    assert_ne!(rotated.host_config_key(), active.host_config_key());
    assert_eq!(
        admin
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        document
    );
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[active.host_config_key()]
    );
    for invalid in [
        serde_json::json!({"bundle":bundle,"dependencyImage":"registry/runtime:latest","contract":document}),
        serde_json::json!({"dependencyImage":dependency_image,"contract":document}),
        serde_json::json!({"bundle":bundle,"dependencyImage":dependency_image.replace(&"a".repeat(64), &"b".repeat(64))}),
    ] {
        assert_eq!(
            client
                .put(&url)
                .bearer_auth("api-key")
                .json(&invalid)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            admin.current_deployment("default").await?,
            Some(rotated.clone())
        );
    }
    // A new bundle must carry a contract before replacing the running deployment.
    current["bundle"]["files"][0]["generation"] = 2.into();
    assert_eq!(
        client
            .put(&url)
            .bearer_auth("api-key")
            .json(&current)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(admin.current_deployment("default").await?, Some(rotated));
    current["contract"] = contract();
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    assert!(
        admin
            .current_deployment("default")
            .await?
            .unwrap()
            .sandboxes
            .is_empty()
    );
    server.abort();
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn deployment_publishes_while_warmup_is_pending_and_retries_failures() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let stop = tokio_util::sync::CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let worker = super::super::preparation::start(service.clone(), stop.clone());
    let original = source();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    provider.preparation_seconds.store(600, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    service
        .deploy_source(&admin, &original, Some(&contract))
        .await?;
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(admin.current_deployment("default").await?.is_some());
    settle().await;
    assert_eq!(provider.prepared.lock().unwrap().len(), 1);

    let mut changed = original.clone();
    changed.code_snapshot = Some(crate::sandbox::testing::code_artifact(2));
    provider.fail_preparation.store(true, Ordering::SeqCst);
    service
        .deploy_source(&admin, &changed, Some(&contract))
        .await?;
    settle().await;
    assert_eq!(
        admin
            .current_deployment("default")
            .await?
            .unwrap()
            .code_snapshot,
        changed.code_snapshot
    );
    assert_eq!(provider.prepared.lock().unwrap().len(), 2);

    provider.fail_preparation.store(false, Ordering::SeqCst);
    provider.preparation_seconds.store(0, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(31)).await;
    settle().await;
    assert_eq!(provider.prepared.lock().unwrap().len(), 3);
    changed.secret_refs.push("rotated".into());
    service.deploy_source(&admin, &changed, None).await?;
    settle().await;
    assert_eq!(provider.prepared.lock().unwrap().len(), 3);
    stop.cancel();
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn background_warmup_starts_all_deployments_and_recovers_after_restart() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    provider.preparation_seconds.store(600, Ordering::SeqCst);
    for n in 0..50 {
        let mut deployment = source();
        deployment.project_id = format!("project-{n}");
        service
            .deploy_source(&admin, &deployment, Some(&contract))
            .await?;
    }
    let stop = tokio_util::sync::CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let worker = super::super::preparation::start(service.clone(), stop.clone());
    settle().await;
    let initial = provider.prepared.lock().unwrap().clone();
    assert_eq!(initial.len(), 50);
    service
        .delete_deployment(&admin, &initial[0].project_id)
        .await?;
    settle().await;
    assert_eq!(provider.prepared.lock().unwrap().len(), 50);
    stop.cancel();
    worker.await?;
    provider.prepared.lock().unwrap().clear();
    provider.preparation_seconds.store(0, Ordering::SeqCst);

    let stop = tokio_util::sync::CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let worker = super::super::preparation::start(service, stop.clone());
    settle().await;
    let prepared = provider.prepared.lock().unwrap().clone();
    assert_eq!(prepared.len(), 49);
    assert!(
        prepared
            .iter()
            .all(|d| d.project_id != initial[0].project_id)
    );
    stop.cancel();
    worker.await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn background_preparation_still_bounds_stalled_work() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &source(), Some(&contract))
        .await?;
    let active = admin.current_deployment("default").await?.unwrap();
    let deployment = crate::sandbox::SandboxDeployment {
        project_id: active.project_id.clone(),
        image_ref: active.image_ref.clone(),
        code_snapshot: active.code_snapshot.clone(),
        canonical_region: service.default_region().into(),
    };
    provider.preparation_seconds.store(1201, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let error = service
        .prepare_registered_deployment(&deployment)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("deployment preparation timed out")
    );
    assert_eq!(started.elapsed(), Duration::from_secs(1200));
    assert_eq!(admin.current_deployment("default").await?, Some(active));
    Ok(())
}

async fn settle() {
    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }
}

#[tokio::test]
async fn compiled_artifact_selects_the_language_image() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    for (entrypoint, image) in [("actors.mjs", "im-runtime"), ("actors.pyz", "im-python")] {
        let mut source = source();
        let mut bundle =
            crate::bundle::ActorBundle::decode(source.code_snapshot.as_ref().unwrap())?;
        bundle.files[0].path = entrypoint.into();
        bundle.files[0].object = format!(
            "durable-actors/v3/artifacts/00000000-0000-4000-8000-000000000001/{entrypoint}"
        );
        source.actor_entrypoint = Some(entrypoint.into());
        source.code_snapshot = Some(bundle.encode()?);
        service
            .deploy_source(&admin, &source, Some(&contract))
            .await?;
        let active = admin.current_deployment("default").await?.unwrap();
        assert_eq!(active.image_ref, image);
        assert_eq!(active.actor_entrypoint.as_deref(), Some(entrypoint));
    }
    Ok(())
}

#[tokio::test]
async fn bundle_registration_validates_immutable_artifacts_before_retiring_hosts() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let source = source();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &source, Some(&contract))
        .await?;
    let active = admin.current_deployment("default").await?;
    for mutation in ["bucket", "object", "generation", "sha256", "path"] {
        let mut bundle =
            crate::bundle::ActorBundle::decode(source.code_snapshot.as_ref().unwrap())?;
        match mutation {
            "bucket" => bundle.bucket = "another-bucket".into(),
            "object" => bundle.files[0].object = "outside/actors.mjs".into(),
            "generation" => bundle.files[0].generation = 0,
            "sha256" => bundle.files[0].sha256 = "bad".into(),
            _ => bundle.files[0].path = "../actors.mjs".into(),
        }
        if let Ok(snapshot) = bundle.encode() {
            let mut invalid = source.clone();
            invalid.code_snapshot = Some(snapshot);
            assert!(
                service
                    .deploy_source(&admin, &invalid, Some(&contract))
                    .await
                    .is_err()
            );
        }
        assert_eq!(admin.current_deployment("default").await?, active);
    }
    assert!(provider.retired.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn bundle_contracts_are_scoped_to_the_project() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let first = source();
    let mut second = first.clone();
    second.project_id = "team-b".into();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &first, Some(&contract))
        .await?;
    assert!(service.deploy_source(&admin, &second, None).await.is_err());
    service
        .deploy_source(&admin, &second, Some(&contract))
        .await?;
    service.delete_deployment(&admin, "default").await?;
    assert!(admin.current_deployment("team-b").await?.is_some());
    Ok(())
}

#[tokio::test]
async fn resolved_targets_expire_within_host_idle_lease_and_authorization_limits() -> Result<()> {
    use crate::clock::{Clock, SystemClock};

    for (default_idle_ms, override_idle_ms, lease_ms, grant_ms) in [
        (10_000, None, 30_000, None),
        (25_000, None, 30_000, Some(60_000)),
        (1, None, 30_000, Some(60_000)),
        (120_000, None, 8_000, Some(60_000)),
        (120_000, None, 60_000, Some(120_000)),
        (30_000, None, 30_000, Some(7_000)),
        (10_000, Some(60_000), 120_000, None),
        (60_000, Some(1_000), 120_000, None),
    ] {
        let (mut service, admin, _) = fixture_with_idle_timeout(default_idle_ms)?;
        let mut spec = source();
        let idle_ms = override_idle_ms.unwrap_or(default_idle_ms);
        if let Some(timeout) = override_idle_ms {
            spec.sandboxes.insert(
                "Counter".into(),
                serde_json::from_value(serde_json::json!({"idleTimeoutMs":timeout}))?,
            );
        }
        admin.register_deployment(&spec, None).await?;
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let before = SystemClock.now_ms()?;
        let lease = HostLease {
            id: HostId::new(format!("host.v3.{}.one", spec.host_config_key())),
            session_id: uuid::Uuid::new_v4().to_string(),
            route: "https://host.example.com".into(),
            expires_at_ms: before + lease_ms,
        };
        let placements = Arc::new(LocalObjectPlacementStore::default());
        placements.set_owner(&actor.storage_key(), lease.clone(), "north-america-west")?;
        service.placements = placements;
        let grant = grant_ms.map(|grant_ms| super::super::session::InvocationGrant {
            subject: "subject".into(),
            grant_id: "grant".into(),
            expires_at: ((before + grant_ms) / 1_000) as i64,
            methods: vec!["increment".into()],
        });
        let target = service
            .resolve_actor_route(&actor, None, None, grant.clone())
            .await?;
        let after = SystemClock.now_ms()?;
        let expiry = u64::try_from(target.expires_at_ms)?;
        let authorization_expiry = grant.map_or(u64::MAX, |grant| grant.expires_at as u64 * 1_000);
        assert!(
            expiry <= after + idle_ms,
            "target outlives host idle timeout: {idle_ms}ms"
        );
        assert!(expiry <= lease.expires_at_ms, "target outlives host lease");
        assert!(
            expiry <= authorization_expiry,
            "target outlives authorization"
        );
        assert!(
            expiry <= (after / 1_000 + 60) * 1_000,
            "target outlives credential"
        );
        assert!(
            expiry
                >= (before + idle_ms)
                    .min(lease.expires_at_ms)
                    .min(authorization_expiry)
                    .min((before / 1_000 + 60) * 1_000)
        );
    }
    Ok(())
}

fn fixture() -> Result<(ControlPlaneService, AdminService, Arc<Provider>)> {
    fixture_with_idle_timeout(60_000)
}

fn fixture_with_idle_timeout(
    host_idle_timeout_ms: u64,
) -> Result<(ControlPlaneService, AdminService, Arc<Provider>)> {
    let issuer = super::tests::test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        std::time::Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let provider = Arc::new(Provider::default());
    let provisioner = Arc::new(
        SandboxHostProvisioner::new(
            provider.clone(),
            HostSandboxRuntimeConfig {
                control_plane_url: "http://control".into(),
                jwt_issuer: "issuer".into(),
                invocation_jwt_audience: "invocation".into(),
                host_idle_timeout_ms,
            },
            issuer.clone(),
            Some(super::super::process::RuntimeImages {
                typescript: "im-runtime".into(),
                python: "im-python".into(),
            }),
        )
        .with_runtime_access(Arc::new(crate::bucket::access::RuntimeAccess::new(
            crate::bucket::access::BucketLocation::Gcs {
                bucket: "owner-bucket".into(),
                artifact_bucket: "test-artifacts".into(),
            },
            crate::bucket::SnapshotStoreConfig::Local,
        )?)),
    );
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        provisioner,
    );
    Ok((service, admin, provider))
}

fn source() -> HostLaunchSpec {
    HostLaunchSpec {
        sandboxes: Default::default(),
        project_id: "default".into(),
        source: None,
        image_ref: "bundle".into(),
        code_snapshot: Some(crate::sandbox::testing::code_artifact(1)),
        working_directory: "/customer".into(),
        actor_entrypoint: Some("actors.mjs".into()),
        secret_refs: vec![],
    }
}

fn contract() -> serde_json::Value {
    serde_json::json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}})
}

#[derive(Default)]
struct Provider {
    retired: Mutex<Vec<String>>,
    prepared: Mutex<Vec<crate::sandbox::SandboxDeployment>>,
    fail_preparation: AtomicBool,
    preparation_seconds: AtomicU64,
}

#[async_trait]
impl SandboxProvider for Provider {
    async fn prepare_registration(
        &self,
        deployment: &crate::sandbox::SandboxDeployment,
    ) -> Result<()> {
        self.prepared.lock().unwrap().push(deployment.clone());
        ensure!(
            !self.fail_preparation.load(Ordering::SeqCst),
            "provider warming failed"
        );
        tokio::time::sleep(Duration::from_secs(
            self.preparation_seconds.load(Ordering::SeqCst),
        ))
        .await;
        Ok(())
    }

    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("unexpected spare inspection")
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("no actor invocation in deployment test")
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
