use super::*;
use crate::{
    control_plane::{ActorTokenPurpose, admin::LocalAdminRegistry},
    placement::testing::LocalObjectPlacementStore,
    sandbox::{
        ActorHostHandle, BuildCodeRequest, BuiltActorCode, SocketCredentials,
        SocketCredentialsRequest,
    },
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn source_deployment_builds_once_and_preserves_code_on_secret_updates_and_failures()
-> Result<()> {
    let (service, admin, provider) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let source = serde_json::json!({"codeRevision":"r1", "imageRef":"im-customer", "workingDirectory":"/project", "actorEntrypoint":"src/actors.ts", "secretRefs":[]});
    for changed in [true, false] {
        let reply: serde_json::Value = client
            .put(&url)
            .bearer_auth("api-key")
            .json(&source)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["changed"], changed);
    }
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    let compiled = admin.current_deployment("default").await?.unwrap();
    assert_eq!(compiled.image_ref, "im-runtime");
    assert_eq!(compiled.code_snapshot.as_deref(), Some("im-code-1"));
    assert_eq!(compiled.actor_entrypoint.as_deref(), Some("actors.mjs"));
    assert_eq!(compiled.working_directory, "/customer");
    assert_eq!(compiled.source.as_ref().unwrap().image_ref, "im-customer");
    let mut roundtrip: serde_json::Value = client
        .get(&url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(roundtrip, source);
    roundtrip["secretRefs"] = serde_json::json!(["replacement-secrets"]);
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&roundtrip)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    let active = admin.current_deployment("default").await?.unwrap();
    assert_eq!(active.code_snapshot, compiled.code_snapshot);
    assert_ne!(active.host_revision(), compiled.host_revision());
    assert_eq!(
        admin
            .deployment_contract("default", None)
            .await?
            .unwrap()
            .contract,
        contract()
    );
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[compiled.host_revision()]
    );

    provider.fail.store(true, Ordering::SeqCst);
    roundtrip["imageRef"] = "im-broken".into();
    roundtrip["codeRevision"] = "r2".into();
    let failed = client
        .put(&url)
        .bearer_auth("api-key")
        .json(&roundtrip)
        .send()
        .await?;
    assert_eq!(failed.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        admin.current_deployment("default").await?,
        Some(active.clone())
    );
    assert_eq!(provider.retired.lock().unwrap().len(), 1);

    provider.fail.store(false, Ordering::SeqCst);
    roundtrip["imageRef"] = "im-updated".into();
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&roundtrip)
        .send()
        .await?
        .error_for_status()?;
    let updated = admin.current_deployment("default").await?.unwrap();
    assert_ne!(updated.code_snapshot, active.code_snapshot);
    assert_eq!(provider.builds.lock().unwrap().len(), 3);
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[compiled.host_revision(), active.host_revision()]
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn cached_code_cannot_be_registered_with_an_unrelated_contract() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let mut source = source();
    service.deploy_source(&admin, &source, None).await?;
    let active = admin.current_deployment("default").await?;
    source.code_revision = "r2".into();
    let unrelated = super::super::contracts::PublicActorContract::new(serde_json::from_str(
        include_str!("../../../sdk/tests/fixtures/public-contract.json"),
    )?)?;
    let result = service
        .deploy_source(&admin, &source, Some(&unrelated))
        .await;
    assert!(
        result.is_err(),
        "a supplied contract must match the cached code"
    );
    assert_eq!(admin.current_deployment("default").await?, active);
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    assert!(provider.retired.lock().unwrap().is_empty());
    service.deploy_source(&admin, &source, None).await?;
    assert_eq!(
        admin
            .deployment_contract("default", None)
            .await?
            .unwrap()
            .code_revision,
        "r2"
    );
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn compiled_deployments_and_cached_contracts_are_scoped_to_the_project() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let mut first = source();
    first.project_id = "team-a".into();
    let mut second = first.clone();
    second.project_id = "team-b".into();
    service.deploy_source(&admin, &first, None).await?;
    service.deploy_source(&admin, &second, None).await?;
    let first_compiled = admin.current_deployment("team-a").await?.unwrap();
    let second_compiled = admin.current_deployment("team-b").await?.unwrap();
    assert_eq!(first_compiled.project_id, "team-a");
    assert_eq!(second_compiled.project_id, "team-b");
    assert_ne!(
        first_compiled.host_revision(),
        second_compiled.host_revision()
    );
    assert_eq!(provider.builds.lock().unwrap().len(), 2);
    first.code_revision = "r2".into();
    service.deploy_source(&admin, &first, None).await?;
    assert_eq!(provider.builds.lock().unwrap().len(), 2);
    assert_eq!(
        admin
            .deployment_contract("team-a", None)
            .await?
            .unwrap()
            .code_revision,
        "r2"
    );
    assert_eq!(
        admin
            .deployment_contract("team-b", None)
            .await?
            .unwrap()
            .code_revision,
        "r1"
    );
    service.delete_deployment(&admin, "team-a").await?;
    assert_eq!(
        admin.current_deployment("team-b").await?,
        Some(second_compiled.clone())
    );
    assert!(
        !provider
            .retired
            .lock()
            .unwrap()
            .contains(&second_compiled.host_revision())
    );
    Ok(())
}

fn fixture() -> Result<(ControlPlaneService, AdminService, Arc<BuildProvider>)> {
    let issuer = super::tests::test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        std::time::Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new("api-key".into(), registry.clone(), issuer.clone())?;
    let provider = Arc::new(BuildProvider::default());
    let provisioner = Arc::new(SandboxHostProvisioner::new(
        provider.clone(),
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invocation".into(),
            actor_idle_timeout_seconds: 60,
            host_idle_timeout_ms: 60_000,
        },
        issuer.clone(),
        Some("im-runtime".into()),
    ));
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
        project_id: "default".into(),
        source: None,
        code_revision: "r1".into(),
        image_ref: "im-customer".into(),
        code_snapshot: None,
        working_directory: "/project".into(),
        actor_entrypoint: Some("src/actors.ts".into()),
        secret_refs: vec![],
    }
}

fn contract() -> serde_json::Value {
    serde_json::json!({"version":1,"actors":[]})
}

#[derive(Default)]
struct BuildProvider {
    builds: Mutex<Vec<serde_json::Value>>,
    retired: Mutex<Vec<String>>,
    fail: AtomicBool,
}

#[async_trait]
impl SandboxProvider for BuildProvider {
    async fn build_code(&self, request: &BuildCodeRequest) -> Result<BuiltActorCode> {
        let mut builds = self.builds.lock().unwrap();
        builds.push(serde_json::to_value(request)?);
        ensure!(!self.fail.load(Ordering::SeqCst), "compilation failed");
        Ok(BuiltActorCode {
            code_snapshot: format!("im-code-{}", builds.len()),
            contract: contract(),
        })
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("no actor invocation in deployment test")
    }
    async fn socket_credentials(&self, _: &SocketCredentialsRequest) -> Result<SocketCredentials> {
        anyhow::bail!("no socket in deployment test")
    }
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        self.retired
            .lock()
            .unwrap()
            .push(request.code_revision.clone());
        Ok(HostTermination {
            provider: "test".into(),
            resource_ids: vec![],
        })
    }
}
