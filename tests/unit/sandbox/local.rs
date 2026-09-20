use super::*;
use crate::{bucket::testing::RuntimeFixture, host::HostId};

#[tokio::test]
async fn shutdown_rejects_new_hosts_before_starting_a_process() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let project = directory.path().to_path_buf();
    let fixture = RuntimeFixture::new()?;
    let provider = LocalSandboxProvider::new(
        project.join("unused-executable"),
        project.clone(),
        fixture.runtime,
        None,
    );
    provider.shutdown().await;
    let request = EnsureHostRequest {
        actor_is_new: true,
        actor: None,
        code_snapshot: None,
        spare: None,
        resources: Default::default(),
        runtime_config: None,

        code_revision: "local".into(),
        canonical_region: "north-america-east".into(),
        host_id: HostId::new("host"),
        session_id: "session".into(),
        host_token: "unused".into(),
        jwt_public_keys: "unused".into(),
        control_plane_url: "http://127.0.0.1:7100".into(),
        jwt_issuer: "local".into(),
        invocation_jwt_audience: "local".into(),
        socket_jwt_audience: "local:websocket".into(),
        image_ref: "local".into(),
        working_directory: project.display().to_string(),
        actor_entrypoint: None,
        secret_refs: vec![],
        actor_idle_timeout_seconds: 60,
        host_idle_timeout_ms: 300_000,
    };
    assert_eq!(
        host_environment(&request, &directory).get("DURABLE_OBJECT_LOG_MODE"),
        Some(&"development".to_owned())
    );
    assert_eq!(
        host_environment(&request, &directory).get("DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_SECONDS"),
        Some(&"60".to_owned())
    );
    let error = provider.ensure_host(&request).await.unwrap_err();
    assert!(error.to_string().contains("shutting down"), "{error:#}");
    Ok(())
}
