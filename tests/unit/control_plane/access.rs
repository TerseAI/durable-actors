use super::*;
use crate::control_plane::access::AccessDisabled;

#[tokio::test]
async fn organization_access_requires_admin_and_can_be_restored() -> Result<()> {
    let (service, admin, _) = fixture()?;
    admin.register_test_deployment(&source()).await?;
    let routes = super::super::super::public_api::router(service.clone(), admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/organizations/org-test/access",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    for (token, enabled, status) in [
        ("wrong", false, reqwest::StatusCode::UNAUTHORIZED),
        ("api-key", false, reqwest::StatusCode::ACCEPTED),
        ("api-key", true, reqwest::StatusCode::ACCEPTED),
    ] {
        let response = client
            .put(&url)
            .bearer_auth(token)
            .json(&serde_json::json!({"enabled":enabled}))
            .send()
            .await?;
        assert_eq!(response.status(), status);
        if token == "api-key" {
            assert_eq!(
                service.registry.project_access_enabled("default").await?,
                enabled
            );
        }
    }
    assert!(admin.current_deployment("default").await?.is_some());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn organization_shutdown_retries_without_affecting_other_tenants() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    for (project, organization) in [
        ("default", "org-test"),
        ("second", "org-test"),
        ("other", "other-org"),
    ] {
        let mut spec = source();
        spec.project_id = project.into();
        spec.organization_id = Some(organization.into());
        admin.register_test_deployment(&spec).await?;
    }
    admin.set_organization_access("org-test", false).await?;
    provider
        .fail_termination
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for expected in [2, 4] {
        service.reconcile_access().await?;
        assert_eq!(provider.retired.lock().unwrap().len(), expected);
    }
    for project in ["default", "second"] {
        assert!(
            service
                .runtime_deployment(project)
                .await
                .unwrap_err()
                .is::<AccessDisabled>()
        );
    }
    service.ensure_project_access("other").await?;
    let other = admin.current_deployment("other").await?.unwrap();
    assert!(
        !provider
            .retired
            .lock()
            .unwrap()
            .contains(&other.host_config_key())
    );
    assert!(
        service
            .deploy_source(&admin, &source(), None)
            .await
            .unwrap_err()
            .is::<AccessDisabled>()
    );
    admin.set_organization_access("org-test", true).await?;
    service.ensure_project_access("default").await?;
    provider.retired.lock().unwrap().clear();
    service.reconcile_access().await?;
    assert!(provider.retired.lock().unwrap().is_empty());
    Ok(())
}
