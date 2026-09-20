use super::*;

#[test]
fn secret_changes_get_a_new_host_revision_without_changing_the_image_revision() {
    let mut deployment = spec("image-1");
    let initial = deployment.host_revision();
    deployment.secret_refs = vec!["secrets-first".into()];
    let first = deployment.host_revision();
    assert_ne!(initial, first);
    deployment.secret_refs = vec!["secrets-second".into()];
    assert_ne!(first, deployment.host_revision());
    assert_eq!(deployment.code_revision, "revision-1");
    deployment.secret_refs.clear();
    assert_eq!(initial, deployment.host_revision());
}

#[tokio::test]
async fn deployment_retains_secret_references() -> Result<()> {
    let registry = LocalAdminRegistry::default();
    let mut deployment = spec("im-1");
    deployment.secret_refs = vec!["project-secrets".into()];
    assert!(registry.register_test_deployment(&deployment).await?);
    assert_eq!(registry.launch_spec().await?, Some(deployment.clone()));
    deployment.secret_refs = vec!["replacement-secrets".into()];
    assert!(registry.register_test_deployment(&deployment).await?);
    Ok(())
}

#[tokio::test]
async fn registration_replaces_the_projects_active_revision() -> Result<()> {
    let registry = LocalAdminRegistry::default();
    assert!(registry.register_test_deployment(&spec("im-1")).await?);
    assert!(!registry.register_test_deployment(&spec("im-1")).await?);
    let mut replacement = spec("im-2");
    replacement.code_revision = "revision-2".into();
    assert!(registry.register_test_deployment(&replacement).await?);
    assert_eq!(registry.launch_spec().await?, Some(replacement));
    Ok(())
}

#[tokio::test]
async fn postgres_registration_replaces_the_single_deployment_atomically() -> Result<()> {
    crate::postgres::testing::with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let registry = PostgresAdminRegistry::from_database(database.clone());
        let mut deployment = spec("image-1");
        deployment.secret_refs = vec!["project-secrets".into()];
        deployment.source = Some(DeploymentSource {
            image_ref: "im-source".into(),
            working_directory: "/project".into(),
            actor_entrypoint: Some("src/actors.ts".into()),
        });

        assert!(registry.register_test_deployment(&deployment).await?);
        assert_eq!(registry.launch_spec().await?, Some(deployment.clone()));
        assert!(!registry.register_test_deployment(&deployment).await?);
        deployment.source.as_mut().unwrap().image_ref = "im-updated".into();
        assert!(registry.register_test_deployment(&deployment).await?);
        assert_eq!(registry.launch_spec().await?, Some(deployment));
        registry.remove_deployment().await?;
        assert_eq!(registry.launch_spec().await?, None);
        registry.remove_deployment().await?;
        Ok(())
    })
    .await
}

fn spec(image: &str) -> HostLaunchSpec {
    HostLaunchSpec {
        source: None,
        code_snapshot: None,
        code_revision: "revision-1".into(),
        image_ref: image.into(),
        working_directory: "/workspace".into(),
        actor_entrypoint: Some("src/durable-objects.ts".into()),
        secret_refs: vec![],
    }
}
