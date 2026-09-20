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
    assert_eq!(
        registry.launch_spec("default").await?,
        Some(deployment.clone())
    );
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
    assert_eq!(registry.launch_spec("default").await?, Some(replacement));
    Ok(())
}

#[tokio::test]
async fn postgres_registration_replaces_the_single_deployment_atomically() -> Result<()> {
    crate::postgres::testing::with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let registry = PostgresAdminRegistry::from_database(database.clone());
        let mut deployment = spec("image-1");
        deployment.secret_refs = vec!["project-secrets".into()];

        assert!(registry.register_test_deployment(&deployment).await?);
        assert_eq!(
            registry.launch_spec("default").await?,
            Some(deployment.clone())
        );
        registry.remove_deployment("default").await?;
        assert_eq!(registry.launch_spec("default").await?, None);
        registry.remove_deployment("default").await?;
        Ok(())
    })
    .await
}

fn spec(image: &str) -> HostLaunchSpec {
    HostLaunchSpec {
        project_id: "default".into(),
        code_revision: "revision-1".into(),
        image_ref: image.into(),
        working_directory: "/workspace".into(),
        actor_entrypoint: Some("src/durable-objects.ts".into()),
        secret_refs: vec![],
    }
}

#[tokio::test]
async fn projects_keep_independent_deployments_and_host_identities() -> Result<()> {
    let registry = LocalAdminRegistry::default();
    let deployment = |project: &str| -> HostLaunchSpec {
        let mut value = serde_json::to_value(spec("same-image")).unwrap();
        value["projectId"] = project.into();
        serde_json::from_value(value).unwrap()
    };
    let first = deployment("team-a");
    let second = deployment("team-b");
    assert_ne!(first.host_revision(), second.host_revision());
    registry.register_test_deployment(&first).await?;
    registry.register_test_deployment(&second).await?;
    assert_eq!(registry.launch_spec("team-a").await?, Some(first));
    assert_eq!(registry.launch_spec("team-b").await?, Some(second.clone()));
    registry.remove_deployment("team-a").await?;
    assert_eq!(registry.launch_spec("team-a").await?, None);
    assert_eq!(registry.launch_spec("team-b").await?, Some(second));
    Ok(())
}

#[tokio::test]
async fn postgres_projects_keep_deployments_contracts_and_deletions_separate() -> Result<()> {
    crate::postgres::testing::with_postgres(async |fixture| {
        let registry =
            PostgresAdminRegistry::from_database(PostgresDatabase::connect(&fixture.url).await?);
        let contract = PublicActorContract::new(serde_json::from_str(include_str!(
            "../../../sdk/tests/fixtures/public-contract.json"
        ))?)?;
        let mut first = spec("first-image");
        first.project_id = "team-a".into();
        let mut second = spec("second-image");
        second.project_id = "team-b".into();
        registry
            .register_deployment(&first, Some(&contract))
            .await?;
        registry
            .register_deployment(&second, Some(&contract))
            .await?;
        assert_eq!(registry.launch_spec("team-a").await?, Some(first.clone()));
        assert_eq!(registry.launch_spec("team-b").await?, Some(second.clone()));
        first.code_revision = "revision-2".into();
        first.secret_refs = vec!["team-a-secret".into()];
        registry
            .register_deployment(&first, Some(&contract))
            .await?;
        assert_eq!(registry.launch_spec("team-b").await?, Some(second.clone()));
        assert!(
            registry
                .deployment_contract("team-b", Some("revision-1"))
                .await?
                .is_some()
        );
        assert!(
            registry
                .deployment_contract("team-b", Some("revision-2"))
                .await?
                .is_none()
        );
        registry.remove_deployment("team-a").await?;
        assert!(
            registry
                .deployment_contract("team-a", None)
                .await?
                .is_none()
        );
        assert!(
            registry
                .deployment_contract("team-b", None)
                .await?
                .is_some()
        );
        assert_eq!(registry.launch_spec("team-b").await?, Some(second));
        Ok(())
    })
    .await
}
