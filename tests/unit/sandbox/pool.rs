use super::*;
use crate::postgres::testing::with_postgres;

fn pool(database: PostgresDatabase) -> Arc<SparePool> {
    SparePool::new(
        database,
        Arc::new(
            super::super::CommandSandboxProvider::new(
                "test".into(),
                "false".into(),
                Default::default(),
            )
            .unwrap(),
        ),
        PoolConfig {
            kind: SpareKind::Actor,
            idle: 1,
            idle_ttl_seconds: 600,
            regions: vec!["region".into()],
            resources: ResourceLimits::default(),
        },
    )
}

#[tokio::test]
async fn only_live_claims_can_become_routable() -> Result<()> {
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        pool.reserve_host("session", "host", "revision").await?;
        let spare = SpareHandle {
            control_route: String::new(),
            control_token: String::new(),
            name: "do-actor-session".into(),
            resource_id: "sb-test".into(),
            route: "https://spare.test".into(),
            canonical_region: "region".into(),
        };
        assert!(pool.host("host").await?.is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pool.wait_ready("host"))
                .await
                .is_err()
        );
        pool.remember("host", "revision", &spare).await?;
        pool.wait_ready("host").await?;
        assert_eq!(pool.host("host").await?, Some(spare.clone()));
        pool.failed("host").await?;
        assert!(pool.remember("host", "revision", &spare).await.is_err());
        assert!(pool.wait_ready("host").await.is_err());
        assert!(pool.host("host").await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn expired_claims_and_outdated_runtime_spares_cannot_be_reused() -> Result<()> {
    with_postgres(async |fixture| {
           let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
           pool.reserve_host("expired", "host", "revision").await?;
           pool.store.0.execute("UPDATE durable_actors_spares SET expires_at = clock_timestamp() - interval '1 second'", &[]).await?;
           let spare = SpareHandle {
control_route: String::new(),
control_token: String::new(), name: "do-actor-expired".into(), resource_id: "sb-expired".into(), route: "https://spare.test".into(), canonical_region: "region".into() };
           assert!(pool.remember("host", "revision", &spare).await.is_err());
           let old = pool.store.reserve("old-runtime", 1).await?.unwrap();
           let current = pool.store.reserve("current-runtime", 1).await?.unwrap();
           pool.store.retire_unwanted(&["current-runtime".into()], 1).await?;
           let retiring = pool.store.retiring().await?;
           assert!(retiring.iter().any(|s| s.name == old));
           assert!(retiring.iter().any(|s| s.name == spare.name));
           assert!(!retiring.iter().any(|s| s.name == current));
           Ok(())
       }).await
}

#[tokio::test]
async fn concurrent_pool_claims_are_exclusive_and_never_return_assigned_sandboxes() -> Result<()> {
    with_postgres(async |fixture| {
        let store = PoolStore(
            PostgresDatabase::connect(&fixture.url).await?,
            SpareKind::Actor,
        );
        let key = "runtime-a/region-a/1000/1024";
        let names =
            futures_util::future::try_join_all((0..8).map(|_| store.reserve(key, 2))).await?;
        let names = names.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(names.len(), 2);
        assert!(store.claim(key, "host-early", "revision").await?.is_none());
        for name in &names {
            assert!(
                store
                    .publish(
                        key,
                        &SpareHandle {
                            control_route: String::new(),
                            control_token: String::new(),
                            name: name.clone(),
                            resource_id: format!("sb-{name}"),
                            route: "https://spare.test".into(),
                            canonical_region: "region-a".into()
                        },
                        600
                    )
                    .await?
            );
        }
        assert!(
            store
                .claim("other-runtime", "host-wrong", "revision")
                .await?
                .is_none()
        );
        let claims = futures_util::future::try_join_all((0..8).map(|i| {
            let store = &store;
            async move { store.claim(key, &format!("host-{i}"), "revision").await }
        }))
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        assert_eq!(claims.len(), 2);
        assert_ne!(claims[0].resource_id, claims[1].resource_id);
        assert!(store.claim(key, "host-retry", "revision").await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn zero_capacity_and_runtime_changes_retire_only_unassigned_spares() -> Result<()> {
    with_postgres(async |fixture| {
        let store = PoolStore(
            PostgresDatabase::connect(&fixture.url).await?,
            SpareKind::Actor,
        );
        assert!(store.reserve("runtime", 0).await?.is_none());
        let name = store.reserve("runtime", 1).await?.unwrap();
        let spare = SpareHandle {
            control_route: String::new(),
            control_token: String::new(),
            name,
            resource_id: "sb-test".into(),
            route: "https://spare.test".into(),
            canonical_region: "region".into(),
        };
        store.publish("runtime", &spare, 600).await?;
        store.retire_unwanted(&[], 0).await?;
        assert!(store.claim("runtime", "host", "rev").await?.is_none());
        assert_eq!(store.retiring().await?.len(), 1);
        assert!(!store.publish("runtime", &spare, 600).await?);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reconciliation_keeps_spares_for_every_project_runtime() -> Result<()> {
    use crate::control_plane::admin::{HostLaunchSpec, LocalAdminRegistry};
    with_postgres(async |fixture| {
        let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
        let registry = LocalAdminRegistry::default();
        let mut names = Vec::new();
        for (project, image) in [("team-a", "im-a"), ("team-b", "im-b")] {
            registry
                .register_test_deployment(&HostLaunchSpec {
                    project_id: project.into(),
                    source: None,
                    image_ref: image.into(),
                    code_snapshot: Some("im-code".into()),
                    working_directory: "/customer".into(),
                    actor_entrypoint: Some("actors.mjs".into()),
                    secret_refs: vec![],
                })
                .await?;
            let key = pool.key(image, "region");
            let name = pool.store.reserve(&key, 1).await?.unwrap();
            pool.store
                .publish(
                    &key,
                    &SpareHandle {
                        name: name.clone(),
                        resource_id: format!("sb-{project}"),
                        route: format!("https://{project}.test"),
                        canonical_region: "region".into(),
                        control_route: String::new(),
                        control_token: String::new(),
                    },
                    600,
                )
                .await?;
            names.push(name);
        }
        pool.reconcile(&registry).await?;
        for name in names {
            let row = pool
                .store
                .0
                .query_opt(
                    "SELECT status FROM durable_actors_spares WHERE name = $1",
                    &[&name],
                )
                .await?
                .unwrap();
            assert_eq!(row.get::<_, &str>(0), "ready");
        }
        Ok(())
    })
    .await
}
