use super::*;
use crate::{
    clock::Clock,
    postgres::testing::with_postgres,
    redis_client::{Redis, RedisConfig},
};
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn defaults_are_configurable_and_local_limits_require_opt_in() -> Result<()> {
    assert_eq!(
        ProjectLimits::from_lookup(&mut |_| None, true)?,
        Some(ProjectLimits::default())
    );
    assert!(local_rate_limiter(&mut |_| None)?.is_none());
    assert!(
        local_rate_limiter(
            &mut |key| (key == "DURABLE_ACTORS_PROJECT_ACTOR_START_BURST").then(|| "1".into())
        )?
        .is_none()
    );
    assert!(
        local_rate_limiter(
            &mut |key| (key == "DURABLE_ACTORS_PROJECT_LIMITS_ENABLED").then(|| "true".into())
        )?
        .is_some()
    );
    assert!(
        ProjectLimits::from_lookup(
            &mut |key| (key == "DURABLE_ACTORS_PROJECT_LIMITS_ENABLED").then(|| "false".into()),
            true
        )?
        .is_none()
    );
    let mut get = |key: &str| match key {
        "DURABLE_ACTORS_PROJECT_ACTOR_STARTS_PER_SECOND" => Some("100".into()),
        "DURABLE_ACTORS_PROJECT_ACTOR_START_BURST" => Some("200".into()),
        _ => None,
    };
    assert_eq!(
        ProjectLimits::from_lookup(&mut get, true)?,
        Some(ProjectLimits {
            starts_per_second: 100,
            start_burst: 200
        })
    );
    assert!(
        ProjectLimits::from_lookup(
            &mut |key| (key == "DURABLE_ACTORS_PROJECT_ACTOR_START_BURST").then(|| "0".into()),
            true
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn memory_limiter_refills_tokens_and_isolates_projects() -> Result<()> {
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let store = MemoryRateLimits::new(clock.clone());
    let limits = ProjectLimits {
        starts_per_second: 2,
        start_burst: 2,
    };
    assert!(store.allow("one", limits).await?);
    assert!(store.allow("one", limits).await?);
    assert!(!store.allow("one", limits).await?);
    assert!(store.allow("other", limits).await?);
    clock.0.store(1499, Ordering::SeqCst);
    assert!(!store.allow("one", limits).await?);
    clock.0.store(1500, Ordering::SeqCst);
    assert!(store.allow("one", limits).await?);
    assert!(!store.allow("one", limits).await?);
    Ok(())
}

#[tokio::test]
async fn concurrent_starts_cannot_exceed_the_burst() -> Result<()> {
    let store = Arc::new(MemoryRateLimits::new(Arc::new(TestClock(AtomicU64::new(
        1000,
    )))));
    assert_burst(&[store], "project").await
}

#[tokio::test]
async fn redis_clients_share_tokens_and_refill() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let config = RedisConfig { url, ca_cert: None };
    let redis = Redis::connect(&config)?;
    let project = uuid::Uuid::new_v4().to_string();
    let key = format!("durable-actors:starts:v1:{project}");
    let mut freeze = ::redis::cmd("HSET");
    freeze
        .arg(&key)
        .arg("tokens")
        .arg(7)
        .arg("updated")
        .arg(9_000_000_000_000_000u64);
    let _: i32 = redis.run(freeze).await?;
    let first = Arc::new(RedisRateLimits(redis.clone()));
    let second = Arc::new(RedisRateLimits(Redis::connect(&config)?));
    assert_burst(&[first.clone(), second], &project).await?;
    let mut refill = ::redis::cmd("HSET");
    refill.arg(&key).arg("updated").arg(0);
    let _: i32 = redis.run(refill).await?;
    assert!(first.allow(&project, ProjectLimits::default()).await?);
    assert!(
        first
            .allow(&uuid::Uuid::new_v4().to_string(), ProjectLimits::default())
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn database_overrides_apply_without_a_restart() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let defaults = ProjectLimits::default();
        let limiter = ProjectRateLimiter::new(defaults, Arc::new(MemoryRateLimits::new(Arc::new(crate::clock::SystemClock))))
            .with_database(database.clone());
        assert_eq!(limiter.limits("p").await?, defaults);
        database.execute("INSERT INTO durable_actors_project_limits (project_id, actor_starts_per_second) VALUES ('p', 500)", &[]).await?;
        assert_eq!(limiter.limits("p").await?, ProjectLimits { starts_per_second: 500, ..defaults });
        database.execute("UPDATE durable_actors_project_limits SET actor_starts_per_second = NULL, actor_start_burst = 1000 WHERE project_id = 'p'", &[]).await?;
        assert_eq!(limiter.limits("p").await?, ProjectLimits { start_burst: 1000, ..defaults });
        assert_eq!(limiter.limits("other").await?, defaults);
        database.execute("DELETE FROM durable_actors_project_limits WHERE project_id = 'p'", &[]).await?;
        assert_eq!(limiter.limits("p").await?, defaults);
        Ok(())
    }).await
}

#[tokio::test]
async fn throttled_starts_return_429_before_provisioning() -> Result<()> {
    use crate::control_plane::service::{HostProvisioner, SandboxHostProvisioner};
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
    let limiter = Arc::new(ProjectRateLimiter::new(
        ProjectLimits {
            starts_per_second: 1,
            start_burst: 1,
        },
        Arc::new(MemoryRateLimits::new(Arc::new(TestClock(AtomicU64::new(
            1000,
        ))))),
    ));
    let provisioner = SandboxHostProvisioner::new(
        Arc::new(crate::sandbox::testing::UnusedSandboxProvider),
        crate::sandbox::HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invoke".into(),
            host_idle_timeout_ms: 10_000,
        },
        crate::control_plane::service::tests::test_issuer()?,
        None,
    )
    .with_project_rate_limiter(Some(limiter.clone()));
    let actor = crate::actor::ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let spec = serde_json::from_value(
        serde_json::json!({"projectId":"default", "imageRef":"image", "workingDirectory":"/app", "secretRefs":[]}),
    )?;
    limiter.check("default").await?;
    let error = provisioner
        .ensure_actor_host(&spec, "north-america-east", &actor, true, None)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref(),
        Some(StartLimitError::Limited)
    ));
    let response = crate::control_plane::public_api::ApiError::routing(error).into_response();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "1");
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["error"]["code"], "actor_start_rate_limited");
    Ok(())
}

async fn assert_burst(stores: &[Arc<dyn RateLimitStore>], project: &str) -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..40 {
        let store = stores[index % stores.len()].clone();
        let project = project.to_string();
        tasks.spawn(async move {
            store
                .allow(
                    &project,
                    ProjectLimits {
                        starts_per_second: 1,
                        start_burst: 7,
                    },
                )
                .await
        });
    }
    let mut accepted = 0;
    while let Some(result) = tasks.join_next().await {
        accepted += usize::from(result??);
    }
    assert_eq!(accepted, 7);
    Ok(())
}

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
