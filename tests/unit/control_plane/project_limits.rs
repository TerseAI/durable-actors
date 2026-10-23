use super::*;
use crate::{
    actor_lifecycle::{LifecycleKind, LifecycleReport, LifecycleStore},
    clock::Clock,
    postgres::testing::with_postgres,
    redis_client::{Redis, RedisConfig},
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const RETENTION: Duration = Duration::from_secs(86_460);

#[test]
fn limits_are_opt_out_hosted_and_opt_in_locally() -> Result<()> {
    assert_eq!(
        ProjectLimits::from_lookup(&mut |_| None, true)?,
        Some(ProjectLimits::default())
    );
    assert!(local_admission(&mut |_| None)?.is_none());
    assert!(
        local_admission(
            &mut |key| (key == "DURABLE_ACTORS_PROJECT_MAX_ACTIVE_ACTORS").then(|| "1".into())
        )?
        .is_none()
    );
    assert!(
        local_admission(
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
    for name in [
        "DURABLE_ACTORS_PROJECT_MAX_ACTIVE_ACTORS",
        "DURABLE_ACTORS_PROJECT_ACTOR_STARTS_PER_SECOND",
        "DURABLE_ACTORS_PROJECT_ACTOR_START_BURST",
    ] {
        for value in ["0", "-1", "1.5", "4294967296", "", "no"] {
            assert!(
                ProjectLimits::from_lookup(&mut |key| (key == name).then(|| value.into()), true)
                    .unwrap_err()
                    .to_string()
                    .contains(name)
            );
        }
    }
    assert!(ProjectLimits::from_lookup(&mut |_| Some("invalid".into()), true).is_err());
    Ok(())
}

#[tokio::test]
async fn lifecycle_reports_release_only_the_matching_activation_once() -> Result<()> {
    for store in stores()? {
        let limits = limits(2, 50, 100);
        let first = activation("p", "one", "first");
        let replacement = activation("p", "one", "replacement");
        let third = activation("p", "two", "third");
        store.reserve(&first, limits).await?;
        store.reserve(&first, limits).await?;
        store.reserve(&replacement, limits).await?;
        assert_quota(store.reserve(&third, limits).await, 2);
        store
            .observe(&event(&first, LifecycleKind::Started, 1))
            .await?;
        store
            .observe(&event(&first, LifecycleKind::Draining, 2))
            .await?;
        store
            .observe(&event(&first, LifecycleKind::Stopped, 1))
            .await?;
        assert_quota(store.reserve(&third, limits).await, 2);
        let mut wrong_epoch = event(&first, LifecycleKind::Stopped, 3);
        wrong_epoch.report.owner_epoch = 2;
        store.observe(&wrong_epoch).await?;
        assert_quota(store.reserve(&third, limits).await, 2);
        store
            .observe(&event(&first, LifecycleKind::Stopped, 3))
            .await?;
        store.reserve(&third, limits).await?;
        store
            .observe(&event(&first, LifecycleKind::Stopped, 3))
            .await?;
        store
            .observe(&event(&first, LifecycleKind::Heartbeat, 4))
            .await?;
        assert!(store.reserve(&first, limits).await.is_err());
        assert_quota(
            store
                .reserve(&activation("p", "four", "four"), limits)
                .await,
            2,
        );
        store
            .reserve(&activation("other", "one", "one"), limits)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn reports_for_preexisting_hosts_are_counted_without_spending_start_tokens() -> Result<()> {
    for store in stores()? {
        let limits = limits(1, 1, 1);
        let legacy = activation("p", "legacy", "old");
        store
            .observe(&event(&legacy, LifecycleKind::Heartbeat, 2))
            .await?;
        assert_quota(
            store.reserve(&activation("p", "new", "new"), limits).await,
            1,
        );
        store
            .observe(&event(&legacy, LifecycleKind::Stopped, 3))
            .await?;
        store
            .reserve(&activation("p", "new", "new"), limits)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn ownership_epochs_keep_u64_precision_in_both_backends() -> Result<()> {
    for store in stores()? {
        let first = activation("p", "one", "first");
        let second = activation("p", "two", "second");
        let limits = limits(1, 50, 100);
        store.reserve(&first, limits).await?;
        let mut started = event(&first, LifecycleKind::Started, 1);
        started.report.owner_epoch = u64::MAX - 1;
        store.observe(&started).await?;
        let mut stopped = event(&first, LifecycleKind::Stopped, 2);
        stopped.report.owner_epoch = u64::MAX;
        store.observe(&stopped).await?;
        assert_quota(store.reserve(&second, limits).await, 1);
        stopped.report.owner_epoch = u64::MAX - 1;
        store.observe(&stopped).await?;
        store.reserve(&second, limits).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pending_and_silent_hosts_remain_charged_when_telemetry_is_missing() -> Result<()> {
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let store = MemoryReservations::new(clock.clone(), RETENTION);
    let pending = activation("p", "one", "pending");
    let live = activation("p", "two", "live");
    store.reserve(&pending, limits(2, 50, 100)).await?;
    store
        .observe(&event(&live, LifecycleKind::Started, 1))
        .await?;
    clock.0.store(1_000_000_000, Ordering::SeqCst);
    assert_quota(
        store
            .reserve(&activation("p", "three", "third"), limits(2, 50, 100))
            .await,
        2,
    );
    Ok(())
}

#[tokio::test]
async fn stopping_frees_capacity_but_waking_again_consumes_another_start_token() -> Result<()> {
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let store = MemoryReservations::new(clock.clone(), RETENTION);
    let limits = limits(1, 1, 1);
    let first = activation("p", "same-actor", "first");
    let second = activation("p", "same-actor", "second");
    store.reserve(&first, limits).await?;
    store
        .observe(&event(&first, LifecycleKind::Stopped, 1))
        .await?;
    assert_rate(store.reserve(&second, limits).await);
    clock.0.store(1999, Ordering::SeqCst);
    assert_rate(store.reserve(&second, limits).await);
    clock.0.store(2000, Ordering::SeqCst);
    store.reserve(&second, limits).await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_admissions_cannot_exceed_capacity() -> Result<()> {
    for store in stores()? {
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..40 {
            let store = store.clone();
            tasks.spawn(async move {
                store
                    .reserve(
                        &activation("p", &index.to_string(), &index.to_string()),
                        limits(7, 50, 100),
                    )
                    .await
            });
        }
        let mut accepted = 0;
        while let Some(result) = tasks.join_next().await {
            match result? {
                Ok(()) => accepted += 1,
                Err(error) => assert!(matches!(
                    error.downcast_ref(),
                    Some(AdmissionRejected::Quota { limit: 7 })
                )),
            }
        }
        assert_eq!(accepted, 7);
    }
    Ok(())
}

#[tokio::test]
async fn separate_redis_clients_share_rate_and_capacity_state() -> Result<()> {
    let Some(redis) = test_redis()? else {
        return Ok(());
    };
    let namespace = format!("admission-test:{}", uuid::Uuid::new_v4());
    let first = RedisReservations::new(redis.clone(), &namespace, RETENTION);
    let second = RedisReservations::new(test_redis()?.unwrap(), &namespace, RETENTION);
    let limits = limits(100, 1, 2);
    first
        .reserve(&activation("p", "one", "one"), limits)
        .await?;
    let mut freeze = ::redis::cmd("HSET");
    freeze
        .arg(format!("{namespace}:{{p}}:bucket"))
        .arg("updated")
        .arg(9_000_000_000_000_000u64);
    let _: i32 = redis.run(freeze).await?;
    second
        .reserve(&activation("p", "two", "two"), limits)
        .await?;
    assert_rate(
        first
            .reserve(&activation("p", "three", "three"), limits)
            .await,
    );
    let mut refill = ::redis::cmd("HSET");
    refill
        .arg(format!("{namespace}:{{p}}:bucket"))
        .arg("updated")
        .arg(0);
    let _: i32 = redis.run(refill).await?;
    second
        .reserve(&activation("p", "three", "three"), limits)
        .await?;
    Ok(())
}

#[tokio::test]
async fn lifecycle_store_updates_admission_without_a_usage_publisher() -> Result<()> {
    let reservations = Arc::new(MemoryReservations::new(
        Arc::new(TestClock(AtomicU64::new(1000))),
        RETENTION,
    ));
    let admission = Arc::new(ActorAdmission::new(
        Arc::new(FixedProjectLimits(limits(1, 50, 100))),
        reservations.clone(),
    ));
    let first = activation("p", "one", "one");
    reservations.reserve(&first, limits(1, 50, 100)).await?;
    let lifecycle = LifecycleStore::default().with_observer(admission.clone());
    lifecycle
        .record(event(&first, LifecycleKind::Stopped, 1))
        .await?;
    admission
        .reserve(
            &activation("p", "two", "two").actor,
            &HostId::new("host-two"),
            "two",
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn postgres_overrides_are_live_partial_and_reset_individually() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let defaults = limits(10_000, 50, 100);
        let store = PostgresProjectLimits::new(database.clone(), defaults);
        assert_eq!(store.get("p").await?, defaults);
        database.execute("INSERT INTO durable_actors_project_limits (project_id, max_active_actors) VALUES ('p', 20000)", &[]).await?;
        assert_eq!(store.get("p").await?, limits(20_000, 50, 100));
        database.execute("UPDATE durable_actors_project_limits SET actor_starts_per_second = 500, actor_start_burst = 1000 WHERE project_id = 'p'", &[]).await?;
        assert_eq!(store.get("p").await?, limits(20_000, 500, 1000));
        assert_eq!(store.get("other").await?, defaults);
        database.execute("UPDATE durable_actors_project_limits SET max_active_actors = NULL WHERE project_id = 'p'", &[]).await?;
        assert_eq!(store.get("p").await?, limits(10_000, 500, 1000));
        database.execute("DELETE FROM durable_actors_project_limits WHERE project_id = 'p'", &[]).await?;
        assert_eq!(store.get("p").await?, defaults);
        Ok(())
    }).await
}

fn stores() -> Result<Vec<Arc<dyn ReservationStore>>> {
    let mut stores: Vec<Arc<dyn ReservationStore>> = vec![Arc::new(MemoryReservations::new(
        Arc::new(TestClock(AtomicU64::new(1000))),
        RETENTION,
    ))];
    if let Some(redis) = test_redis()? {
        stores.push(Arc::new(RedisReservations::new(
            redis,
            &format!("admission-test:{}", uuid::Uuid::new_v4()),
            RETENTION,
        )));
    }
    Ok(stores)
}

fn test_redis() -> Result<Option<Redis>> {
    std::env::var("DURABLE_ACTORS_TEST_REDIS_URL")
        .ok()
        .map(|url| Redis::connect(&RedisConfig { url, ca_cert: None }))
        .transpose()
}

fn limits(max_active_actors: u32, starts_per_second: u32, start_burst: u32) -> ProjectLimits {
    ProjectLimits {
        max_active_actors,
        starts_per_second,
        start_burst,
    }
}

fn activation(project: &str, actor: &str, session: &str) -> Activation {
    Activation::new(
        &ActorKey {
            project_id: project.into(),
            actor_name: "Counter".into(),
            actor_id: actor.into(),
        },
        &format!("host-{session}"),
        session,
    )
}

fn event(activation: &Activation, kind: LifecycleKind, sequence: u64) -> LifecycleEvent {
    LifecycleEvent {
        organization_id: None,
        event_id: format!("{}:{sequence}", activation.session),
        actor: activation.actor.clone(),
        host_id: activation.host.clone(),
        session_id: activation.session.clone(),
        region: "north-america-east".into(),
        received_at_ms: 1000,
        report: LifecycleReport {
            kind,
            owner_epoch: 1,
            sequence,
            started_at_ms: 1000,
            occurred_at_ms: 1000,
            resources: Default::default(),
            resident: Some(true),
            waiting: vec![],
        },
    }
}

fn assert_quota(result: Result<()>, limit: u32) {
    assert!(
        matches!(result.unwrap_err().downcast_ref(), Some(AdmissionRejected::Quota { limit: actual }) if *actual == limit)
    );
}

fn assert_rate(result: Result<()>) {
    assert!(matches!(
        result.unwrap_err().downcast_ref(),
        Some(AdmissionRejected::Rate {
            retry_after_seconds: 1
        })
    ));
}

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
