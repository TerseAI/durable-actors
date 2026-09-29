use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

struct TestClock(AtomicU64);

impl crate::clock::Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

#[derive(Default)]
struct RetryDispatcher(Mutex<Vec<Occurrence>>);

#[async_trait]
impl CronDispatcher for RetryDispatcher {
    async fn dispatch(&self, occurrence: &Occurrence) -> Result<CronOutcome> {
        let mut calls = self.0.lock().unwrap();
        calls.push(occurrence.clone());
        ensure!(calls.len() > 1, "transient failure");
        Ok(CronOutcome::Completed)
    }
}

#[tokio::test]
async fn scheduler_retries_the_original_occurrence_then_advances() -> Result<()> {
    let store = Arc::new(SqliteCronStore::open(":memory:")?);
    store.publish("project", &definitions(), 0).await?;
    store.register(&actor(), "north-america-east", 1).await?;
    let dispatcher = Arc::new(RetryDispatcher::default());
    let clock = Arc::new(TestClock(AtomicU64::new(60_000)));
    let scheduler = CronScheduler::new(
        store.clone(),
        Arc::new(super::super::admin::LocalAdminRegistry::default()),
        dispatcher.clone(),
        clock.clone(),
    );
    let first = store.claim(60_000, 1).await?.pop().unwrap();
    scheduler.execute(&first).await?;
    clock.0.store(61_000, Ordering::SeqCst);
    let retry = store.claim(61_000, 1).await?.pop().unwrap();
    scheduler.execute(&retry).await?;
    let calls = dispatcher.0.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].expression, calls[1].expression);
    assert_eq!(calls[0].scheduled_time, calls[1].scheduled_time);
    drop(calls);
    assert_eq!(
        store.claim(120_000, 1).await?.pop().unwrap().scheduled_time,
        120_000
    );
    Ok(())
}

fn actor() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Jobs".into(),
        actor_id: "one".into(),
    }
}

fn definitions() -> Vec<CronDefinition> {
    vec![CronDefinition {
        actor_name: "Jobs".into(),
        method: "refresh".into(),
        expression: "* * * * *".into(),
        retries: 0,
    }]
}

async fn delivery_and_retry_policy_contract(store: &dyn CronStore) -> Result<()> {
    let mut schedules = definitions();
    store.publish("project", &schedules, 0).await?;
    store.register(&actor(), "north-america-east", 1).await?;
    assert!(store.claim(59_999, 1).await?.is_empty());
    let first = store
        .claim(60_000, 1)
        .await?
        .pop()
        .expect("registration creates wakeups");
    store
        .finish(&first, 60_000, CronOutcome::DeliveryFailed)
        .await?;
    assert!(store.claim(60_999, 1).await?.is_empty());
    let delivery = store.claim(61_000, 1).await?.pop().unwrap();
    assert_eq!(delivery.request_id(), first.request_id());
    assert_eq!(delivery.failures, 0);
    store
        .finish(&delivery, 61_000, CronOutcome::HandlerFailed)
        .await?;
    assert!(store.claim(119_999, 1).await?.is_empty());

    schedules[0].retries = 1;
    store.publish("project", &schedules, 119_000).await?;
    let next = store.claim(120_000, 1).await?.pop().unwrap();
    assert_eq!(next.id, first.id);
    assert_ne!(next.request_id(), first.request_id());
    store
        .finish(&next, 120_000, CronOutcome::HandlerFailed)
        .await?;
    let retry = store.claim(121_000, 1).await?.pop().unwrap();
    assert_eq!(retry.scheduled_time, 120_000);
    assert_eq!(retry.failures, 1);
    store
        .finish(&retry, 121_000, CronOutcome::DeliveryFailed)
        .await?;
    let redelivery = store.claim(123_000, 1).await?.pop().unwrap();
    assert_eq!(redelivery.failures, 1);
    store
        .finish(&redelivery, 123_000, CronOutcome::HandlerFailed)
        .await?;
    assert!(store.claim(179_999, 1).await?.is_empty());
    let following = store.claim(180_000, 1).await?.pop().unwrap();
    assert_eq!(following.failures, 0);
    assert_eq!(following.attempt, 1);
    Ok(())
}

#[tokio::test]
async fn sqlite_separates_delivery_recovery_from_opt_in_handler_retries() -> Result<()> {
    delivery_and_retry_policy_contract(&SqliteCronStore::open(":memory:")?).await
}

#[tokio::test]
async fn postgres_separates_delivery_recovery_from_opt_in_handler_retries() -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        let store = PostgresCronStore::new(
            crate::postgres::PostgresDatabase::connect(&database.url).await?,
        );
        delivery_and_retry_policy_contract(&store).await
    })
    .await
}

#[tokio::test]
async fn independent_postgres_replicas_recover_claims_and_share_reconciliation_work() -> Result<()>
{
    crate::postgres::testing::with_postgres(async |database| {
        let first = PostgresCronStore::new(
            crate::postgres::PostgresDatabase::connect(&database.url).await?,
        );
        let second = PostgresCronStore::new(
            crate::postgres::PostgresDatabase::connect(&database.url).await?,
        );
        first.publish("project", &definitions(), 0).await?;
        first.register(&actor(), "north-america-east", 1).await?;
        let actor_two = ActorKey {
            actor_id: "two".into(),
            ..actor()
        };
        second.register(&actor_two, "north-america-east", 1).await?;
        let (a, b) = tokio::join!(first.claim(60_000, 1), second.claim(60_000, 1));
        let a = a?.pop().unwrap();
        let b = b?.pop().unwrap();
        assert_ne!(a.id, b.id);
        assert!(second.claim(60_001, 2).await?.is_empty());
        assert!(second.finish(&b, 60_002, CronOutcome::Completed).await?);
        assert!(first.renew(&a, 80_000).await?);
        let expired = second
            .claim(140_000, 2)
            .await?
            .into_iter()
            .find(|job| job.id == a.id)
            .unwrap();
        assert_eq!(expired.request_id(), a.request_id());
        assert!(!first.finish(&a, 140_001, CronOutcome::Completed).await?);
        assert!(!first.renew(&a, 140_001).await?);
        assert!(
            second
                .finish(&expired, 140_001, CronOutcome::Completed)
                .await?
        );
        let (a, b) = tokio::join!(
            first.claim_reconciliation(0),
            second.claim_reconciliation(0)
        );
        assert_eq!(usize::from(a?) + usize::from(b?), 1);
        assert!(!second.claim_reconciliation(59_999).await?);
        assert!(second.claim_reconciliation(60_000).await?);
        Ok(())
    })
    .await
}

#[test]
fn parses_cloudflare_expressions_in_utc() -> Result<()> {
    assert_eq!(next_after("*/5 * * * *", 0)?, 300_000);
    for expression in [
        "0 17 * * SUN",
        "0 17 * * 1",
        "0 18 * * FRIL",
        "59 23 LW * *",
        "0 0 15W * *",
        "0 0 * JAN MON#2",
    ] {
        assert!(next_after(expression, 0)? > 0, "{expression}");
    }
    assert_eq!(next_after("0 17 * * SUN", 0)?, next_after("0 17 * * 1", 0)?);
    for expression in [
        "* * * *",
        "0 0 0 * * *",
        "60 * * * *",
        "* * * * 0",
        "0 0 31 FEB *",
    ] {
        assert!(next_after(expression, 0).is_err(), "{expression}");
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_registration_and_publication_do_not_lose_wakeups() -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        let first = PostgresCronStore::new(
            crate::postgres::PostgresDatabase::connect(&database.url).await?,
        );
        let second = PostgresCronStore::new(
            crate::postgres::PostgresDatabase::connect(&database.url).await?,
        );
        first.publish("project", &definitions(), 0).await?;
        let mut schedules = definitions();
        schedules.push(CronDefinition {
            method: "cleanup".into(),
            ..schedules[0].clone()
        });
        let registration = async {
            for id in 0..10 {
                let actor = ActorKey {
                    actor_id: id.to_string(),
                    ..actor()
                };
                first.register(&actor, "north-america-east", 1).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        tokio::try_join!(registration, second.publish("project", &schedules, 2))?;
        first
            .register(
                &ActorKey {
                    actor_id: "0".into(),
                    ..actor()
                },
                "north-america-east",
                90_000,
            )
            .await?;
        second.publish("project", &schedules, 90_000).await?;
        let due = second.claim(90_000, 100).await?;
        assert_eq!(due.len(), 20);
        assert!(due.iter().all(|job| job.scheduled_time == 60_000));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn sqlite_recovers_wakeups_after_restart_and_reconciles_deployments() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("crons.sqlite3");
    let store = SqliteCronStore::open(&path)?;
    store.publish("project", &definitions(), 0).await?;
    store.register(&actor(), "north-america-east", 1).await?;
    let abandoned = store.claim(60_000, 1).await?.pop().unwrap();
    drop(store);
    let store = SqliteCronStore::open(&path)?;
    assert!(store.claim(60_001, 1).await?.is_empty());
    let recovered = store.claim(60_000 + LEASE_MS, 1).await?.pop().unwrap();
    assert_eq!(recovered.scheduled_time, 60_000);
    assert!(store.renew(&recovered, 60_001 + LEASE_MS).await?);
    assert!(
        !store
            .finish(&abandoned, 60_002 + LEASE_MS, CronOutcome::Completed)
            .await?
    );
    store.publish("project", &[], 60_003 + LEASE_MS).await?;
    assert!(!store.renew(&recovered, 60_004 + LEASE_MS).await?);
    assert!(store.claim(1_000_000, 10).await?.is_empty());
    store.publish("project", &definitions(), 1_000_000).await?;
    assert!(store.claim(1_000_000, 10).await?.is_empty());
    assert_eq!(store.claim(1_020_000, 10).await?.len(), 1);
    store.remove_project("project").await?;
    assert!(store.projects().await?.is_empty());
    Ok(())
}
