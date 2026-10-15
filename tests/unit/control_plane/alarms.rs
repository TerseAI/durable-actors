use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;

use super::*;

fn actor() -> ActorKey {
    actor_with_id("one")
}

fn actor_with_id(actor_id: &str) -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: actor_id.into(),
    }
}

fn claimed(actor: ActorKey, revision: u64) -> ClaimedAlarm {
    ClaimedAlarm { actor, revision }
}

async fn with_each_store(test: impl AsyncFn(&dyn AlarmStore) -> Result<()>) -> Result<()> {
    test(&MemoryAlarmStore::default()).await?;
    crate::postgres::testing::with_postgres(async |database| {
        test(&PostgresAlarmStore::new(PostgresDatabase::lazy(
            &database.url,
        )?))
        .await
    })
    .await
}

#[test]
fn defaults_bound_alarm_delivery() {
    assert_eq!(DEFAULT_POLL_INTERVAL, Duration::from_millis(100));
    assert_eq!(MAX_IN_FLIGHT, 100);
    assert_eq!(MAX_ATTEMPTS, 3);
    assert_eq!(ALARM_CONNECT_TIMEOUT, Duration::from_secs(5));
    assert_eq!(ALARM_REQUEST_TIMEOUT, Duration::from_secs(30));
}

#[test]
fn queued_alarms_outlive_every_attempt_and_backoff() {
    let jittered_backoff: Duration = (0..MAX_ATTEMPTS - 1)
        .map(|retry| MIN_RETRY_DELAY * 2u32.pow(retry as u32) * 2)
        .sum();
    assert!(QUEUED_TIMEOUT > ATTEMPT_TIMEOUT * MAX_ATTEMPTS as u32 + jittered_backoff);
}

#[tokio::test]
async fn due_alarms_are_claimed_once_while_queued() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;

        assert!(store.claim_due(99, 0, 100).await?.is_empty());
        assert_eq!(
            store.claim_due(100, 0, 100).await?,
            vec![claimed(actor(), 1)]
        );
        assert!(store.claim_due(200, 99, 100).await?.is_empty());
        assert_eq!(store.get(&actor()).await?, Some(100));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn queued_alarms_are_reclaimed_after_their_window_expires() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;
        store.claim_due(100, 0, 100).await?;

        assert_eq!(
            store.claim_due(400, 100, 100).await?,
            vec![claimed(actor(), 1)]
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn due_alarms_are_claimed_in_bounded_batches() -> Result<()> {
    with_each_store(async |store| {
        for index in 0..101 {
            store
                .update(&actor_with_id(&index.to_string()), Some(100))
                .await?;
        }

        assert_eq!(store.claim_due(100, 0, 100).await?.len(), 100);
        assert_eq!(store.claim_due(100, 0, 100).await?.len(), 1);
        assert!(store.claim_due(100, 0, 100).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_oldest_due_alarms_are_claimed_first() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor_with_id("late"), Some(200)).await?;
        store.update(&actor_with_id("early"), Some(100)).await?;

        assert_eq!(
            store.claim_due(200, 0, 1).await?,
            vec![claimed(actor_with_id("early"), 1)]
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn latest_update_replaces_or_deletes_an_alarm() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(200)).await?;
        store.update(&actor(), Some(100)).await?;
        assert_eq!(store.get(&actor()).await?, Some(100));

        store.update(&actor(), None).await?;
        assert_eq!(store.get(&actor()).await?, None);
        assert!(store.claim_due(200, 0, 100).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn finishing_a_claim_clears_the_alarm() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;
        let alarm = store.claim_due(100, 0, 100).await?.remove(0);

        store.finish(&alarm).await?;

        assert_eq!(store.get(&actor()).await?, None);
        assert!(store.claim_due(1_000, 1_000, 100).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_alarm_replaced_during_delivery_survives_finish_and_is_claimable() -> Result<()> {
    with_each_store(async |store| {
        store.update(&actor(), Some(100)).await?;
        let alarm = store.claim_due(100, 0, 100).await?.remove(0);

        store.update(&actor(), Some(150)).await?;
        store.finish(&alarm).await?;

        assert_eq!(store.get(&actor()).await?, Some(150));
        assert_eq!(
            store.claim_due(150, 0, 100).await?,
            vec![claimed(actor(), 2)]
        );
        Ok(())
    })
    .await
}

#[tokio::test(start_paused = true)]
async fn delivery_retries_in_memory_until_an_attempt_succeeds() -> Result<()> {
    let store = MemoryAlarmStore::default();
    let alarm = due_alarm(&store).await?;
    let delivery = FailingDelivery::new(1);

    deliver(&store, &delivery, &alarm).await;

    assert_eq!(delivery.attempts(), 2);
    assert_eq!(store.get(&actor()).await?, None);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn delivery_gives_up_and_finishes_after_max_attempts() -> Result<()> {
    let store = MemoryAlarmStore::default();
    let alarm = due_alarm(&store).await?;
    let delivery = FailingDelivery::new(usize::MAX);

    deliver(&store, &delivery, &alarm).await;

    assert_eq!(delivery.attempts(), MAX_ATTEMPTS);
    assert_eq!(store.get(&actor()).await?, None);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_stalled_attempt_times_out_and_is_retried() -> Result<()> {
    let store = MemoryAlarmStore::default();
    let alarm = due_alarm(&store).await?;
    let (started, mut starts) = mpsc::unbounded_channel();
    let delivery = StallingDelivery {
        stalled: actor(),
        started,
    };
    let began = tokio::time::Instant::now();

    deliver(&store, &delivery, &alarm).await;

    assert_eq!(
        std::iter::from_fn(|| starts.try_recv().ok()).count(),
        MAX_ATTEMPTS
    );
    assert!(began.elapsed() >= ATTEMPT_TIMEOUT * MAX_ATTEMPTS as u32);
    assert_eq!(store.get(&actor()).await?, None);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_stalled_alarm_does_not_hold_back_newly_due_alarms() -> Result<()> {
    let store = Arc::new(MemoryAlarmStore::default());
    let (started, mut starts) = mpsc::unbounded_channel();
    let delivery = Arc::new(StallingDelivery {
        stalled: actor_with_id("slow"),
        started,
    });
    let stop = CancellationToken::new();
    store.update(&actor_with_id("slow"), Some(0)).await?;
    tokio::spawn(run(
        store.clone(),
        delivery,
        DEFAULT_POLL_INTERVAL,
        stop.clone(),
    ));
    assert_eq!(starts.recv().await, Some(actor_with_id("slow")));

    store.update(&actor_with_id("fast"), Some(0)).await?;

    assert_eq!(starts.recv().await, Some(actor_with_id("fast")));
    assert_eq!(store.get(&actor_with_id("slow")).await?, Some(0));
    stop.cancel();
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn at_most_max_in_flight_alarms_are_delivered_at_once() -> Result<()> {
    let store = Arc::new(MemoryAlarmStore::default());
    for index in 0..=MAX_IN_FLIGHT {
        store
            .update(&actor_with_id(&index.to_string()), Some(0))
            .await?;
    }
    let (started, mut starts) = mpsc::unbounded_channel();
    let delivery = Arc::new(StalledEverywhere { started });
    let stop = CancellationToken::new();
    tokio::spawn(run(
        store.clone(),
        delivery,
        DEFAULT_POLL_INTERVAL,
        stop.clone(),
    ));
    for _ in 0..MAX_IN_FLIGHT {
        starts.recv().await;
    }

    tokio::time::sleep(DEFAULT_POLL_INTERVAL * 5).await;

    assert!(starts.try_recv().is_err());
    stop.cancel();
    Ok(())
}

async fn due_alarm(store: &MemoryAlarmStore) -> Result<ClaimedAlarm> {
    store.update(&actor(), Some(100)).await?;
    Ok(store.claim_due(100, 0, 1).await?.remove(0))
}

struct FailingDelivery {
    failures: usize,
    attempts: AtomicUsize,
}

impl FailingDelivery {
    fn new(failures: usize) -> Self {
        Self {
            failures,
            attempts: AtomicUsize::new(0),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AlarmDelivery for FailingDelivery {
    async fn attempt(&self, _alarm: &ClaimedAlarm) -> Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(attempt >= self.failures, "actor host is unavailable");
        Ok(())
    }
}

struct StallingDelivery {
    stalled: ActorKey,
    started: mpsc::UnboundedSender<ActorKey>,
}

#[async_trait]
impl AlarmDelivery for StallingDelivery {
    async fn attempt(&self, alarm: &ClaimedAlarm) -> Result<()> {
        self.started.send(alarm.actor.clone())?;
        if alarm.actor == self.stalled {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

struct StalledEverywhere {
    started: mpsc::UnboundedSender<ActorKey>,
}

#[async_trait]
impl AlarmDelivery for StalledEverywhere {
    async fn attempt(&self, alarm: &ClaimedAlarm) -> Result<()> {
        self.started.send(alarm.actor.clone())?;
        std::future::pending().await
    }
}
