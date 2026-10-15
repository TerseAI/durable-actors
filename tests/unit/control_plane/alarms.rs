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

#[test]
fn default_poll_interval_is_100_milliseconds() {
    assert_eq!(DEFAULT_POLL_INTERVAL, Duration::from_millis(100));
    assert_eq!(DEFAULT_BATCH_SIZE, 100);
    assert_eq!(ALARM_CONNECT_TIMEOUT, Duration::from_secs(5));
    assert_eq!(ALARM_REQUEST_TIMEOUT, Duration::from_secs(30));
}

#[tokio::test]
async fn memory_store_claims_due_alarms_once() -> Result<()> {
    let store = MemoryAlarmStore::default();
    store.update(&actor(), Some(100)).await?;

    assert!(store.claim_due(99, 100).await?.is_empty());
    let claimed = store.claim_due(100, 100).await?.pop().unwrap();
    assert_eq!(claimed.actor, actor());
    assert_eq!(claimed.revision, 1);
    assert!(store.claim_due(100, 100).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn memory_store_claims_due_alarms_in_bounded_batches() -> Result<()> {
    let store = MemoryAlarmStore::default();
    for index in 0..101 {
        store
            .update(&actor_with_id(&index.to_string()), Some(100))
            .await?;
    }

    assert_eq!(store.claim_due(100, 100).await?.len(), 100);
    assert_eq!(store.claim_due(100, 100).await?.len(), 1);
    assert!(store.claim_due(100, 100).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn latest_update_replaces_or_deletes_an_alarm() -> Result<()> {
    let store = MemoryAlarmStore::default();
    store.update(&actor(), Some(200)).await?;
    store.update(&actor(), Some(100)).await?;
    assert_eq!(store.get(&actor()).await?, Some(100));

    store.update(&actor(), None).await?;
    assert_eq!(store.get(&actor()).await?, None);
    assert!(store.claim_due(200, 100).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_update_after_a_claim_schedules_a_new_alarm() -> Result<()> {
    let store = MemoryAlarmStore::default();
    store.update(&actor(), Some(100)).await?;
    assert_eq!(store.claim_due(100, 100).await?.len(), 1);

    store.update(&actor(), Some(200)).await?;

    assert!(store.claim_due(100, 100).await?.is_empty());
    assert_eq!(store.claim_due(200, 100).await?.pop().unwrap().revision, 2);
    Ok(())
}

#[tokio::test]
async fn postgres_store_claims_due_alarms_once() -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        let store = PostgresAlarmStore::new(PostgresDatabase::lazy(&database.url)?);
        store.update(&actor(), Some(100)).await?;

        let claimed = store.claim_due(100, 100).await?.pop().unwrap();
        assert_eq!(claimed.revision, 1);
        assert!(store.claim_due(100, 100).await?.is_empty());
        Ok(())
    })
    .await
}
