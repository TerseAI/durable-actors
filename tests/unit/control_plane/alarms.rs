use super::*;

fn actor() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

#[tokio::test]
async fn memory_store_claims_due_alarms_once() -> Result<()> {
    let store = MemoryAlarmStore::default();
    store.update(&actor(), Some(100)).await?;

    assert!(store.claim_due(99).await?.is_none());
    let claimed = store.claim_due(100).await?.unwrap();
    assert_eq!(claimed.actor, actor());
    assert_eq!(claimed.revision, 1);
    assert!(store.claim_due(100).await?.is_none());
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
    assert!(store.claim_due(200).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn restoring_a_claim_does_not_overwrite_newer_state() -> Result<()> {
    let store = MemoryAlarmStore::default();
    store.update(&actor(), Some(100)).await?;
    let claimed = store.claim_due(100).await?.unwrap();

    store.update(&actor(), Some(200)).await?;
    store.restore(&claimed).await?;

    assert!(store.claim_due(100).await?.is_none());
    assert_eq!(store.claim_due(200).await?.unwrap().revision, 2);
    Ok(())
}

#[tokio::test]
async fn postgres_store_claims_and_restores_due_alarms() -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        let store = PostgresAlarmStore::new(PostgresDatabase::lazy(&database.url)?);
        store.update(&actor(), Some(100)).await?;

        let claimed = store.claim_due(100).await?.unwrap();
        assert_eq!(claimed.revision, 1);
        assert!(store.claim_due(100).await?.is_none());

        store.restore(&claimed).await?;
        assert_eq!(store.claim_due(100).await?.unwrap().revision, 1);
        Ok(())
    })
    .await
}
