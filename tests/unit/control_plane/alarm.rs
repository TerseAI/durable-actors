use super::*;

async fn exercise(store: &dyn AlarmStore) -> Result<()> {
    let actor = ActorKey {
        project_id: "test".into(),
        actor_name: "Timer".into(),
        actor_id: "one".into(),
    };
    let alarm = Alarm {
        generation: uuid::Uuid::new_v4().to_string(),
        deadline: 100,
    };
    store.register(&actor, "north-america-east", &alarm).await?;
    store.register(&actor, "north-america-east", &alarm).await?;
    assert!(store.claim(99, 5).await?.is_empty());
    let first = store.claim(100, 5).await?.remove(0);
    assert!(store.claim(101, 5).await?.is_empty());
    let retry = store.claim(100 + LEASE_MS, 5).await?.remove(0);
    assert_ne!(first.token, retry.token);
    assert!(!store.finish(&first, 100 + LEASE_MS, true).await?);
    assert!(store.finish(&retry, 101 + LEASE_MS, false).await?);
    assert!(store.claim(102 + LEASE_MS, 5).await?.is_empty());
    let recovered = store.claim(101 + LEASE_MS + 2_000, 5).await?.remove(0);
    assert!(
        store
            .finish(&recovered, 102 + LEASE_MS + 2_000, true)
            .await?
    );
    assert!(store.claim(i64::MAX / 2, 5).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn sqlite_alarm_claims_survive_restart_and_fence_expired_attempts() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("alarms.sqlite3");
    exercise(&SqliteAlarmStore::open(path.clone())?).await?;
    let actor = ActorKey {
        project_id: "test".into(),
        actor_name: "Timer".into(),
        actor_id: "restart".into(),
    };
    let alarm = Alarm {
        generation: uuid::Uuid::new_v4().to_string(),
        deadline: 0,
    };
    SqliteAlarmStore::open(path.clone())?
        .register(&actor, "north-america-east", &alarm)
        .await?;
    let deliveries = SqliteAlarmStore::open(path)?.claim(100, 1).await?;
    assert_eq!(deliveries[0].alarm, alarm);
    Ok(())
}

#[tokio::test]
async fn postgres_alarm_claims_fence_expired_attempts() -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        exercise(&PostgresAlarmStore::new(
            crate::postgres::PostgresDatabase::connect(&database.url).await?,
        ))
        .await
    })
    .await
}
