use super::*;
use crate::postgres::testing::with_postgres;

#[tokio::test]
async fn intervals_survive_restart_and_retry_without_double_counting() -> Result<()> {
    with_postgres(async |db| {
        let journal = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        let assignment = fixture();
        journal.start(&assignment, 1_000).await?;
        journal.start(&assignment, 9_000).await?;
        journal
            .observe(&assignment.session_id, Observation::Running, 11_000)
            .await?;
        journal
            .observe(&assignment.session_id, Observation::Running, 11_000)
            .await?;
        let restarted = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        let pending = restarted.pending().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].start_ms, 1_000);
        assert_eq!(pending[0].end_ms, 11_000);
        assert_eq!(pending[0].assignment.cpu_millis, 2_000);
        assert_eq!(pending[0].assignment.memory_mib, 8_192);
        assert_eq!(pending, restarted.pending().await?);
        restarted.ack(&[pending[0].id.clone()]).await?;
        assert!(restarted.pending().await?.is_empty());
        restarted
            .observe(
                &assignment.session_id,
                Observation::Stopped(Some(15_000)),
                20_000,
            )
            .await?;
        let final_interval = restarted.pending().await?;
        assert_eq!(final_interval[0].start_ms, 11_000);
        assert_eq!(final_interval[0].end_ms, 15_000);
        assert!(restarted.active().await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn missing_pods_stop_at_last_confirmed_observation() -> Result<()> {
    with_postgres(async |db| {
        let journal = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        let assignment = fixture();
        journal.start(&assignment, 1_000).await?;
        journal
            .observe(&assignment.session_id, Observation::Running, 2_000)
            .await?;
        journal
            .observe(&assignment.session_id, Observation::Stopped(None), 90_000)
            .await?;
        assert_eq!(journal.pending().await?.len(), 1);
        assert!(journal.active().await?.is_empty());
        Ok(())
    })
    .await
}

fn fixture() -> UsageAssignment {
    UsageAssignment {
        project_id: "project".into(),
        session_id: "session".into(),
        resource_id: "namespace/pod/uid".into(),
        region: "us-east1".into(),
        cpu_millis: 2_000,
        memory_mib: 8_192,
    }
}

#[tokio::test]
async fn recovery_splits_long_intervals_into_bounded_events() -> Result<()> {
    with_postgres(async |db| {
        let journal = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        let assignment = fixture();
        journal.start(&assignment, 1_000).await?;
        journal
            .observe(&assignment.session_id, Observation::Running, 7_202_000)
            .await?;
        let pending = journal.pending().await?;
        assert_eq!(pending.len(), 3);
        assert_eq!(
            pending
                .iter()
                .map(|event| event.end_ms - event.start_ms)
                .sum::<i64>(),
            7_201_000
        );
        assert!(
            pending
                .iter()
                .all(|event| event.end_ms - event.start_ms <= 3_600_000)
        );
        Ok(())
    })
    .await
}
