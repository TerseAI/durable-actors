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
        assert_eq!(pending[0].end_ms, 10_000);
        assert_eq!(pending[0].assignment.cpu_millis, 2_000);
        assert_eq!(pending[0].assignment.memory_mib, 8_192);
        restarted
            .observe(&assignment.session_id, Observation::Running, 22_000)
            .await?;
        assert_eq!(pending[0], restarted.pending().await?[0]);
        restarted.ack(&pending).await?;
        assert_eq!(restarted.pending().await?[0].start_ms, 10_000);
        restarted
            .observe(
                &assignment.session_id,
                Observation::Stopped(Some(25_000)),
                30_000,
            )
            .await?;
        let final_interval = restarted.pending().await?;
        assert_eq!(final_interval[0].start_ms, 10_000);
        assert_eq!(final_interval.last().unwrap().end_ms, 25_000);
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

pub(super) fn fixture() -> UsageAssignment {
    UsageAssignment {
        billing_account_id: None,
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
        assert_eq!(pending.len(), 100);
        assert_eq!(
            pending
                .iter()
                .map(|event| event.end_ms - event.start_ms)
                .sum::<i64>(),
            999_000
        );
        assert!(
            pending
                .iter()
                .all(|event| event.end_ms - event.start_ms <= 10_000)
        );
        let mut previous_end = 1_000;
        loop {
            let batch = journal.pending().await?;
            if batch.is_empty() {
                break;
            }
            for event in &batch {
                assert_eq!(event.start_ms, previous_end);
                previous_end = event.end_ms;
            }
            journal.ack(&batch).await?;
        }
        assert_eq!(previous_end, 7_200_000);
        journal
            .observe(
                &assignment.session_id,
                Observation::Stopped(None),
                8_000_000,
            )
            .await?;
        let tail = journal.pending().await?;
        assert_eq!((tail[0].start_ms, tail[0].end_ms), (7_200_000, 7_202_000));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_publishers_and_stale_acknowledgments_preserve_the_final_tail() -> Result<()> {
    with_postgres(async |db| {
        let journal = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        let assignment = fixture();
        journal.start(&assignment, 1_234).await?;
        journal
            .observe(&assignment.session_id, Observation::Running, 35_678)
            .await?;
        let first = journal.pending().await?;
        journal
            .observe(&assignment.session_id, Observation::Stopped(None), 99_999)
            .await?;
        let second = journal.pending().await?;
        assert_eq!(first, second[..first.len()]);
        let (a, b) = tokio::join!(journal.ack(&first), journal.ack(&second));
        a?;
        b?;
        journal.ack(&first).await?;
        assert!(journal.pending().await?.is_empty());
        assert_eq!(second.last().unwrap().end_ms, 35_678);
        let mut changed = assignment.clone();
        changed.billing_account_id = Some("different-account".into());
        assert!(journal.start(&changed, 1_234).await.is_err());
        Ok(())
    })
    .await
}
