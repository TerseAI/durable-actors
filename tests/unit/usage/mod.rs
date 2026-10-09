use super::*;
use crate::postgres::testing::with_postgres;

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
async fn lifecycle_events_are_durable_idempotent_and_acknowledged_independently() -> Result<()> {
    with_postgres(async |db| {
        let database = PostgresDatabase::connect(&db.url).await?;
        let outbox = UsageOutbox::new(database.clone());
        let assignment = fixture();
        let mut connection = database.connection().await?;
        let transaction = connection.transaction().await?;
        UsageOutbox::enqueue_in(&transaction, &assignment, UsageEventType::Started, 1_000).await?;
        UsageOutbox::enqueue_in(&transaction, &assignment, UsageEventType::Started, 1_000).await?;
        UsageOutbox::enqueue_in(&transaction, &assignment, UsageEventType::Stopped, 9_000).await?;
        transaction.commit().await?;

        let pending = outbox.pending().await?;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, "sandbox_usage_v1:session:started");
        assert_eq!(pending[0].event_type, UsageEventType::Started);
        assert_eq!(pending[0].observed_at_ms, 1_000);
        assert_eq!(pending[1].id, "sandbox_usage_v1:session:stopped");
        assert_eq!(pending[1].event_type, UsageEventType::Stopped);
        assert_eq!(pending[1].observed_at_ms, 9_000);

        outbox.ack(std::slice::from_ref(&pending[0])).await?;
        assert_eq!(outbox.pending().await?, pending[1..]);
        outbox.ack(&pending[1..]).await?;
        assert!(outbox.pending().await?.is_empty());
        Ok(())
    })
    .await
}
