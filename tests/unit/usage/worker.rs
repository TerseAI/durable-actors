use super::*;
use crate::postgres::testing::with_postgres;
use std::sync::atomic::{AtomicBool, Ordering};

struct Publisher(AtomicBool);
#[async_trait]
impl UsageSink for Publisher {
    async fn deliver(&self, _: &[UsageEvent]) -> Result<()> {
        ensure!(!self.0.load(Ordering::SeqCst), "publish response lost");
        Ok(())
    }
}

#[tokio::test]
async fn failed_publish_retries_the_same_events_after_restart() -> Result<()> {
    with_postgres(async |db| {
        let database = PostgresDatabase::connect(&db.url).await?;
        let outbox = UsageOutbox::new(database.clone());
        let assignment = crate::usage::tests::fixture();
        let mut connection = database.connection().await?;
        let transaction = connection.transaction().await?;
        UsageOutbox::enqueue_in(&transaction, &assignment, UsageEventType::Started, 1_000).await?;
        transaction.commit().await?;
        let expected = outbox.pending().await?;
        let sink = Arc::new(Publisher(AtomicBool::new(true)));
        let relay = UsageRelay::new(outbox.clone(), sink.clone());
        assert!(relay.deliver().await.is_err());
        let restarted = UsageOutbox::new(PostgresDatabase::connect(&db.url).await?);
        assert_eq!(expected, restarted.pending().await?);
        sink.0.store(false, Ordering::SeqCst);
        UsageRelay::new(restarted.clone(), sink).deliver().await?;
        assert!(restarted.pending().await?.is_empty());
        Ok(())
    })
    .await
}
