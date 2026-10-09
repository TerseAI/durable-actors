use super::*;
use crate::postgres::testing::with_postgres;
use std::sync::atomic::{AtomicBool, Ordering};

struct Observer;
#[async_trait]
impl UsageObserver for Observer {
    async fn observe(&self, _: &UsageAssignment) -> Result<Observation> {
        Ok(Observation::Running)
    }
}

struct Publisher(AtomicBool);
#[async_trait]
impl UsageSink for Publisher {
    async fn deliver(&self, _: &[UsageInterval]) -> Result<()> {
        ensure!(!self.0.load(Ordering::SeqCst), "publish response lost");
        Ok(())
    }
}

#[tokio::test]
async fn failed_publish_retries_from_the_same_checkpoint_after_restart() -> Result<()> {
    with_postgres(async |db| {
        let journal = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        let assignment = crate::usage::tests::fixture();
        journal.start(&assignment, 1_000).await?;
        journal.start(&assignment, 9_000).await?;
        journal
            .observe(&assignment.session_id, Observation::Running, 20_000)
            .await?;
        let expected = journal.pending().await?;
        assert_eq!(expected[0].start_ms, 1_000);
        let sink = Arc::new(Publisher(AtomicBool::new(true)));
        let worker = UsageWorker::new(journal.clone(), Arc::new(Observer), Some(sink.clone()));
        assert!(worker.deliver().await.is_err());
        let restarted = UsageJournal::new(PostgresDatabase::connect(&db.url).await?);
        assert_eq!(expected, restarted.pending().await?);
        sink.0.store(false, Ordering::SeqCst);
        UsageWorker::new(restarted.clone(), Arc::new(Observer), Some(sink))
            .deliver()
            .await?;
        assert!(restarted.pending().await?.is_empty());
        Ok(())
    })
    .await
}
