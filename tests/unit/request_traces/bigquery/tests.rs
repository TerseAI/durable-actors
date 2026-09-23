use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Executor {
    calls: AtomicUsize,
    queries: Mutex<Vec<QuerySpec>>,
}
#[async_trait]
impl QueryExecutor for Executor {
    async fn query(&self, query: QuerySpec) -> Result<QueryPage> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.queries.lock().unwrap().push(query);
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(QueryPage {
            rows: vec![],
            next: Some(ResultPage {
                job_id: "job".into(),
                page_token: "page".into(),
            }),
        })
    }
    async fn page(&self, _: ResultPage, _: u32) -> Result<QueryPage> {
        Ok(QueryPage {
            rows: vec![],
            next: None,
        })
    }
}
struct Clock;
impl crate::clock::Clock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(10 * 86_400_000)
    }
}
fn reader(executor: Arc<Executor>) -> BigQueryReader {
    BigQueryReader::new(
        executor,
        Arc::new(Clock),
        "gcp.dataset.trace_events_v1".into(),
        "prod".into(),
        30,
        Duration::from_secs(15),
        b"shared-key",
    )
}
fn query(project: &str) -> HistoryQuery {
    HistoryQuery {
        project_id: Some(project.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn identical_reads_share_one_job_and_scope_is_parameterized() -> Result<()> {
    let executor = Arc::new(Executor::default());
    let reader = Arc::new(reader(executor.clone()));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let reader = reader.clone();
        tasks.spawn(async move { reader.history(&query("tenant")).await });
    }
    while let Some(task) = tasks.join_next().await {
        task??;
    }
    assert_eq!(executor.calls.load(Ordering::Relaxed), 1);
    let mut second = query("other");
    second.actor_name = Some("Room".into());
    reader.history(&second).await?;
    assert_eq!(executor.calls.load(Ordering::Relaxed), 2);
    let queries = executor.queries.lock().unwrap();
    assert_eq!(queries[1].parameters["project_id"], "other");
    assert!(!queries[1].sql.contains("Room"));
    assert_eq!(queries[1].parameters["actor_name"], "Room");
    Ok(())
}

#[tokio::test]
async fn history_cursor_cannot_cross_project_or_filter_boundaries() -> Result<()> {
    let reader = reader(Arc::new(Executor::default()));
    let first = reader.history(&query("one")).await?;
    let cursor = first.next_cursor.unwrap();
    let mut other = query("two");
    other.cursor = Some(cursor.clone());
    assert!(reader.history(&other).await.is_err());
    let mut same = query("one");
    same.cursor = Some(cursor);
    assert!(reader.history(&same).await?.next_cursor.is_none());
    Ok(())
}

#[tokio::test]
async fn unscoped_reads_fail_before_submitting_a_job() {
    let executor = Arc::new(Executor::default());
    let reader = reader(executor.clone());
    assert!(reader.history(&HistoryQuery::default()).await.is_err());
    assert_eq!(executor.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn history_cursor_survives_a_different_backend_replica() -> Result<()> {
    let first = reader(Arc::new(Executor::default()))
        .history(&query("one"))
        .await?;
    let mut next = query("one");
    next.cursor = first.next_cursor;
    let second = reader(Arc::new(Executor::default())).history(&next).await?;
    assert!(!second.reset);
    assert_eq!(second.epoch, first.epoch);
    assert!(second.next_cursor.is_none());
    Ok(())
}
