use super::fixtures::event;
use super::*;

#[tokio::test]
async fn appends_are_idempotent_and_reads_are_bounded_and_ordered() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence::new(directory.path().join("traces.sqlite3"));
    let first = vec![event("a"), event("b")];
    store.append(&first).await?;
    store.append(&first).await?;
    store.append(&[event("c")]).await?;
    assert_eq!(ids(store.load_recent(100).await?), ["a", "b", "c"]);
    assert_eq!(ids(store.load_recent(2).await?), ["b", "c"]);
    assert!(store.load_recent(0).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn local_retention_is_independent_of_the_read_limit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence {
        path: directory.path().join("traces.sqlite3"),
        retention: 3,
        connection: Arc::new(Mutex::new(None)),
    };
    store
        .append(&[event("a"), event("b"), event("c"), event("d")])
        .await?;
    assert_eq!(ids(store.load_recent(1).await?), ["d"]);
    assert_eq!(ids(store.load_recent(10).await?), ["b", "c", "d"]);
    Ok(())
}

#[tokio::test]
async fn failed_batches_do_not_partially_commit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence::new(directory.path().join("traces.sqlite3"));
    store.run(|connection| {
        connection.execute_batch("CREATE TRIGGER reject_bad BEFORE INSERT ON traces WHEN NEW.event_id = 'bad' BEGIN SELECT RAISE(ABORT, 'write failed'); END;")?;
        Ok(())
    }).await?;
    assert!(store.append(&[event("good"), event("bad")]).await.is_err());
    assert!(store.load_recent(10).await?.is_empty());
    Ok(())
}

fn ids(events: Vec<TraceEvent>) -> Vec<String> {
    events.into_iter().map(|event| event.event_id).collect()
}

impl SqliteTracePersistence {
    async fn load_recent(&self, limit: usize) -> Result<Vec<TraceEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut events: Vec<_> = self
            .history(
                "default",
                &HistoryQuery {
                    limit: limit.min(500),
                    ..Default::default()
                },
            )
            .await?
            .records
            .into_iter()
            .map(|r| r.event)
            .collect();
        events.reverse();
        Ok(events)
    }
}
