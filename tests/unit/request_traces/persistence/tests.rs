use super::*;
use crate::request_traces::{RequestKind, RequestOutcome, RequestTrace};

#[tokio::test]
async fn live_snapshot_only_offers_a_resume_cursor() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let page = store
        .replay(&ReplayQuery {
            limit: 2,
            ..Default::default()
        })
        .await?;
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.cursor, 3);
    assert!(page.next_cursor.is_none());
    assert!(!page.resume_cursor.is_empty());
    Ok(())
}

#[tokio::test]
async fn live_snapshot_resumes_with_late_arrivals() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence::new(directory.path().join("traces.sqlite3"));
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let first = store
        .replay(&ReplayQuery {
            limit: 2,
            ..Default::default()
        })
        .await?;
    assert_eq!(
        first
            .records
            .iter()
            .map(|r| r.event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["c", "b"]
    );
    store.append(&[event("late")]).await?;
    let replay = store
        .replay(&ReplayQuery {
            cursor: Some(first.resume_cursor),
            ..Default::default()
        })
        .await?;
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].event.event_id, "late");
    Ok(())
}

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

#[tokio::test]
async fn legacy_snapshots_are_imported_once_without_modifying_the_original() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("request-traces.sqlite3");
    let mut legacy = serde_json::to_value(event("legacy"))?;
    legacy.as_object_mut().unwrap().remove("eventId");
    legacy["sequence"] = 42.into();
    let bytes = serde_json::to_vec(&serde_json::json!({"version": 1, "history": {
        "epoch": "old", "cursor": 42, "dropped": 2, "records": [legacy]
    }}))?;
    std::fs::write(path.with_extension("json"), &bytes)?;
    let store = SqliteTracePersistence::new(path.clone());
    let events = store.load_recent(10).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].trace.request_id, "same-request");
    let restored = SqliteTracePersistence::new(path.clone());
    assert_eq!(ids(restored.load_recent(10).await?), ids(events));
    assert_eq!(std::fs::read(path.with_extension("json"))?, bytes);
    Ok(())
}

#[tokio::test]
async fn a_corrupt_legacy_snapshot_is_not_silently_discarded() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("request-traces.sqlite3");
    std::fs::write(path.with_extension("json"), "broken")?;
    let store = SqliteTracePersistence::new(path);
    assert!(store.load_recent(10).await.is_err());
    assert!(store.load_recent(10).await.is_err());
    Ok(())
}

fn ids(events: Vec<TraceEvent>) -> Vec<String> {
    events.into_iter().map(|event| event.event_id).collect()
}

pub(super) fn event(id: &str) -> TraceEvent {
    TraceEvent {
        event_id: id.into(),
        host_id: "host".into(),
        session_id: "session".into(),
        trace: RequestTrace {
            request_id: "same-request".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
            kind: RequestKind::Method,
            operation: "increment".into(),
            connection_id: None,
            started_at_ms: 1,
            duration_ms: 1.0,
            queue_wait_ms: None,
            outcome: RequestOutcome::Completed,
        },
    }
}

#[tokio::test]
async fn replay_pages_do_not_skip_late_events_and_expired_cursors_reset() -> Result<()> {
    let store = SqliteTracePersistence {
        retention: 3,
        ..SqliteTracePersistence::in_memory()
    };
    let initial = store.replay(&ReplayQuery::default()).await?;
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let first = store
        .replay(&ReplayQuery {
            cursor: Some(initial.resume_cursor.clone()),
            limit: 2,
            ..Default::default()
        })
        .await?;
    assert_eq!(
        first
            .records
            .iter()
            .map(|r| r.event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(first.next_cursor.is_some());
    let last = store
        .replay(&ReplayQuery {
            cursor: Some(first.resume_cursor),
            limit: 2,
            ..Default::default()
        })
        .await?;
    assert_eq!(last.records[0].event.event_id, "c");
    assert!(last.next_cursor.is_none());
    store.append(&[event("d")]).await?;
    let reset = store
        .replay(&ReplayQuery {
            cursor: Some(initial.resume_cursor),
            ..Default::default()
        })
        .await?;
    assert!(reset.reset);
    assert_eq!(reset.records.len(), 3);
    assert_eq!(reset.records[0].event.event_id, "d");
    Ok(())
}

#[tokio::test]
async fn cursor_survives_reopening() -> Result<()> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("traces.sqlite3");
    let store = SqliteTracePersistence::new(path.clone());
    store.append(&[event("a"), event("b")]).await?;
    let first = store
        .replay(&ReplayQuery {
            limit: 1,
            ..Default::default()
        })
        .await?;
    drop(store);
    let store = SqliteTracePersistence::new(path);
    store.append(&[event("c")]).await?;
    let query = ReplayQuery {
        cursor: Some(first.resume_cursor),
        ..Default::default()
    };
    let replay = store.replay(&query).await?;
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].event.event_id, "c");
    let previous_format = serde_json::json!({
        "generation": first.epoch, "position": first.cursor,
        "direction": "forward", "watermark": first.cursor, "time": 0, "pruned": 0
    });
    let replay = store
        .replay(&ReplayQuery {
            cursor: Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&previous_format)?)),
            ..Default::default()
        })
        .await?;
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].event.event_id, "c");
    Ok(())
}

impl SqliteTracePersistence {
    async fn load_recent(&self, limit: usize) -> Result<Vec<TraceEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut events: Vec<_> = self
            .replay(&ReplayQuery {
                limit: limit.min(500),
                ..Default::default()
            })
            .await?
            .records
            .into_iter()
            .map(|r| r.event)
            .collect();
        events.reverse();
        Ok(events)
    }
}

#[tokio::test]
async fn existing_sqlite_events_survive_the_query_schema_migration() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("traces.sqlite3");
    {
        let connection = Connection::open(&path)?;
        connection.execute_batch("CREATE TABLE traces (position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL); PRAGMA user_version = 1;")?;
        connection.execute(
            "INSERT INTO traces (position, event_id, event) VALUES (42, ?1, ?2)",
            params!["saved", serde_json::to_string(&event("saved"))?],
        )?;
    }
    let store = SqliteTracePersistence::new(path);
    let page = store.replay(&ReplayQuery::default()).await?;
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].sequence, 42);
    assert_eq!(page.records[0].event.event_id, "saved");
    store.append(&[event("new")]).await?;
    assert_eq!(
        store.replay(&ReplayQuery::default()).await?.records[0].sequence,
        43
    );
    Ok(())
}
