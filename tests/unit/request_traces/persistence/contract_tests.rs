use super::postgres::PostgresTracePersistence;
use super::tests::event;
use super::*;
use crate::postgres::{PostgresDatabase, testing::with_postgres};
use crate::request_traces::{RequestKind, RequestOutcome};

async fn contract(store: &dyn TracePersistence) -> Result<()> {
    store.initialize().await?;
    let initial = store.replay("default", &ReplayQuery::default()).await?;
    assert!(initial.records.is_empty());
    let mut records = vec![event("first"), event("second"), event("last")];
    for (index, record) in records.iter_mut().enumerate() {
        record.trace.started_at_ms = 1000 + index as u64;
        record.trace.duration_ms = (index + 1) as f64 * 10.0;
        record.trace.queue_wait_ms = Some(index as f64);
    }
    records[1].trace.outcome = RequestOutcome::Failed;
    records[2].trace.outcome = RequestOutcome::Rerouted;
    let mut foreign = event("foreign");
    foreign.trace.project_id = "other".into();
    store.append(&records).await?;
    store.append(&records).await?;
    store.append(&[foreign]).await?;
    let first = store
        .history(
            "default",
            &HistoryQuery {
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&first), ["last", "second"]);
    let mut late = event("late");
    late.trace.started_at_ms = 500;
    store.append(&[late]).await?;
    let second = store
        .history(
            "default",
            &HistoryQuery {
                cursor: first.next_cursor.clone(),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&second), ["first"]);
    assert!(
        store
            .history(
                "other",
                &HistoryQuery {
                    cursor: first.next_cursor.clone(),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert!(
        store
            .history(
                "default",
                &HistoryQuery {
                    cursor: first.next_cursor,
                    actor_name: Some("Other".into()),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let replay = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(first.resume_cursor),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&replay), ["late"]);
    let replay = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(initial.resume_cursor),
                limit: 2,
            },
        )
        .await?;
    assert_eq!(ids(&replay), ["first", "second"]);
    let replay = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: replay.next_cursor,
                limit: 2,
            },
        )
        .await?;
    assert_eq!(ids(&replay), ["last", "late"]);
    assert!(replay.next_cursor.is_none());
    let filtered = store
        .history(
            "default",
            &HistoryQuery {
                from_ms: Some(1000),
                to_ms: Some(1001),
                outcome: Some(RequestOutcome::Failed),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(ids(&filtered), ["second"]);
    let range = TimeRange {
        from_ms: Some(1000),
        to_ms: Some(1002),
    };
    let metrics = store.metrics("default", &range).await?;
    assert_eq!(metrics.total.count, 3);
    assert_eq!(metrics.total.success, Some(50.0));
    assert_eq!(metrics.total.p95, Some(20.0));
    assert_eq!(metrics.total.queue_p95, Some(1.0));
    assert_eq!(metrics.classes.len(), 1);
    let waits = store
        .queue_waits(
            "default",
            &QueueWaitQuery {
                from_ms: range.from_ms,
                to_ms: range.to_ms,
                actor_name: Some("Counter".into()),
            },
        )
        .await?;
    assert_eq!(waits.len(), 1);
    assert_eq!(
        (waits[0].admitted, waits[0].average_ms, waits[0].max_ms),
        (2, 0.5, 1.0)
    );
    assert_eq!(
        store
            .metrics("empty", &TimeRange::default())
            .await?
            .total
            .count,
        0
    );
    assert_eq!(
        store
            .metrics("other", &TimeRange::default())
            .await?
            .total
            .count,
        1
    );
    socket_contract(store).await?;
    Ok(())
}

async fn socket_contract(store: &dyn TracePersistence) -> Result<()> {
    let mut records = Vec::new();
    for (id, operation, time, outcome) in [
        ("connect", "onConnect", 1000, RequestOutcome::Completed),
        ("message", "onMessage", 3000, RequestOutcome::Failed),
        ("close", "onDisconnect", 5000, RequestOutcome::Completed),
    ] {
        let mut record = event(id);
        record.trace.kind = RequestKind::Websocket;
        record.trace.connection_id = Some("socket".into());
        record.trace.operation = operation.into();
        record.trace.started_at_ms = time;
        record.trace.outcome = outcome;
        record.trace.metadata =
            (operation == "onConnect").then(|| serde_json::json!({"userId":"ada"}));
        records.push(record);
    }
    store.append(&records).await?;
    let sessions = store
        .websockets(
            "default",
            &TimeRange {
                from_ms: Some(2000),
                to_ms: Some(4000),
            },
        )
        .await?;
    assert_eq!(sessions.len(), 1);
    let session = &sessions[0];
    assert_eq!(
        (
            session.opened_at_ms,
            session.closed_at_ms,
            session.messages,
            session.failures
        ),
        (Some(1000), Some(5000), 1, 1)
    );
    assert_eq!(session.metadata, Some(serde_json::json!({"userId":"ada"})));
    assert!(
        store
            .websockets("other", &TimeRange::default())
            .await?
            .is_empty()
    );
    Ok(())
}

fn ids(page: &TracePage) -> Vec<&str> {
    page.records
        .iter()
        .map(|record| record.event.event_id.as_str())
        .collect()
}

#[tokio::test]
async fn sqlite_satisfies_the_analytics_contract() -> Result<()> {
    contract(&SqliteTracePersistence::in_memory()).await
}

#[tokio::test]
async fn postgres_satisfies_the_analytics_contract() -> Result<()> {
    with_postgres(async |db| {
        let store = PostgresTracePersistence::new(
            PostgresDatabase::lazy(&db.url)?,
            Duration::from_secs(30 * 86400),
        );
        contract(&store).await
    })
    .await
}

#[tokio::test]
async fn postgres_history_and_cursors_survive_new_control_plane_instances() -> Result<()> {
    with_postgres(async |db| {
        let first = PostgresTracePersistence::new(
            PostgresDatabase::lazy(&db.url)?,
            Duration::from_secs(86400),
        );
        first.append(&[event("a"), event("b")]).await?;
        let page = first
            .history(
                "default",
                &HistoryQuery {
                    limit: 1,
                    ..Default::default()
                },
            )
            .await?;
        drop(first);
        let second = PostgresTracePersistence::new(
            PostgresDatabase::lazy(&db.url)?,
            Duration::from_secs(86400),
        );
        second.append(&[event("c")]).await?;
        let older = second
            .history(
                "default",
                &HistoryQuery {
                    cursor: page.next_cursor,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(ids(&older), ["a"]);
        let live = second
            .replay(
                "default",
                &ReplayQuery {
                    cursor: Some(page.resume_cursor),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(ids(&live), ["c"]);
        assert!(!live.reset);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn postgres_retention_prunes_history_and_resets_expired_cursors() -> Result<()> {
    with_postgres(async |db| {
        let store = PostgresTracePersistence::new(PostgresDatabase::lazy(&db.url)?, Duration::from_secs(86400));
        let start = store.replay("default", &ReplayQuery::default()).await?;
        store.append(&[event("old"), event("recent")]).await?;
        let page = store.history("default", &HistoryQuery { limit: 1, ..Default::default() }).await?;
        db.pool.get().await?.execute("UPDATE durable_actors_traces SET received_at = now() - interval '2 days' WHERE event_id = 'old'", &[]).await?;
        assert_eq!(store.prune_batch().await?, 1);
        let history = store.history("default", &HistoryQuery { cursor: page.next_cursor, ..Default::default() }).await?;
        assert!(history.reset);
        assert_eq!(ids(&history), ["recent"]);
        assert_eq!(history.evicted, 1);
        let replay = store.replay("default", &ReplayQuery { cursor: Some(start.resume_cursor), ..Default::default() }).await?;
        assert!(replay.reset);
        assert_eq!(ids(&replay), ["recent"]);
        Ok(())
    }).await
}

#[tokio::test]
async fn postgres_serializes_project_writers_without_blocking_other_projects() -> Result<()> {
    with_postgres(async |db| {
        let store = Arc::new(PostgresTracePersistence::new(PostgresDatabase::lazy(&db.url)?, Duration::from_secs(86400)));
        store.append(&[event("initial")]).await?;
        let before = store.replay("default", &ReplayQuery::default()).await?;
        let mut client = db.pool.get().await?;
        let transaction = client.transaction().await?;
        transaction.query_one("SELECT head FROM durable_actors_trace_projects WHERE project_id = 'default' FOR UPDATE", &[]).await?;
        let mut pending = tokio::spawn({
            let store = store.clone();
            async move { store.append(&[event("waiting")]).await }
        });
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut pending).await.is_err());
        let mut other = event("independent");
        other.trace.project_id = "other".into();
        tokio::time::timeout(Duration::from_secs(2), store.append(&[other])).await??;
        transaction.commit().await?;
        pending.await??;
        let second = PostgresTracePersistence::new(PostgresDatabase::lazy(&db.url)?, Duration::from_secs(86400));
        second.append(&[event("after")]).await?;
        let page = second.replay("default", &ReplayQuery { cursor: Some(before.resume_cursor), ..Default::default() }).await?;
        assert_eq!(ids(&page), ["waiting", "after"]);
        assert!(page.records[0].sequence < page.records[1].sequence);
        Ok(())
    }).await
}

#[tokio::test]
async fn postgres_failed_batches_roll_back_events_and_cursor_metadata() -> Result<()> {
    with_postgres(async |db| {
        let store = PostgresTracePersistence::new(PostgresDatabase::lazy(&db.url)?, Duration::from_secs(86400));
        store.initialize().await?;
        db.pool.get().await?.batch_execute("ALTER TABLE durable_actors_traces ADD CONSTRAINT reject_bad CHECK (event_id <> 'bad')").await?;
        assert!(store.append(&[event("good"), event("bad")]).await.is_err());
        assert!(store.history("default", &HistoryQuery::default()).await?.records.is_empty());
        store.append(&[event("good")]).await?;
        let page = store.history("default", &HistoryQuery::default()).await?;
        assert_eq!(ids(&page), ["good"]);
        assert_eq!(page.cursor, 1);
        Ok(())
    }).await
}
