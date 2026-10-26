use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::{
    live::{LiveLogFeed, LiveLogUpdate},
    persistence::{LogPersistence, LogReport},
    *,
};
use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
};

#[tokio::test]
async fn live_logs_do_not_wait_for_blocked_persistence_or_actor_completion() -> Result<()> {
    let persistence = Arc::new(BlockedPersistence::default());
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let store = LogStore::start(persistence.clone(), LiveLogFeed::default(), stop);
    let mut live = store.live.subscribe();
    store.record(
        &actor("alpha"),
        "host",
        "session",
        vec![log("first", 100)],
        2,
    )?;
    let update = tokio::time::timeout(Duration::from_millis(100), live.recv()).await??;
    let LiveLogUpdate::Batch(report) = update.as_ref() else {
        panic!("expected log batch")
    };
    assert_eq!(report.records[0].project_id, "alpha");
    assert_eq!(report.records[0].host_id, "host");
    assert_eq!(report.records[0].log.fields["nested"]["count"], 3);
    tokio::time::timeout(Duration::from_secs(1), persistence.writing.notified()).await?;
    assert!(store.history("alpha", &LogQuery::default()).await.is_err());
    assert_eq!(
        store.recent("alpha", &LogQuery::default())?.records.len(),
        1
    );
    for index in 0..150 {
        store.record(
            &actor("alpha"),
            "host",
            "session",
            vec![log(&format!("queued-{index}"), 101)],
            0,
        )?;
    }
    assert!(store.status("alpha").persistence_failed);
    assert!(store.status("alpha").dropped > 2);
    assert_eq!(store.status("beta").dropped, 0);
    Ok(())
}

#[tokio::test]
async fn failed_initialization_keeps_live_logs_available() -> Result<()> {
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let store = LogStore::start(Arc::new(FailedPersistence), LiveLogFeed::default(), stop);
    tokio::task::yield_now().await;
    let mut live = store.live.subscribe();
    store.record(
        &actor("alpha"),
        "host",
        "session",
        vec![log("failed", 100)],
        0,
    )?;
    assert!(matches!(live.try_recv()?.as_ref(), LiveLogUpdate::Batch(_)));
    assert!(store.status("alpha").persistence_failed);
    Ok(())
}

#[tokio::test]
async fn sqlite_history_roundtrips_filters_paginates_and_deduplicates() -> Result<()> {
    let persistence = persistence::sqlite::SqliteLogPersistence::new(
        ":memory:".into(),
        Duration::from_secs(86400),
    );
    history_contract(&persistence).await
}

#[tokio::test]
async fn clickhouse_history_roundtrips_filters_paginates_and_deduplicates() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_CLICKHOUSE_URL") else {
        return Ok(());
    };
    let persistence = persistence::clickhouse::ClickHouseLogPersistence::new(
        crate::request_traces::persistence::clickhouse::ClickHouseConfig {
            url,
            database: std::env::var("DURABLE_ACTORS_TEST_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "default".into()),
            user: std::env::var("DURABLE_ACTORS_TEST_CLICKHOUSE_USER")
                .unwrap_or_else(|_| "default".into()),
            password: std::env::var("DURABLE_ACTORS_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        },
        Duration::from_secs(86400),
    )?;
    history_contract(&persistence).await
}

#[tokio::test]
async fn postgres_history_roundtrips_filters_paginates_and_deduplicates() -> Result<()> {
    crate::postgres::testing::with_postgres_schema(async |database| {
        let database = crate::postgres::PostgresDatabase::lazy(&database.url)?;
        let persistence = persistence::postgres::PostgresLogPersistence::new(
            database,
            Duration::from_secs(86400),
        );
        history_contract(&persistence).await
    })
    .await
}

async fn history_contract(persistence: &dyn LogPersistence) -> Result<()> {
    persistence.initialize().await?;
    let project = format!("logs-{}", uuid::Uuid::new_v4());
    let now = SystemClock.now_ms()?;
    let records = vec![
        attributed(&project, "a", 100),
        attributed(&project, "b", 100),
    ];
    let report = LogReport {
        id: uuid::Uuid::new_v4().to_string(),
        project: project.clone(),
        records: records.clone(),
        dropped: 4,
        received_at_ms: now,
    };
    persistence.append(&report).await?;
    persistence.append(&report).await?;
    let query = LogQuery {
        limit: 1,
        ..Default::default()
    };
    let first = persistence.history(&project, &query).await?;
    assert_eq!(first.dropped, 4);
    assert_eq!(first.records, vec![records[1].clone()]);
    let second = persistence
        .history(
            &project,
            &LogQuery {
                cursor: first.next_cursor.clone(),
                ..query.clone()
            },
        )
        .await?;
    assert_eq!(second.records, vec![records[0].clone()]);
    assert!(second.next_cursor.is_none());
    assert!(
        persistence
            .history("another-project", &query)
            .await?
            .records
            .is_empty()
    );
    assert!(
        persistence
            .history(
                "another-project",
                &LogQuery {
                    cursor: first.next_cursor.clone(),
                    ..query.clone()
                }
            )
            .await
            .is_err()
    );
    assert!(
        persistence
            .history(
                &project,
                &LogQuery {
                    actor_name: Some("Other".into()),
                    ..Default::default()
                }
            )
            .await?
            .records
            .is_empty()
    );
    assert!(
        persistence
            .history(
                &project,
                &LogQuery {
                    request_id: Some("missing".into()),
                    ..Default::default()
                }
            )
            .await?
            .records
            .is_empty()
    );
    let filtered = LogQuery {
        actor_name: Some("Counter".into()),
        actor_id: Some("one".into()),
        request_id: Some("request".into()),
        level: Some("ERROR".into()),
        search: Some("100%".into()),
        from_ms: Some(100),
        to_ms: Some(100),
        ..Default::default()
    };
    assert_eq!(
        persistence
            .history(&project, &filtered)
            .await?
            .records
            .len(),
        2
    );
    assert!(
        persistence
            .history(
                &project,
                &LogQuery {
                    search: Some("_wildcard".into()),
                    ..Default::default()
                }
            )
            .await?
            .records
            .is_empty()
    );
    persistence
        .append(&LogReport {
            id: uuid::Uuid::new_v4().to_string(),
            project: project.clone(),
            records: vec![attributed(&project, "expired", 200)],
            dropped: 7,
            received_at_ms: now - 2 * 86400 * 1000,
        })
        .await?;
    let page = persistence.history(&project, &LogQuery::default()).await?;
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.dropped, 4);
    Ok(())
}

fn actor(project: &str) -> ActorKey {
    ActorKey {
        project_id: project.into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

fn log(id: &str, timestamp_ms: u64) -> CustomerLog {
    CustomerLog {
        event_id: id.into(),
        timestamp_ms,
        stream: LogStream::Stderr,
        level: "ERROR".into(),
        message: "100% complete before failing".into(),
        fields: json!({"nested":{"count":3,"ok":false},"items":[1,"two"]}),
        request_id: Some("request".into()),
    }
}

fn attributed(project: &str, id: &str, timestamp: u64) -> AttributedLog {
    AttributedLog {
        project_id: project.into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
        host_id: "host".into(),
        session_id: "session".into(),
        log: log(id, timestamp),
    }
}

#[derive(Default)]
struct BlockedPersistence {
    writing: tokio::sync::Notify,
}
#[async_trait]
impl LogPersistence for BlockedPersistence {
    async fn initialize(&self) -> Result<()> {
        Ok(())
    }
    async fn append(&self, _: &LogReport) -> Result<()> {
        self.writing.notify_one();
        std::future::pending().await
    }
    async fn history(&self, _: &str, _: &LogQuery) -> Result<LogPage> {
        anyhow::bail!("history unavailable")
    }
}
struct FailedPersistence;
#[async_trait]
impl LogPersistence for FailedPersistence {
    async fn initialize(&self) -> Result<()> {
        anyhow::bail!("database unavailable")
    }
    async fn append(&self, _: &LogReport) -> Result<()> {
        anyhow::bail!("database unavailable")
    }
    async fn history(&self, _: &str, _: &LogQuery) -> Result<LogPage> {
        anyhow::bail!("database unavailable")
    }
}

#[tokio::test]
async fn shutdown_flushes_acknowledged_reports() -> Result<()> {
    let persistence = Arc::new(persistence::sqlite::SqliteLogPersistence::new(
        ":memory:".into(),
        Duration::from_secs(86400),
    ));
    let store = LogStore::start(
        persistence.clone(),
        LiveLogFeed::default(),
        CancellationToken::new(),
    );
    for index in 0..8 {
        store.record(
            &actor("shutdown"),
            "host",
            "session",
            vec![log(&format!("log-{index}"), index)],
            0,
        )?;
    }
    store.shutdown().await;
    assert_eq!(
        persistence
            .history("shutdown", &LogQuery::default())
            .await?
            .records
            .len(),
        8
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_is_bounded_when_log_storage_is_stalled() -> Result<()> {
    let store = LogStore::start(
        Arc::new(BlockedPersistence::default()),
        LiveLogFeed::default(),
        CancellationToken::new(),
    );
    store.record(
        &actor("shutdown"),
        "host",
        "session",
        vec![log("one", 1)],
        0,
    )?;
    tokio::time::timeout(Duration::from_secs(5), store.shutdown()).await?;
    assert!(store.status("shutdown").persistence_failed);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn live_snapshot_and_duplicate_reports_do_not_double_count_drops() -> Result<()> {
    let store = LogStore::default();
    let updates = store.live.subscribe();
    store.record(
        &actor("alpha"),
        "host",
        "session",
        vec![log("first", 100)],
        3,
    )?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(live::stream(
        store.clone(),
        "alpha".into(),
        LogQuery::default(),
        updates,
        sender,
    ));
    assert_eq!(read_page(&mut receiver).await?.dropped, 3);
    assert_eq!(read_page(&mut receiver).await?.dropped, 3);
    let report = LogReport {
        id: "remote-report".into(),
        project: "alpha".into(),
        records: vec![attributed("alpha", "remote", 101)],
        dropped: 2,
        received_at_ms: 100,
    };
    store.live.publish(&report);
    let page = read_page(&mut receiver).await?;
    assert_eq!(page.dropped, 5);
    store.live.publish(&report);
    assert_eq!(read_page(&mut receiver).await?.dropped, 5);
    store.live.status(true);
    while !read_page(&mut receiver).await?.persistence_failed {}
    tokio::time::advance(Duration::from_secs(91)).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while read_page(&mut receiver).await?.persistence_failed {}
        anyhow::Ok(())
    })
    .await??;
    store.shutdown().await;
    task.await?;
    Ok(())
}

async fn read_page(
    receiver: &mut tokio::sync::mpsc::Receiver<
        Result<axum::response::sse::Event, std::convert::Infallible>,
    >,
) -> Result<LogPage> {
    use axum::response::IntoResponse;
    let event = receiver.recv().await.unwrap();
    let response =
        axum::response::Sse::new(futures_util::stream::once(async move { event })).into_response();
    let bytes = axum::body::to_bytes(response.into_body(), 65536).await?;
    let text = std::str::from_utf8(&bytes)?;
    Ok(serde_json::from_str(
        text.lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap(),
    )?)
}

#[tokio::test]
async fn redis_broadcasts_customer_logs_between_controllers_before_persistence() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let redis = crate::redis_client::Redis::connect(&crate::redis_client::RedisConfig {
        url,
        ca_cert: None,
    })?;
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let publisher = LiveLogFeed::start(Some(redis.clone()), stop.clone());
    let subscriber = LiveLogFeed::start(Some(redis.clone()), stop.clone());
    let mut updates = subscriber.subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut command = redis::cmd("PUBSUB");
            command.arg("NUMSUB").arg("durable-actors:v1:customer-logs");
            let (_, count): (String, u64) = redis.run(command).await?;
            if count >= 2 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let report = LogReport {
        id: uuid::Uuid::new_v4().to_string(),
        project: "redis-tenant".into(),
        records: vec![attributed("redis-tenant", "remote-log", 100)],
        dropped: 2,
        received_at_ms: SystemClock.now_ms()?,
    };
    publisher.publish(&report);
    let update = tokio::time::timeout(Duration::from_secs(2), updates.recv()).await??;
    let LiveLogUpdate::Batch(received) = update.as_ref() else {
        panic!("expected remote report")
    };
    assert_eq!(received.id, report.id);
    assert_eq!(received.records, report.records);
    assert_eq!(received.dropped, 2);
    let reader = LogStore::start(
        Arc::new(BlockedPersistence::default()),
        subscriber,
        stop.clone(),
    );
    let (sender, mut pages) = tokio::sync::mpsc::channel(2);
    let stream = tokio::spawn(live::stream(
        reader.clone(),
        "redis-tenant".into(),
        LogQuery::default(),
        reader.live.subscribe(),
        sender,
    ));
    read_page(&mut pages).await?;
    publisher.status(true);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !read_page(&mut pages).await?.persistence_failed {}
        anyhow::Ok(())
    })
    .await??;
    publisher.gap("overflow".into(), "redis-tenant", 5);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = read_page(&mut pages).await?;
            if page.dropped >= 5 {
                assert!(page.persistence_failed);
                return anyhow::Ok(());
            }
        }
    })
    .await??;
    publisher.status(false);
    tokio::time::timeout(Duration::from_secs(2), async {
        while read_page(&mut pages).await?.persistence_failed {}
        anyhow::Ok(())
    })
    .await??;
    reader.shutdown().await;
    stream.await?;

    Ok(())
}
