use super::*;
use crate::{
    clock::Clock,
    request_traces::{
        RequestKind, RequestOutcome,
        config::AnalyticsConfig,
        persistence::{SqliteTracePersistence, TracePersistence, tests::event},
        pubsub::{GooglePublisher, PubSubTraceSink},
        sink::{TraceScope, TraceSink},
    },
};

#[tokio::test]
#[ignore = "requires an isolated, provisioned GCP analytics pipeline and ADC; publishes retained test events"]
async fn pubsub_bigquery_end_to_end_matches_sqlite() -> Result<()> {
    let config = AnalyticsConfig::from_lookup(&mut |name| std::env::var(name).ok())?
        .context("set DURABLE_ACTORS_ANALYTICS_* for a test dataset")?;
    let clock = Arc::new(crate::clock::SystemClock);
    let now = clock.now_ms()?;
    let project = format!("test-{}", uuid::Uuid::new_v4().simple());
    let scope = TraceScope {
        project_id: project.clone(),
        region: "test".into(),
        host_id: "host".into(),
        session_id: "session".into(),
    };
    let mut traces = Vec::new();
    for (index, operation) in [
        "onConnect",
        "onMessage",
        "onMessage",
        "onDisconnect",
        "increment",
        "increment",
    ]
    .iter()
    .enumerate()
    {
        let mut trace = event(&format!("event-{index}")).trace;
        trace.operation = (*operation).into();
        trace.started_at_ms = now - 10_000 + index as u64;
        trace.duration_ms = 10.0 + index as f64;
        trace.queue_wait_ms = (index != 0).then_some(index as f64);
        trace.outcome = match index {
            2 => RequestOutcome::Failed,
            5 => RequestOutcome::Rerouted,
            _ => RequestOutcome::Completed,
        };
        if index < 4 {
            trace.kind = RequestKind::Websocket;
            trace.connection_id = Some("connection".into());
        }
        if index == 0 {
            trace.metadata = Some(serde_json::json!({"name":"Ada"}));
        }
        traces.push(trace);
    }
    let sqlite = SqliteTracePersistence::in_memory();
    sqlite.append(&scope.events(traces.clone())).await?;
    let publisher = Arc::new(GooglePublisher::new(&config.topic).await?);
    let sink = PubSubTraceSink::new(
        publisher,
        clock.clone(),
        config.environment.clone(),
        vec!["name".into()],
        config.retention_days,
        1024,
    );
    sink.record(&scope, traces.clone(), 0).await?;
    sink.record(&scope, traces.clone(), 0).await?;
    let other = TraceScope {
        project_id: format!("other-{project}"),
        ..scope
    };
    sink.record(&other, traces, 0).await?;
    let executor = Arc::new(transport::BigQueryTransport::new(
        google_cloud_auth::credentials::Builder::default().build()?,
        config.query_project,
        config.location,
        config.maximum_bytes_billed,
    )?);
    let reader = BigQueryReader::new(
        executor,
        clock,
        config.table,
        config.environment,
        config.retention_days,
        Duration::from_secs(5),
        b"isolated-live-test",
    );
    let range = TimeRange {
        project_id: Some(project.clone()),
        from_ms: Some(now - 20_000),
        to_ms: Some(now),
    };
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if reader.metrics(&range).await?.total.count == 6 {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
    .await??;
    assert_eq!(
        serde_json::to_value(reader.metrics(&range).await?)?,
        serde_json::to_value(sqlite.metrics(&range).await?)?
    );
    let queue = QueueWaitQuery {
        project_id: range.project_id.clone(),
        from_ms: range.from_ms,
        to_ms: range.to_ms,
        actor_name: None,
    };
    assert_eq!(
        serde_json::to_value(reader.queue_waits(&queue).await?)?,
        serde_json::to_value(sqlite.queue_waits(&queue).await?)?
    );
    assert_eq!(
        serde_json::to_value(reader.websockets(&range).await?)?,
        serde_json::to_value(sqlite.websockets(&range).await?)?
    );
    let mut query = range.history();
    query.limit = 2;
    let mut records = Vec::new();
    loop {
        let page = reader.history(&query).await?;
        records.extend(page.records.into_iter().map(|row| row.event.trace.event_id));
        query.cursor = page.next_cursor;
        if query.cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        records,
        [
            "event-5", "event-4", "event-3", "event-2", "event-1", "event-0"
        ]
    );
    Ok(())
}
