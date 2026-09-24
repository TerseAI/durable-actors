use super::*;
use crate::request_traces::metrics::ClassMetrics;

pub(super) async fn overview(
    database: &PostgresDatabase,
    project: &str,
    range: &TimeRange,
) -> Result<OverviewMetrics> {
    let mut client = database.connection().await?;
    let transaction = transaction(&mut client, true).await?;
    let rows = transaction
        .query(
            include_str!("overview.sql"),
            &[
                &project,
                &range.from_ms.map(|ms| ms as i64),
                &range.to_ms.map(|ms| ms as i64),
            ],
        )
        .await?;
    let mut metrics = OverviewMetrics::default();
    for row in rows {
        let attempts: i64 = row.get(2);
        let completed: i64 = row.get(3);
        let metric = ClassMetrics {
            actor_name: row.get(0),
            count: row.get::<_, i64>(1) as u64,
            success: (attempts > 0).then(|| 100.0 * completed as f64 / attempts as f64),
            p95: row.get(4),
            queue_p95: row.get(5),
        };
        if metric.actor_name.is_empty() {
            metrics.total = metric;
        } else {
            metrics.classes.push(metric);
        }
    }
    Ok(metrics)
}

pub(super) async fn queue_waits(
    database: &PostgresDatabase,
    project: &str,
    query: &QueueWaitQuery,
) -> Result<Vec<QueueWaitRow>> {
    let mut client = database.connection().await?;
    let transaction = transaction(&mut client, true).await?;
    let rows = transaction
        .query(
            include_str!("queue_waits.sql"),
            &[
                &project,
                &query.from_ms.map(|ms| ms as i64),
                &query.to_ms.map(|ms| ms as i64),
                &query.actor_name,
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| QueueWaitRow {
            actor_name: row.get(0),
            actor_id: row.get(1),
            admitted: row.get::<_, i64>(2) as u64,
            average_ms: row.get(3),
            max_ms: row.get(4),
        })
        .collect())
}

pub(super) async fn websockets(
    database: &PostgresDatabase,
    project: &str,
    range: &TimeRange,
) -> Result<Vec<SocketSession>> {
    let mut client = database.connection().await?;
    let transaction = transaction(&mut client, true).await?;
    let rows = transaction
        .query(
            include_str!("websockets.sql"),
            &[
                &project,
                &range.from_ms.map(|ms| ms as i64),
                &range.to_ms.map(|ms| ms as i64),
            ],
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let connect: Option<String> = row.get(7);
            let metadata = connect
                .map(|event| serde_json::from_str::<serde_json::Value>(&event))
                .transpose()?
                .and_then(|mut event| event.as_object_mut()?.remove("metadata"));
            Ok(SocketSession {
                connection_id: row.get(0),
                actor_name: row.get(1),
                actor_id: row.get(2),
                host_id: row.get(3),
                opened_at_ms: row.get::<_, Option<i64>>(4).map(|ms| ms as u64),
                closed_at_ms: row.get::<_, Option<i64>>(5).map(|ms| ms as u64),
                last_seen_ms: row.get::<_, Option<i64>>(6).map(|ms| ms as u64),
                messages: row.get::<_, i64>(8) as u64,
                failures: row.get::<_, i64>(9) as u64,
                metadata,
            })
        })
        .collect()
}
