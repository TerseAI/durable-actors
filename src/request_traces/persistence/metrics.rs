use crate::request_traces::metrics::{
    ClassMetrics, OverviewMetrics, QueueWaitQuery, QueueWaitRow, SocketSession, TimeRange,
};
use anyhow::Result;
use rusqlite::{Connection, params};

pub(super) fn overview(connection: &mut Connection, range: &TimeRange) -> Result<OverviewMetrics> {
    let mut statement = connection.prepare(include_str!("overview.sql"))?;
    let rows = statement.query_map(
        params![
            range.from_ms.map(|ms| ms as i64),
            range.to_ms.map(|ms| ms as i64),
            range.project_id
        ],
        |row| {
            let attempts: i64 = row.get(2)?;
            let completed: i64 = row.get(3)?;
            Ok(ClassMetrics {
                actor_name: row.get(0)?,
                count: row.get::<_, i64>(1)? as u64,
                success: (attempts > 0).then(|| 100.0 * completed as f64 / attempts as f64),
                p95: row.get(4)?,
                queue_p95: row.get(5)?,
            })
        },
    )?;
    let mut metrics = OverviewMetrics::default();
    for row in rows {
        let row = row?;
        if row.actor_name.is_empty() {
            metrics.total = row;
        } else {
            metrics.classes.push(row);
        }
    }
    Ok(metrics)
}

pub(super) fn queue_waits(
    connection: &mut Connection,
    query: &QueueWaitQuery,
) -> Result<Vec<QueueWaitRow>> {
    let mut statement = connection.prepare(include_str!("queue_waits.sql"))?;
    let rows = statement.query_map(
        params![
            query.from_ms.map(|ms| ms as i64),
            query.to_ms.map(|ms| ms as i64),
            query.actor_name,
            query.project_id
        ],
        |row| {
            Ok(QueueWaitRow {
                actor_name: row.get(0)?,
                actor_id: row.get(1)?,
                admitted: row.get::<_, i64>(2)? as u64,
                average_ms: row.get(3)?,
                max_ms: row.get(4)?,
            })
        },
    )?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub(super) fn websockets(
    connection: &mut Connection,
    range: &TimeRange,
) -> Result<Vec<SocketSession>> {
    let mut statement = connection.prepare(include_str!("websockets.sql"))?;
    let rows = statement.query_map(
        params![
            range.from_ms.map(|ms| ms as i64),
            range.to_ms.map(|ms| ms as i64),
            range.project_id
        ],
        |row| {
            let connect_event: Option<String> = row.get(7)?;
            let metadata = connect_event
                .map(|event| serde_json::from_str::<serde_json::Value>(&event))
                .transpose()
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        7,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?
                .and_then(|mut event| event.as_object_mut()?.remove("metadata"));
            Ok(SocketSession {
                connection_id: row.get(0)?,
                actor_name: row.get(1)?,
                actor_id: row.get(2)?,
                host_id: row.get(3)?,
                opened_at_ms: row.get::<_, Option<i64>>(4)?.map(|ms| ms as u64),
                closed_at_ms: row.get::<_, Option<i64>>(5)?.map(|ms| ms as u64),
                last_seen_ms: row.get::<_, Option<i64>>(6)?.map(|ms| ms as u64),
                messages: row.get::<_, i64>(8)? as u64,
                failures: row.get::<_, i64>(9)? as u64,
                metadata,
            })
        },
    )?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
