use crate::request_traces::metrics::{
    ClassMetrics, OverviewMetrics, QueueWaitQuery, QueueWaitRow, TimeRange,
};
use anyhow::Result;
use rusqlite::{Connection, params};

pub(super) fn overview(
    connection: &mut Connection,
    project_id: &str,
    range: &TimeRange,
) -> Result<OverviewMetrics> {
    let mut statement = connection.prepare(include_str!("overview.sql"))?;
    let rows = statement.query_map(
        params![
            range.from_ms.map(|ms| ms as i64),
            range.to_ms.map(|ms| ms as i64),
            project_id
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
    project_id: &str,
    query: &QueueWaitQuery,
) -> Result<Vec<QueueWaitRow>> {
    let mut statement = connection.prepare(include_str!("queue_waits.sql"))?;
    let rows = statement.query_map(
        params![
            query.from_ms.map(|ms| ms as i64),
            query.to_ms.map(|ms| ms as i64),
            query.actor_name,
            project_id
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
