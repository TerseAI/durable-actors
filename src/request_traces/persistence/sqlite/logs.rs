use anyhow::Result;
use rusqlite::{Connection, params, params_from_iter, types::Value};

use crate::request_traces::logs::{ActorLog, LogPage, LogQuery};

pub(super) fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS actor_logs (
            position INTEGER PRIMARY KEY AUTOINCREMENT,
            log_id TEXT NOT NULL UNIQUE,
            project_id TEXT NOT NULL,
            actor_name TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            request_id TEXT,
            timestamp_ms INTEGER NOT NULL,
            log TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS actor_logs_time ON actor_logs(project_id, timestamp_ms DESC, log_id DESC);",
    )?;
    Ok(())
}

pub(super) fn append(
    connection: &mut Connection,
    records: &[ActorLog],
    retention: usize,
) -> Result<()> {
    let transaction = connection.transaction()?;
    {
        let mut statement = transaction.prepare(
            "INSERT INTO actor_logs (log_id, project_id, actor_name, actor_id, request_id, timestamp_ms, log)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (log_id) DO NOTHING",
        )?;
        for record in records {
            statement.execute(params![
                record.log_id,
                record.project_id,
                record.actor_name,
                record.actor_id,
                record.request_id,
                i64::try_from(record.timestamp_ms)?,
                serde_json::to_string(record)?,
            ])?;
        }
    }
    transaction.execute(
        "DELETE FROM actor_logs WHERE position <= COALESCE((SELECT position FROM actor_logs ORDER BY position DESC LIMIT 1 OFFSET ?1), 0)",
        [retention as i64],
    )?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn query(
    connection: &mut Connection,
    project_id: &str,
    query: &LogQuery,
) -> Result<LogPage> {
    let mut clauses = vec!["project_id = ?"];
    let mut values = vec![Value::Text(project_id.into())];
    for (clause, value) in [
        ("actor_name = ?", query.actor_name.clone().map(Value::Text)),
        ("actor_id = ?", query.actor_id.clone().map(Value::Text)),
        ("request_id = ?", query.request_id.clone().map(Value::Text)),
        (
            "timestamp_ms >= ?",
            query.from_ms.map(|ms| Value::Integer(ms as i64)),
        ),
        (
            "timestamp_ms <= ?",
            query.to_ms.map(|ms| Value::Integer(ms as i64)),
        ),
    ] {
        if let Some(value) = value {
            clauses.push(clause);
            values.push(value);
        }
    }
    if let Some(after) = query.after()? {
        clauses.push("(timestamp_ms, log_id) < (?, ?)");
        values.extend([
            Value::Integer(after.timestamp_ms as i64),
            Value::Text(after.log_id),
        ]);
    }
    values.push(Value::Integer((query.limit() + 1) as i64));
    let mut statement = connection.prepare(&format!(
        "SELECT project_id, log FROM actor_logs WHERE {} ORDER BY timestamp_ms DESC, log_id DESC LIMIT ?",
        clauses.join(" AND ")
    ))?;
    let records = statement
        .query_map(params_from_iter(values), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .map(|row| {
            let (project_id, json) = row?;
            Ok(ActorLog {
                project_id,
                ..serde_json::from_str(&json)?
            })
        })
        .collect::<Result<Vec<_>>>()?;
    LogPage::new(records, query.limit())
}
