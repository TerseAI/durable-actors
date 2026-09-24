use anyhow::Result;
use rusqlite::{Connection, Transaction, params_from_iter, types::Value};

use super::{
    cursor::{HistoryCursor, HistoryPage},
    replay,
};
use crate::request_traces::{TracePage, TraceRecord, history::HistoryQuery};

pub(super) fn query(
    connection: &mut Connection,
    project_id: &str,
    query: &HistoryQuery,
) -> Result<TracePage> {
    let transaction = connection.transaction()?;
    let metadata = replay::metadata(&transaction, project_id)?;
    let page = HistoryPage::new(project_id, query, metadata)?;
    let records = select(
        &transaction,
        project_id,
        query,
        page.watermark,
        page.after.as_ref(),
    )?;
    page.finish(query.limit, records)
}

fn select(
    transaction: &Transaction<'_>,
    project_id: &str,
    query: &HistoryQuery,
    watermark: u64,
    cursor: Option<&HistoryCursor>,
) -> Result<Vec<TraceRecord>> {
    let mut clauses = vec!["project_id = ?", "position <= ?"];
    let mut values = vec![
        Value::Text(project_id.into()),
        Value::Integer(watermark as i64),
    ];
    for (clause, value) in [
        ("actor_name = ?", query.actor_name.clone().map(Value::Text)),
        ("actor_id = ?", query.actor_id.clone().map(Value::Text)),
        (
            "outcome = ?",
            query
                .outcome
                .map(|outcome| Value::Text(outcome.as_str().into())),
        ),
        (
            "started_at_ms >= ?",
            query.from_ms.map(|v| Value::Integer(v as i64)),
        ),
        (
            "started_at_ms <= ?",
            query.to_ms.map(|v| Value::Integer(v as i64)),
        ),
    ] {
        if let Some(value) = value {
            clauses.push(clause);
            values.push(value);
        }
    }
    if let Some(cursor) = cursor {
        clauses.push("(started_at_ms, position) < (?, ?)");
        values.extend([
            Value::Integer(cursor.time as i64),
            Value::Integer(cursor.sequence as i64),
        ]);
    }
    values.push(Value::Integer((query.limit + 1) as i64));
    let mut statement = transaction.prepare(&format!(
        "SELECT position, event FROM traces WHERE {} ORDER BY started_at_ms DESC, position DESC LIMIT ?",
        clauses.join(" AND ")
    ))?;
    let rows = statement.query_map(params_from_iter(values), |row| {
        Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?))
    })?;
    rows.map(|row| {
        let (sequence, json) = row?;
        Ok(TraceRecord {
            sequence,
            event: serde_json::from_str(&json)?,
        })
    })
    .collect()
}
