use anyhow::Result;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::{Connection, Transaction, params_from_iter, types::Value};
use serde::{Deserialize, Serialize};

use super::replay;
use crate::request_traces::{
    TracePage, TraceRecord, history::HistoryQuery, replay::InvalidTraceCursor,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    project_id: String,
    generation: String,
    watermark: u64,
    pruned: u64,
    time: u64,
    sequence: u64,
    filters: String,
}

pub(super) fn query(
    connection: &mut Connection,
    project_id: &str,
    query: &HistoryQuery,
) -> Result<TracePage> {
    let transaction = connection.transaction()?;
    let metadata = replay::metadata(&transaction, project_id)?;
    let retained: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM traces WHERE project_id = ?1",
        [project_id],
        |row| row.get(0),
    )?;
    let cursor = query
        .cursor
        .as_deref()
        .map(|value| decode(value, project_id, query, &metadata))
        .transpose()?;
    let reset = cursor
        .as_ref()
        .is_some_and(|c| c.generation != metadata.generation || c.pruned < metadata.pruned);
    let cursor = cursor.filter(|_| !reset);
    let watermark = cursor.as_ref().map_or(metadata.head, |c| c.watermark);
    let mut records = select(&transaction, project_id, query, watermark, cursor.as_ref())?;
    let more = records.len() > query.limit;
    records.truncate(query.limit);
    let next_cursor = if more {
        let last = records.last().unwrap();
        Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
            project_id: project_id.into(),
            generation: metadata.generation.clone(),
            watermark,
            pruned: metadata.pruned,
            time: last.event.trace.started_at_ms,
            sequence: last.sequence,
            filters: query.filter_key()?,
        })?))
    } else {
        None
    };
    Ok(TracePage {
        resume_cursor: replay::resume_cursor(project_id, &metadata.generation, watermark)?,
        epoch: metadata.generation,
        cursor: watermark,
        capacity: query.limit,
        evicted: metadata.total.saturating_sub(retained as u64),
        dropped: 0,
        persistence_failed: false,
        records,
        next_cursor,
        reset,
    })
}

fn select(
    transaction: &Transaction<'_>,
    project_id: &str,
    query: &HistoryQuery,
    watermark: u64,
    cursor: Option<&Cursor>,
) -> Result<Vec<TraceRecord>> {
    let mut clauses = vec!["project_id = ?", "position <= ?"];
    let mut values = vec![
        Value::Text(project_id.into()),
        Value::Integer(watermark as i64),
    ];
    for (clause, value) in [
        (
            "json_extract(event, '$.requestId') = ?",
            query.request_id.clone().map(Value::Text),
        ),
        (
            "json_extract(event, '$.connectionId') = ?",
            query.connection_id.clone().map(Value::Text),
        ),
        ("actor_name = ?", query.actor_name.clone().map(Value::Text)),
        ("actor_id = ?", query.actor_id.clone().map(Value::Text)),
        (
            "outcome = ?",
            query
                .outcome
                .map(serde_json::to_value)
                .transpose()?
                .and_then(|v| v.as_str().map(|s| Value::Text(s.into()))),
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

fn decode(
    value: &str,
    project_id: &str,
    query: &HistoryQuery,
    metadata: &replay::Metadata,
) -> Result<Cursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| InvalidTraceCursor)?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| InvalidTraceCursor)?;
    if cursor.project_id != project_id
        || cursor.generation.is_empty()
        || cursor.generation.len() > 64
        || cursor.watermark > i64::MAX as u64
        || cursor.pruned > cursor.watermark
        || cursor.sequence == 0
        || cursor.sequence > cursor.watermark
        || cursor.time > 9_007_199_254_740_991
        || cursor.filters != query.filter_key()?
        || (cursor.generation == metadata.generation
            && (cursor.watermark > metadata.head || cursor.pruned > metadata.pruned))
    {
        return Err(InvalidTraceCursor.into());
    }
    Ok(cursor)
}
