use crate::request_traces::{
    TracePage, TraceRecord,
    replay::{InvalidTraceCursor, ReplayQuery},
};
use anyhow::Result;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Cursor {
    project_id: String,
    generation: String,
    position: u64,
}

pub(super) struct Metadata {
    pub(super) generation: String,
    pub(super) head: u64,
    pub(super) pruned: u64,
    pub(super) total: u64,
}

pub(super) fn query(
    connection: &mut Connection,
    project_id: &str,
    query: &ReplayQuery,
) -> Result<TracePage> {
    let transaction = connection.transaction()?;
    let metadata = metadata(&transaction, project_id)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(|value| decode(value, project_id))
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|c| c.generation == metadata.generation && c.position > metadata.head)
    {
        return Err(InvalidTraceCursor.into());
    }
    let reset = cursor
        .as_ref()
        .is_some_and(|c| c.generation != metadata.generation || c.position < metadata.pruned);
    let after = cursor.filter(|_| !reset).map(|c| c.position);
    let mut records = select(&transaction, project_id, query.limit, after, metadata.head)?;
    let more = after.is_some() && records.len() > query.limit;
    records.truncate(query.limit);
    let position = if more {
        records.last().unwrap().sequence
    } else {
        metadata.head
    };
    let resume_cursor = encode(&Cursor {
        project_id: project_id.into(),
        generation: metadata.generation.clone(),
        position,
    })?;
    Ok(TracePage {
        epoch: metadata.generation,
        cursor: position,
        capacity: query.limit,
        evicted: metadata.total.saturating_sub(500),
        dropped: 0,
        persistence_failed: false,
        records,
        next_cursor: more.then(|| resume_cursor.clone()),
        resume_cursor,
        reset,
    })
}

pub(super) fn metadata(transaction: &Transaction<'_>, project_id: &str) -> Result<Metadata> {
    Ok(transaction.query_row(
        "SELECT generation, COALESCE(p.pruned, 0), COALESCE(p.total, 0), COALESCE(p.head, 0) FROM trace_meta LEFT JOIN trace_projects p ON p.project_id = ?1",
        [project_id],
        |row| Ok(Metadata { generation: row.get(0)?, pruned: row.get::<_, i64>(1)? as u64, total: row.get::<_, i64>(2)? as u64, head: row.get::<_, i64>(3)? as u64 }),
    )?)
}

fn select(
    transaction: &Transaction<'_>,
    project_id: &str,
    limit: usize,
    after: Option<u64>,
    head: u64,
) -> Result<Vec<TraceRecord>> {
    let order = if after.is_some() {
        "position ASC"
    } else {
        "started_at_ms DESC, position DESC"
    };
    let mut statement = transaction.prepare(&format!(
        "SELECT position, event FROM traces WHERE project_id = ?4 AND position > ?1 AND position <= ?2 ORDER BY {order} LIMIT ?3"
    ))?;
    let rows = statement.query_map(
        params![
            after.unwrap_or(0) as i64,
            head as i64,
            (limit + 1) as i64,
            project_id
        ],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?)),
    )?;
    rows.map(|row| {
        let (sequence, json) = row?;
        Ok(TraceRecord {
            sequence,
            event: serde_json::from_str(&json)?,
        })
    })
    .collect()
}

fn encode(cursor: &Cursor) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}
fn decode(value: &str, project_id: &str) -> Result<Cursor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| InvalidTraceCursor)?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| InvalidTraceCursor)?;
    if cursor.project_id != project_id || cursor.position > i64::MAX as u64 {
        return Err(InvalidTraceCursor.into());
    }
    Ok(cursor)
}

pub(super) fn resume_cursor(project_id: &str, generation: &str, position: u64) -> Result<String> {
    encode(&Cursor {
        project_id: project_id.into(),
        generation: generation.into(),
        position,
    })
}
