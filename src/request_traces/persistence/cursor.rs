//! Request history pages, newest first. A page continues after the last record's start time and
//! event ID, and only with the filters it was read with.

use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::request_traces::{InvalidTraceQuery, TracePage, TraceRecord, history::HistoryQuery};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    project: String,
    filters: String,
    pub time: u64,
    pub event_id: String,
}

impl Cursor {
    pub(super) fn decode(value: &str, project: &str, query: &HistoryQuery) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| InvalidTraceQuery)?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| InvalidTraceQuery)?;
        ensure!(
            cursor.project == project
                && cursor.filters == query.filter_key()?
                && !cursor.event_id.is_empty()
                && cursor.event_id.len() <= 256,
            InvalidTraceQuery
        );
        Ok(cursor)
    }
}

/// The page for records read with one more than the limit, which tells whether more follow.
pub(super) fn page(
    project: &str,
    query: &HistoryQuery,
    mut records: Vec<TraceRecord>,
) -> Result<TracePage> {
    let more = records.len() > query.limit;
    records.truncate(query.limit);
    let next_cursor = match records.last() {
        Some(last) if more => Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
            project: project.into(),
            filters: query.filter_key()?,
            time: last.event.trace.started_at_ms,
            event_id: last.event.event_id.clone(),
        })?)),
        _ => None,
    };
    Ok(TracePage {
        records,
        next_cursor,
    })
}
