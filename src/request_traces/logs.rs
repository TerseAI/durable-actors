use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use super::{InvalidTraceQuery, TRACE_CAPACITY, history::HistoryQuery};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActorLog {
    pub log_id: String,
    #[serde(skip)]
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: String,
    pub request_id: Option<String>,
    pub timestamp_ms: u64,
    pub level: String,
    pub message: String,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LogQuery {
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub request_id: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

impl LogQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        HistoryQuery {
            request_id: self.request_id.clone(),
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            limit: self.limit(),
            cursor: self.cursor.clone(),
            ..Default::default()
        }
        .validate()
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit.unwrap_or(TRACE_CAPACITY)
    }

    pub(crate) fn matches(&self, log: &ActorLog) -> bool {
        self.actor_name
            .as_ref()
            .is_none_or(|name| *name == log.actor_name)
            && self.actor_id.as_ref().is_none_or(|id| *id == log.actor_id)
            && self
                .request_id
                .as_ref()
                .is_none_or(|id| log.request_id.as_ref() == Some(id))
    }

    pub(crate) fn after(&self) -> Result<Option<LogCursor>> {
        self.cursor.as_deref().map(LogCursor::decode).transpose()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogPage {
    pub records: Vec<ActorLog>,
    pub next_cursor: Option<String>,
}

impl LogPage {
    pub(crate) fn new(mut records: Vec<ActorLog>, limit: usize) -> Result<Self> {
        let more = records.len() > limit;
        records.truncate(limit);
        let next_cursor = match records.last() {
            Some(last) if more => Some(LogCursor::after(last).encode()?),
            _ => None,
        };
        Ok(Self {
            records,
            next_cursor,
        })
    }
}

/// Logs page newest first; the cursor is the last row returned.
#[derive(Serialize, Deserialize)]
pub(crate) struct LogCursor {
    pub timestamp_ms: u64,
    pub log_id: String,
}

impl LogCursor {
    fn after(log: &ActorLog) -> Self {
        Self {
            timestamp_ms: log.timestamp_ms,
            log_id: log.log_id.clone(),
        }
    }

    fn decode(value: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| InvalidTraceQuery)?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| InvalidTraceQuery)?;
        ensure!(cursor.log_id.len() <= 64, InvalidTraceQuery);
        Ok(cursor)
    }

    fn encode(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?))
    }
}
