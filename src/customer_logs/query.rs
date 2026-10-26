use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use super::AttributedLog;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LogQuery {
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub request_id: Option<String>,
    pub level: Option<String>,
    pub search: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    #[serde(default = "page_size")]
    pub limit: usize,
    pub cursor: Option<String>,
}

impl Default for LogQuery {
    fn default() -> Self {
        Self {
            actor_name: None,
            actor_id: None,
            request_id: None,
            level: None,
            search: None,
            from_ms: None,
            to_ms: None,
            limit: page_size(),
            cursor: None,
        }
    }
}

impl LogQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=500).contains(&self.limit),
            "limit must be between 1 and 500"
        );
        ensure!(
            self.actor_name
                .iter()
                .chain(self.actor_id.iter())
                .chain(self.request_id.iter())
                .all(|id| !id.is_empty() && id.len() <= 256),
            "invalid log filter ID"
        );
        ensure!(
            self.level.as_deref().is_none_or(|level| matches!(
                level,
                "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL"
            )),
            "invalid log level"
        );
        ensure!(
            self.search.as_ref().is_none_or(|value| value.len() <= 512),
            "search exceeds limit"
        );
        ensure!(
            self.from_ms
                .into_iter()
                .chain(self.to_ms)
                .all(|value| value <= 8_640_000_000_000_000),
            "invalid timestamp"
        );
        ensure!(
            self.from_ms
                .zip(self.to_ms)
                .is_none_or(|(from, to)| from <= to),
            "fromMs must not exceed toMs"
        );
        ensure!(
            self.cursor
                .as_ref()
                .is_none_or(|value| !value.is_empty() && value.len() <= 8192),
            "invalid log cursor"
        );
        Ok(())
    }

    pub(crate) fn validate_for(&self, project: &str) -> Result<()> {
        self.validate()?;
        self.after(project)?;
        Ok(())
    }

    pub(crate) fn matches(&self, record: &AttributedLog) -> bool {
        self.actor_name
            .as_ref()
            .is_none_or(|value| *value == record.actor_name)
            && self
                .actor_id
                .as_ref()
                .is_none_or(|value| *value == record.actor_id)
            && self
                .request_id
                .as_ref()
                .is_none_or(|value| Some(value) == record.log.request_id.as_ref())
            && self
                .level
                .as_ref()
                .is_none_or(|value| *value == record.log.level)
            && self
                .search
                .as_ref()
                .is_none_or(|value| record.log.message.contains(value))
            && self
                .from_ms
                .is_none_or(|value| record.log.timestamp_ms >= value)
            && self
                .to_ms
                .is_none_or(|value| record.log.timestamp_ms <= value)
    }

    pub(super) fn after(&self, project: &str) -> Result<Option<Cursor>> {
        self.cursor
            .as_ref()
            .map(|value| {
                let cursor: Cursor = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value)?)?;
                ensure!(
                    cursor.project == project
                        && cursor.filters == self.filter_key()?
                        && cursor.time <= 8_640_000_000_000_000
                        && cursor.id.len() <= 512,
                    "log cursor does not match query"
                );
                Ok(cursor)
            })
            .transpose()
    }

    pub(super) fn finish(&self, project: &str, mut records: Vec<AttributedLog>) -> Result<LogPage> {
        let more = records.len() > self.limit;
        records.truncate(self.limit);
        let next_cursor = if more {
            records
                .last()
                .map(|last| -> Result<String> {
                    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
                        project: project.into(),
                        filters: self.filter_key()?,
                        time: last.log.timestamp_ms,
                        id: last.log.event_id.clone(),
                    })?))
                })
                .transpose()?
        } else {
            None
        };
        Ok(LogPage {
            records,
            next_cursor,
            ..Default::default()
        })
    }

    fn filter_key(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
            &self.actor_name,
            &self.actor_id,
            &self.request_id,
            &self.level,
            &self.search,
            self.from_ms,
            self.to_ms,
        ))?)
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogPage {
    pub records: Vec<AttributedLog>,
    pub next_cursor: Option<String>,
    pub dropped: u64,
    pub persistence_failed: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    project: String,
    filters: String,
    pub time: u64,
    pub id: String,
}

fn page_size() -> usize {
    100
}
