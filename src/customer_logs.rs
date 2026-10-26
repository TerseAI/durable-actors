pub(crate) mod live;
pub(crate) mod persistence;
mod query;
mod store;

pub(crate) use query::{LogPage, LogQuery};
pub(crate) use store::LogStore;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub(crate) const LOG_BATCH_SIZE: usize = 64;
pub(crate) const MAX_LOG_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CustomerLog {
    pub event_id: String,
    pub timestamp_ms: u64,
    pub stream: LogStream,
    pub level: String,
    pub message: String,
    pub fields: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl CustomerLog {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.event_id.is_empty() && self.event_id.len() <= 128,
            "invalid log event ID"
        );
        ensure!(
            self.timestamp_ms <= 8_640_000_000_000_000,
            "invalid log timestamp"
        );
        ensure!(
            matches!(
                self.level.as_str(),
                "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL"
            ),
            "invalid log level"
        );
        ensure!(
            self.message.len() <= MAX_LOG_BYTES,
            "log message exceeds limit"
        );
        ensure!(
            self.fields.is_object() && serde_json::to_vec(&self.fields)?.len() <= MAX_LOG_BYTES,
            "log fields exceed limit or are not an object"
        );
        ensure!(
            self.request_id
                .as_ref()
                .is_none_or(|id| !id.is_empty() && id.len() <= 256),
            "invalid log request ID"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttributedLog {
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: String,
    pub session_id: String,
    #[serde(flatten)]
    pub log: CustomerLog,
}

#[cfg(test)]
#[path = "../tests/unit/customer_logs.rs"]
mod tests;
