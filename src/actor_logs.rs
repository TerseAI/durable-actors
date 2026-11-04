use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::actor::ActorKey;

pub(crate) mod sqlite;

pub(crate) const LOG_LINE_LIMIT: usize = 16 * 1024;
pub(crate) const LOG_BATCH_LINES: usize = 256;
const DEFAULT_PAGE_LINES: usize = 200;
const MAX_PAGE_LINES: usize = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LogStream {
    Stdout,
    Stderr,
    System,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogLine {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub stream: LogStream,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogEvent {
    pub project_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: String,
    pub session_id: String,
    #[serde(flatten)]
    pub line: LogLine,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogPage {
    pub lines: Vec<LogEvent>,
    pub before: Option<String>,
}

#[async_trait]
pub(crate) trait LogPersistence: Send + Sync {
    async fn append(&self, events: Vec<LogEvent>) -> Result<()>;
    async fn page(&self, filter: &LogFilter) -> Result<LogPage>;
}

#[derive(Clone)]
pub(crate) struct LogStore {
    persistence: Arc<dyn LogPersistence>,
}

impl LogStore {
    pub(crate) fn new(persistence: Arc<dyn LogPersistence>) -> Self {
        Self { persistence }
    }

    pub(crate) async fn record(
        &self,
        actor: &ActorKey,
        host_id: &str,
        session_id: &str,
        lines: Vec<LogLine>,
    ) -> Result<()> {
        ensure!(lines.len() <= LOG_BATCH_LINES, "log batch too large");
        ensure!(
            lines
                .iter()
                .all(|line| line.message.len() <= LOG_LINE_LIMIT),
            "log line too large"
        );
        let events = lines
            .into_iter()
            .map(|line| LogEvent {
                project_id: actor.project_id.clone(),
                actor_name: actor.actor_name.clone(),
                actor_id: actor.actor_id.clone(),
                host_id: host_id.into(),
                session_id: session_id.into(),
                line,
            })
            .collect();
        self.persistence.append(events).await
    }

    pub(crate) async fn page(&self, filter: &LogFilter) -> Result<LogPage> {
        self.persistence.page(filter).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LogQuery {
    pub actor_name: String,
    pub actor_id: String,
    pub session_id: Option<String>,
    pub before: Option<String>,
    pub limit: Option<usize>,
}

impl LogQuery {
    pub(crate) fn filter(self, project_id: &str) -> Result<LogFilter> {
        let actor = ActorKey {
            project_id: project_id.into(),
            actor_name: self.actor_name,
            actor_id: self.actor_id,
        };
        actor.validate()?;
        let limit = self.limit.unwrap_or(DEFAULT_PAGE_LINES);
        ensure!(
            (1..=MAX_PAGE_LINES).contains(&limit),
            "limit must be between 1 and {MAX_PAGE_LINES}"
        );
        let before = self.before.as_deref().map(LogPosition::parse).transpose()?;
        Ok(LogFilter {
            actor,
            session_id: self.session_id,
            before,
            limit,
        })
    }
}

#[derive(Clone)]
pub(crate) struct LogFilter {
    actor: ActorKey,
    session_id: Option<String>,
    before: Option<LogPosition>,
    limit: usize,
}

#[derive(Clone, Debug)]
struct LogPosition {
    timestamp_ms: u64,
    session_id: String,
    sequence: u64,
}

impl LogPosition {
    fn of(event: &LogEvent) -> Self {
        Self {
            timestamp_ms: event.line.timestamp_ms,
            session_id: event.session_id.clone(),
            sequence: event.line.sequence,
        }
    }

    fn parse(cursor: &str) -> Result<Self> {
        let mut parts = cursor.splitn(3, '.');
        let (Some(timestamp), Some(sequence), Some(session_id)) =
            (parts.next(), parts.next(), parts.next())
        else {
            anyhow::bail!("invalid log cursor");
        };
        Ok(Self {
            timestamp_ms: timestamp.parse().context("invalid log cursor")?,
            session_id: session_id.into(),
            sequence: sequence.parse().context("invalid log cursor")?,
        })
    }
}

impl std::fmt::Display for LogPosition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}.{}.{}",
            self.timestamp_ms, self.sequence, self.session_id
        )
    }
}

#[cfg(test)]
#[path = "../tests/unit/actor_logs.rs"]
mod tests;
