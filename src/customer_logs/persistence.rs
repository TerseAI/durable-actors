use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{AttributedLog, LogPage, LogQuery};

pub(crate) mod clickhouse;
pub(crate) mod postgres;
pub(crate) mod sqlite;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct LogReport {
    pub id: String,
    pub project: String,
    pub records: Vec<AttributedLog>,
    pub dropped: u64,
    pub received_at_ms: u64,
}

#[async_trait]
pub(crate) trait LogPersistence: Send + Sync {
    async fn initialize(&self) -> Result<()>;
    async fn append(&self, report: &LogReport) -> Result<()>;
    async fn history(&self, project: &str, query: &LogQuery) -> Result<LogPage>;
}
