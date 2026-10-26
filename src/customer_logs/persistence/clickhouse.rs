use std::time::Duration;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

use super::{LogPersistence, LogReport};
use crate::customer_logs::{AttributedLog, LogPage, LogQuery};
use crate::request_traces::persistence::clickhouse::ClickHouseConfig;

pub(crate) struct ClickHouseLogPersistence {
    client: Client,
    retention: Duration,
}

impl ClickHouseLogPersistence {
    pub(crate) fn new(config: ClickHouseConfig, retention: Duration) -> Result<Self> {
        let url = reqwest::Url::parse(&config.url)?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "invalid ClickHouse URL"
        );
        let client = Client::default()
            .with_url(config.url)
            .with_database(config.database)
            .with_user(config.user)
            .with_password(config.password)
            .with_setting("max_execution_time", "10");
        Ok(Self { client, retention })
    }
}

#[async_trait]
impl LogPersistence for ClickHouseLogPersistence {
    async fn initialize(&self) -> Result<()> {
        let schema = include_str!("clickhouse.sql")
            .replace("RETENTION_SECONDS", &self.retention.as_secs().to_string());
        for statement in schema
            .split(';')
            .filter(|statement| !statement.trim().is_empty())
        {
            self.client.query(statement).execute().await?;
        }
        Ok(())
    }

    async fn append(&self, report: &LogReport) -> Result<()> {
        if !report.records.is_empty() {
            let mut insert = self.client.insert::<LogRow>("actor_logs").await?;
            for record in &report.records {
                insert
                    .write(&LogRow::new(record, report.received_at_ms)?)
                    .await?;
            }
            insert.end().await?;
        }
        if report.dropped > 0 {
            let mut insert = self.client.insert::<StatusRow>("actor_log_status").await?;
            insert
                .write(&StatusRow {
                    project_id: report.project.clone(),
                    report_id: report.id.clone(),
                    dropped: report.dropped,
                    received_at_ms: report.received_at_ms,
                })
                .await?;
            insert.end().await?;
        }
        Ok(())
    }

    async fn history(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        let after = query.after(project)?;
        let rows = self
            .client
            .query(include_str!("clickhouse_history.sql"))
            .param("project", project)
            .param("retention", self.retention.as_secs())
            .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
            .param("actor_id", query.actor_id.as_deref().unwrap_or(""))
            .param("request_id", query.request_id.as_deref().unwrap_or(""))
            .param("level", query.level.as_deref().unwrap_or(""))
            .param("search", query.search.as_deref().unwrap_or(""))
            .param("from", query.from_ms.unwrap_or(0))
            .param("to", query.to_ms.unwrap_or(u64::MAX))
            .param("has_cursor", after.is_some())
            .param("after_time", after.as_ref().map_or(0, |cursor| cursor.time))
            .param(
                "after_id",
                after.as_ref().map_or("", |cursor| cursor.id.as_str()),
            )
            .param("limit", query.limit + 1)
            .fetch_all::<JsonLog>()
            .await?;
        let records = rows
            .into_iter()
            .map(|row| serde_json::from_str(&row.event_json).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        let mut page = query.finish(project, records)?;
        let status = self.client.query("SELECT sum(dropped) AS dropped FROM actor_log_status FINAL WHERE project_id={project:String} AND received_at_ms>=toUnixTimestamp64Milli(now64())-{retention:UInt64}*1000")
            .param("project", project).param("retention", self.retention.as_secs()).fetch_one::<StatusTotal>().await?;
        page.dropped = status.dropped;
        Ok(page)
    }
}

#[derive(Row, Serialize)]
struct LogRow {
    project_id: String,
    event_id: String,
    timestamp_ms: u64,
    actor_name: String,
    actor_id: String,
    host_id: String,
    session_id: String,
    request_id: Option<String>,
    stream: String,
    level: String,
    message: String,
    fields_json: String,
    event_json: String,
    received_at_ms: u64,
}

impl LogRow {
    fn new(record: &AttributedLog, received_at_ms: u64) -> Result<Self> {
        Ok(Self {
            project_id: record.project_id.clone(),
            event_id: record.log.event_id.clone(),
            timestamp_ms: record.log.timestamp_ms,
            actor_name: record.actor_name.clone(),
            actor_id: record.actor_id.clone(),
            host_id: record.host_id.clone(),
            session_id: record.session_id.clone(),
            request_id: record.log.request_id.clone(),
            stream: match record.log.stream {
                super::super::LogStream::Stdout => "stdout",
                super::super::LogStream::Stderr => "stderr",
            }
            .into(),
            level: record.log.level.clone(),
            message: record.log.message.clone(),
            fields_json: serde_json::to_string(&record.log.fields)?,
            event_json: serde_json::to_string(record)?,
            received_at_ms,
        })
    }
}

#[derive(Row, Serialize)]
struct StatusRow {
    project_id: String,
    report_id: String,
    dropped: u64,
    received_at_ms: u64,
}
#[derive(Row, Deserialize)]
struct StatusTotal {
    dropped: u64,
}
#[derive(Row, Deserialize)]
struct JsonLog {
    event_json: String,
}
