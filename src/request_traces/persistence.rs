use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

use super::{
    TracePage,
    history::HistoryQuery,
    logs::{LogHistogram, LogHistogramQuery, LogPage, LogQuery},
    metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, TimeRange},
    replay::ReplayQuery,
    sockets::SocketRecord,
};

pub(crate) mod clickhouse;
mod cursor;
pub(crate) mod sqlite;

// TraceStore validates project IDs and query bounds before calling the backends.
#[async_trait]
pub(crate) trait TraceQueries: Send + Sync {
    async fn history(&self, project_id: &str, query: &HistoryQuery) -> Result<TracePage>;
    async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage>;
    async fn logs(&self, project_id: &str, query: &LogQuery) -> Result<LogPage>;
    async fn log_histogram(
        &self,
        project_id: &str,
        query: &LogHistogramQuery,
    ) -> Result<LogHistogram>;
    async fn metrics(&self, project_id: &str, query: &TimeRange) -> Result<OverviewMetrics>;
    async fn queue_waits(
        &self,
        project_id: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>>;
    /// The project's connections opened by `to_ms`, newest first; with `open_only`, only those
    /// without a recorded close.
    async fn socket_records(
        &self,
        project_id: &str,
        to_ms: u64,
        open_only: bool,
        limit: usize,
    ) -> Result<Vec<SocketRecord>>;
    /// Each gateway's latest heartbeat.
    async fn gateway_heartbeats(&self) -> Result<HashMap<String, u64>>;
}

/// Answers every telemetry query with nothing, for deployments without an analytics backend.
pub(crate) struct NoTraceQueries;

#[async_trait]
impl TraceQueries for NoTraceQueries {
    async fn history(&self, _: &str, query: &HistoryQuery) -> Result<TracePage> {
        Ok(TracePage::empty(query.limit))
    }

    async fn replay(&self, _: &str, query: &ReplayQuery) -> Result<TracePage> {
        Ok(TracePage {
            snapshot: true,
            ..TracePage::empty(query.limit)
        })
    }

    async fn logs(&self, _: &str, _: &LogQuery) -> Result<LogPage> {
        LogPage::new(Vec::new(), 0)
    }

    async fn log_histogram(&self, _: &str, query: &LogHistogramQuery) -> Result<LogHistogram> {
        Ok(LogHistogram::empty(query))
    }

    async fn metrics(&self, _: &str, _: &TimeRange) -> Result<OverviewMetrics> {
        Ok(OverviewMetrics::default())
    }

    async fn queue_waits(&self, _: &str, _: &QueueWaitQuery) -> Result<Vec<QueueWaitRow>> {
        Ok(Vec::new())
    }

    async fn socket_records(
        &self,
        _: &str,
        _: u64,
        _: bool,
        _: usize,
    ) -> Result<Vec<SocketRecord>> {
        Ok(Vec::new())
    }

    async fn gateway_heartbeats(&self) -> Result<HashMap<String, u64>> {
        Ok(HashMap::new())
    }
}
