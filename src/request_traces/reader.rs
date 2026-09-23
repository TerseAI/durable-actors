use super::{TracePage, TraceStore, history::HistoryQuery, metrics::*, replay::ReplayQuery};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub(crate) trait TraceReader: Send + Sync {
    async fn history(&self, query: &HistoryQuery) -> Result<TracePage>;
    async fn metrics(&self, query: &TimeRange) -> Result<OverviewMetrics>;
    async fn queue_waits(&self, query: &QueueWaitQuery) -> Result<Vec<QueueWaitRow>>;
    async fn websockets(&self, query: &TimeRange) -> Result<Vec<SocketSession>>;
    async fn replay(&self, query: &ReplayQuery) -> Result<TracePage>;
}

#[async_trait]
impl TraceReader for TraceStore {
    async fn history(&self, query: &HistoryQuery) -> Result<TracePage> {
        self.history(query).await
    }
    async fn metrics(&self, query: &TimeRange) -> Result<OverviewMetrics> {
        self.metrics(query).await
    }
    async fn queue_waits(&self, query: &QueueWaitQuery) -> Result<Vec<QueueWaitRow>> {
        self.queue_waits(query).await
    }
    async fn websockets(&self, query: &TimeRange) -> Result<Vec<SocketSession>> {
        self.websockets(query).await
    }
    async fn replay(&self, query: &ReplayQuery) -> Result<TracePage> {
        self.replay(query).await
    }
}
