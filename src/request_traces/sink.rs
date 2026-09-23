use super::{RequestTrace, TraceEvent, TraceStore};
use anyhow::Result;
use async_trait::async_trait;

pub(crate) struct TraceScope {
    pub project_id: String,
    pub region: String,
    pub host_id: String,
    pub session_id: String,
}

impl TraceScope {
    pub(crate) fn events(&self, traces: Vec<RequestTrace>) -> Vec<TraceEvent> {
        traces
            .into_iter()
            .map(|trace| TraceEvent {
                project_id: self.project_id.clone(),
                region: self.region.clone(),
                host_id: self.host_id.clone(),
                session_id: self.session_id.clone(),
                trace,
            })
            .collect()
    }
}

#[async_trait]
pub(crate) trait TraceSink: Send + Sync {
    async fn record(
        &self,
        scope: &TraceScope,
        traces: Vec<RequestTrace>,
        dropped: u64,
    ) -> Result<()>;
}

#[async_trait]
impl TraceSink for TraceStore {
    async fn record(
        &self,
        scope: &TraceScope,
        traces: Vec<RequestTrace>,
        dropped: u64,
    ) -> Result<()> {
        self.record(scope, traces, dropped).await
    }
}
