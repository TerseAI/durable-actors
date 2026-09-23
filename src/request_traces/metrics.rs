use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TimeRange {
    pub project_id: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
}

impl TimeRange {
    pub(crate) fn history(&self) -> super::history::HistoryQuery {
        super::history::HistoryQuery {
            project_id: self.project_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            ..Default::default()
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        super::history::HistoryQuery {
            project_id: self.project_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            ..Default::default()
        }
        .validate()
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueueWaitQuery {
    pub project_id: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    pub actor_name: Option<String>,
}

impl QueueWaitQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        super::history::HistoryQuery {
            project_id: self.project_id.clone(),
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            actor_name: self.actor_name.clone(),
            ..Default::default()
        }
        .validate()
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClassMetrics {
    pub actor_name: String,
    pub count: u64,
    pub success: Option<f64>,
    pub p95: Option<f64>,
    pub queue_p95: Option<f64>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OverviewMetrics {
    pub total: ClassMetrics,
    pub classes: Vec<ClassMetrics>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueueWaitRow {
    pub actor_name: String,
    pub actor_id: String,
    pub admitted: u64,
    pub average_ms: f64,
    pub max_ms: f64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SocketSession {
    pub connection_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub host_id: Option<String>,
    pub opened_at_ms: Option<u64>,
    pub closed_at_ms: Option<u64>,
    pub last_seen_ms: Option<u64>,
    pub messages: u64,
    pub failures: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}
