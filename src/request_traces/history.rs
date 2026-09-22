use anyhow::{Result, ensure};
use serde::Deserialize;

use super::{RequestOutcome, TRACE_CAPACITY};

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoryQuery {
    pub actor_name: Option<String>,
    pub actor_id: Option<String>,
    pub outcome: Option<RequestOutcome>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    #[serde(default = "page_size")]
    pub limit: usize,
    pub cursor: Option<String>,
}

impl Default for HistoryQuery {
    fn default() -> Self {
        Self {
            actor_name: None,
            actor_id: None,
            outcome: None,
            from_ms: None,
            to_ms: None,
            limit: page_size(),
            cursor: None,
        }
    }
}

impl HistoryQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=TRACE_CAPACITY).contains(&self.limit),
            "limit must be between 1 and 500"
        );
        ensure!(
            self.cursor
                .as_ref()
                .is_none_or(|c| !c.is_empty() && c.len() <= 4096),
            "invalid history cursor"
        );
        ensure!(
            self.from_ms
                .into_iter()
                .chain(self.to_ms)
                .all(|ms| ms <= 9_007_199_254_740_991),
            "invalid timestamp"
        );
        ensure!(
            self.from_ms
                .zip(self.to_ms)
                .is_none_or(|(from, to)| from <= to),
            "fromMs must not exceed toMs"
        );
        crate::actor::ActorKey {
            project_id: "history".into(),
            actor_name: self.actor_name.clone().unwrap_or_else(|| "Actor".into()),
            actor_id: self.actor_id.clone().unwrap_or_else(|| "actor".into()),
        }
        .validate()
    }

    pub(super) fn filter_key(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
            &self.actor_name,
            &self.actor_id,
            self.outcome,
            self.from_ms,
            self.to_ms,
        ))?)
    }
}

fn page_size() -> usize {
    100
}
