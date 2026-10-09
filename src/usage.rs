use crate::postgres::PostgresDatabase;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

mod pubsub;
pub(crate) mod worker;
pub(crate) use pubsub::PubSubUsageSink;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageAssignment {
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_account_id: Option<String>,
    pub session_id: String,
    pub resource_id: String,
    pub region: String,
    pub cpu_millis: u32,
    pub memory_mib: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum UsageEventType {
    #[serde(rename = "sandbox.started")]
    Started,
    #[serde(rename = "sandbox.stopped")]
    Stopped,
}

impl UsageEventType {
    fn id_component(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: UsageEventType,
    #[serde(flatten)]
    pub assignment: UsageAssignment,
    pub observed_at_ms: i64,
}

#[async_trait]
pub(crate) trait UsageSink: Send + Sync {
    async fn deliver(&self, events: &[UsageEvent]) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct UsageOutbox(PostgresDatabase);

impl UsageOutbox {
    pub(crate) fn new(database: PostgresDatabase) -> Self {
        Self(database)
    }

    pub(crate) async fn enqueue_in(
        transaction: &tokio_postgres::Transaction<'_>,
        assignment: &UsageAssignment,
        event_type: UsageEventType,
        observed_at_ms: i64,
    ) -> Result<()> {
        ensure!(
            assignment.cpu_millis > 0 && assignment.memory_mib > 0 && observed_at_ms >= 0,
            "invalid usage event"
        );
        let event = UsageEvent {
            id: format!(
                "sandbox_usage_v1:{}:{}",
                assignment.session_id,
                event_type.id_component()
            ),
            event_type,
            assignment: assignment.clone(),
            observed_at_ms,
        };
        let value = serde_json::to_value(&event)?;
        transaction
            .execute(
                "INSERT INTO durable_actors_usage_outbox (id, event) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                &[&event.id, &value],
            )
            .await?;
        let row = transaction
            .query_one(
                "SELECT event FROM durable_actors_usage_outbox WHERE id=$1",
                &[&event.id],
            )
            .await?;
        ensure!(
            row.get::<_, serde_json::Value>(0) == value,
            "usage event identity conflict"
        );
        Ok(())
    }

    pub(crate) async fn pending(&self) -> Result<Vec<UsageEvent>> {
        let connection = self.0.connection().await?;
        connection
            .query(
                "SELECT event FROM durable_actors_usage_outbox ORDER BY created_at, id LIMIT 100",
                &[],
            )
            .await?
            .iter()
            .map(|row| serde_json::from_value(row.get(0)).map_err(Into::into))
            .collect()
    }

    pub(crate) async fn ack(&self, events: &[UsageEvent]) -> Result<()> {
        let ids: Vec<_> = events.iter().map(|event| event.id.clone()).collect();
        self.0
            .execute(
                "DELETE FROM durable_actors_usage_outbox WHERE id=ANY($1)",
                &[&ids],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/usage/mod.rs"]
mod tests;
