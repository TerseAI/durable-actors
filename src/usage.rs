use crate::postgres::PostgresDatabase;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub(crate) mod worker;
pub use worker::HttpUsageSink;
mod pubsub;
pub use pubsub::PubSubUsageSink;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageAssignment {
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_account_id: Option<String>,
    pub session_id: String,
    pub resource_id: String,
    pub region: String,
    pub cpu_millis: u32,
    pub memory_mib: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageInterval {
    pub id: String,
    #[serde(flatten)]
    pub assignment: UsageAssignment,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[async_trait]
pub trait UsageSink: Send + Sync {
    async fn deliver(&self, events: &[UsageInterval]) -> Result<()>;
}

#[async_trait]
pub trait UsageAuthorizer: Send + Sync {
    async fn authorize(&self, project_id: &str, billing_account_id: Option<&str>) -> Result<bool>;
}

#[derive(Clone)]
pub(crate) struct UsageJournal(PostgresDatabase);

impl UsageJournal {
    pub(crate) fn new(database: PostgresDatabase) -> Self {
        Self(database)
    }

    #[cfg(test)]
    async fn start(&self, assignment: &UsageAssignment, now: i64) -> Result<()> {
        let mut connection = self.0.connection().await?;
        let transaction = connection.transaction().await?;
        Self::start_in(&transaction, assignment, now).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn start_in(
        transaction: &tokio_postgres::Transaction<'_>,
        assignment: &UsageAssignment,
        now: i64,
    ) -> Result<()> {
        ensure!(
            assignment.cpu_millis > 0 && assignment.memory_mib > 0 && now >= 0,
            "invalid usage assignment"
        );
        let value = serde_json::to_value(assignment)?;
        transaction.execute("INSERT INTO durable_actors_usage_sessions (session_id, assignment, checkpoint_ms, observed_ms) VALUES ($1,$2,$3,$3) ON CONFLICT DO NOTHING", &[&assignment.session_id, &value, &now]).await?;
        let row = transaction
            .query_one(
                "SELECT assignment FROM durable_actors_usage_sessions WHERE session_id=$1",
                &[&assignment.session_id],
            )
            .await?;
        ensure!(
            row.get::<_, serde_json::Value>(0) == value,
            "usage session identity conflict"
        );
        Ok(())
    }

    pub(crate) async fn active(&self) -> Result<Vec<UsageAssignment>> {
        self.0
            .connection()
            .await?
            .query(
                "SELECT assignment FROM durable_actors_usage_sessions WHERE NOT stopped",
                &[],
            )
            .await?
            .iter()
            .map(|row| serde_json::from_value(row.get(0)).map_err(Into::into))
            .collect()
    }

    pub(crate) async fn observe(
        &self,
        session: &str,
        observation: Observation,
        now: i64,
    ) -> Result<()> {
        let mut connection = self.0.connection().await?;
        let transaction = connection.transaction().await?;
        let row = transaction.query_opt("SELECT GREATEST(observed_ms, checkpoint_ms) FROM durable_actors_usage_sessions WHERE session_id=$1 AND NOT stopped FOR UPDATE", &[&session]).await?;
        let Some(row) = row else {
            return Ok(());
        };
        let previous: i64 = row.get(0);
        let end = match observation {
            Observation::Running => now,
            Observation::Stopped(Some(end)) => end.min(now),
            Observation::Stopped(None) => previous,
        }
        .max(previous);
        transaction.execute("UPDATE durable_actors_usage_sessions SET observed_ms=$2, stopped=$3 WHERE session_id=$1", &[&session,&end,&matches!(observation, Observation::Stopped(_))]).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn pending(&self) -> Result<Vec<UsageInterval>> {
        let connection = self.0.connection().await?;
        // Preserve original event IDs while draining records created before checkpoint publishing.
        let mut events: Vec<UsageInterval> = connection.query("SELECT event FROM durable_actors_usage_outbox WHERE delivered_at IS NULL ORDER BY created_at, id LIMIT 100", &[]).await?.iter()
            .map(|row| serde_json::from_value(row.get(0))).collect::<std::result::Result<_, _>>()?;
        let rows = connection.query("SELECT assignment, checkpoint_ms, observed_ms, stopped FROM durable_actors_usage_sessions WHERE observed_ms > checkpoint_ms AND (stopped OR observed_ms / 10000 * 10000 > checkpoint_ms) ORDER BY checkpoint_ms, session_id LIMIT 100", &[]).await?;
        for row in rows {
            let assignment: UsageAssignment = serde_json::from_value(row.get(0))?;
            let mut start: i64 = row.get(1);
            let observed: i64 = row.get(2);
            let end = if row.get::<_, bool>(3) {
                observed
            } else {
                observed / 10_000 * 10_000
            };
            while start < end && events.len() < 100 {
                // Stable boundaries reproduce IDs if publication succeeds but checkpointing fails.
                let interval_end = end.min((start / 10_000 + 1) * 10_000);
                events.push(UsageInterval {
                    id: format!(
                        "sandbox_usage_v1:{}:{start}:{interval_end}",
                        assignment.session_id
                    ),
                    assignment: assignment.clone(),
                    start_ms: start,
                    end_ms: interval_end,
                });
                start = interval_end;
            }
        }
        Ok(events)
    }

    pub(crate) async fn ack(&self, events: &[UsageInterval]) -> Result<()> {
        let mut connection = self.0.connection().await?;
        let transaction = connection.transaction().await?;
        for event in events {
            transaction.execute("UPDATE durable_actors_usage_sessions SET checkpoint_ms=GREATEST(checkpoint_ms,$3) WHERE session_id=$1 AND checkpoint_ms >= $2 AND observed_ms >= $3", &[&event.assignment.session_id, &event.start_ms, &event.end_ms]).await?;
        }
        let ids: Vec<_> = events.iter().map(|event| event.id.clone()).collect();
        transaction.execute("UPDATE durable_actors_usage_outbox SET delivered_at=clock_timestamp() WHERE id=ANY($1) AND delivered_at IS NULL", &[&ids]).await?;
        transaction.commit().await?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Observation {
    Running,
    Stopped(Option<i64>),
}

#[cfg(test)]
#[path = "../tests/unit/usage/mod.rs"]
mod tests;
