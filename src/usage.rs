use crate::postgres::PostgresDatabase;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub(crate) mod worker;
pub use worker::HttpUsageSink;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageAssignment {
    pub project_id: String,
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
    async fn authorize(&self, project_id: &str) -> Result<bool>;
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
        transaction.execute("INSERT INTO durable_actors_usage_sessions (session_id, assignment, checkpoint_ms) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", &[&assignment.session_id, &value, &now]).await?;
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
        let row = transaction.query_opt("SELECT assignment, checkpoint_ms FROM durable_actors_usage_sessions WHERE session_id=$1 AND NOT stopped FOR UPDATE", &[&session]).await?;
        let Some(row) = row else {
            return Ok(());
        };
        let assignment: UsageAssignment = serde_json::from_value(row.get(0))?;
        let start: i64 = row.get(1);
        let end = match observation {
            Observation::Running => now,
            Observation::Stopped(Some(end)) => end.min(now),
            Observation::Stopped(None) => start,
        }
        .max(start);
        let mut cursor = start;
        while end > cursor {
            let interval_end = end.min((cursor / 3_600_000 + 1) * 3_600_000);
            let event = UsageInterval {
                id: format!("sandbox_usage_v1:{session}:{cursor}:{interval_end}"),
                assignment: assignment.clone(),
                start_ms: cursor,
                end_ms: interval_end,
            };
            transaction
                .execute(
                    "INSERT INTO durable_actors_usage_outbox (id,event) VALUES ($1,$2)",
                    &[&event.id, &serde_json::to_value(&event)?],
                )
                .await?;
            cursor = interval_end;
        }
        transaction.execute("UPDATE durable_actors_usage_sessions SET checkpoint_ms=$2, stopped=$3 WHERE session_id=$1", &[&session,&end,&matches!(observation, Observation::Stopped(_))]).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn pending(&self) -> Result<Vec<UsageInterval>> {
        self.0.connection().await?.query("SELECT event FROM durable_actors_usage_outbox WHERE delivered_at IS NULL ORDER BY created_at, id LIMIT 100", &[]).await?.iter()
            .map(|row| serde_json::from_value(row.get(0)).map_err(Into::into)).collect()
    }

    pub(crate) async fn ack(&self, ids: &[String]) -> Result<()> {
        self.0
            .execute(
                "UPDATE durable_actors_usage_outbox SET delivered_at=clock_timestamp() WHERE id=ANY($1)",
                &[&ids],
            )
            .await?;
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
