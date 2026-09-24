use std::{collections::BTreeMap, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio_postgres::IsolationLevel;

use super::{
    HistoryQuery, OverviewMetrics, QueueWaitQuery, QueueWaitRow, ReplayQuery, SocketSession,
    TimeRange, TracePersistence, pagination::Metadata,
};
use crate::{
    postgres::PostgresDatabase,
    request_traces::{TraceEvent, TracePage, TraceRecord},
};

mod history;
mod metrics;
mod replay;
mod retention;

#[derive(Clone)]
pub(crate) struct PostgresTracePersistence {
    database: PostgresDatabase,
    retention: Duration,
}

impl PostgresTracePersistence {
    pub(crate) fn new(database: PostgresDatabase, retention: Duration) -> Self {
        Self {
            database,
            retention,
        }
    }
}

#[async_trait]
impl TracePersistence for PostgresTracePersistence {
    async fn initialize(&self) -> Result<()> {
        drop(self.database.connection().await?);
        Ok(())
    }

    async fn append(&self, events: &[TraceEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut projects: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for event in events {
            projects
                .entry(&event.trace.project_id)
                .or_default()
                .push(serde_json::to_string(event)?);
        }
        let mut client = self.database.connection().await?;
        let transaction = transaction(&mut client, false).await?;
        // Lock projects in one order; positions become visible in commit order.
        for (project, events) in projects {
            let head = lock_project(&transaction, project).await?;
            let row = transaction
                .query_one(
                    include_str!("postgres/append.sql"),
                    &[&project, &head, &events],
                )
                .await?;
            let inserted: i64 = row.get(0);
            let last: Option<i64> = row.get(1);
            transaction.execute(
                "UPDATE durable_actors_trace_projects SET head = COALESCE($2, head), total = total + $3, retained = retained + $3 WHERE project_id = $1",
                &[&project, &last, &inserted],
            ).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn history(&self, project_id: &str, query: &HistoryQuery) -> Result<TracePage> {
        validate_project(project_id)?;
        query.validate()?;
        history::query(&self.database, project_id, query).await
    }

    async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
        validate_project(project_id)?;
        query.validate()?;
        replay::query(&self.database, project_id, query).await
    }

    async fn metrics(&self, project_id: &str, query: &TimeRange) -> Result<OverviewMetrics> {
        validate_project(project_id)?;
        query.validate()?;
        metrics::overview(&self.database, project_id, query).await
    }

    async fn queue_waits(
        &self,
        project_id: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        validate_project(project_id)?;
        query.validate()?;
        metrics::queue_waits(&self.database, project_id, query).await
    }

    async fn websockets(&self, project_id: &str, query: &TimeRange) -> Result<Vec<SocketSession>> {
        validate_project(project_id)?;
        query.validate()?;
        metrics::websockets(&self.database, project_id, query).await
    }
}

fn validate_project(project_id: &str) -> Result<()> {
    crate::control_plane::admin::validate_component("project ID", project_id, 64)
}

async fn transaction(
    client: &mut deadpool_postgres::Object,
    read_only: bool,
) -> Result<deadpool_postgres::Transaction<'_>> {
    let transaction = client
        .build_transaction()
        .read_only(read_only)
        .isolation_level(if read_only {
            IsolationLevel::RepeatableRead
        } else {
            IsolationLevel::ReadCommitted
        })
        .start()
        .await?;
    transaction
        .batch_execute("SET LOCAL statement_timeout = '10s'; SET LOCAL lock_timeout = '2s'")
        .await?;
    Ok(transaction)
}

async fn lock_project(transaction: &tokio_postgres::Transaction<'_>, project: &str) -> Result<i64> {
    transaction.execute("INSERT INTO durable_actors_trace_projects (project_id) VALUES ($1) ON CONFLICT DO NOTHING", &[&project]).await?;
    Ok(transaction
        .query_one(
            "SELECT head FROM durable_actors_trace_projects WHERE project_id = $1 FOR UPDATE",
            &[&project],
        )
        .await?
        .get(0))
}

async fn metadata(
    transaction: &tokio_postgres::Transaction<'_>,
    project: &str,
) -> Result<(Metadata, u64)> {
    let row = transaction.query_one(
        "SELECT generation, COALESCE(head, 0), COALESCE(pruned, 0), COALESCE(total, 0), COALESCE(retained, 0) FROM durable_actors_trace_meta LEFT JOIN durable_actors_trace_projects ON project_id = $1",
        &[&project],
    ).await?;
    Ok((
        Metadata {
            generation: row.get(0),
            head: row.get::<_, i64>(1) as u64,
            pruned: row.get::<_, i64>(2) as u64,
            total: row.get::<_, i64>(3) as u64,
        },
        row.get::<_, i64>(4) as u64,
    ))
}

fn records(rows: Vec<tokio_postgres::Row>) -> Result<Vec<TraceRecord>> {
    rows.into_iter()
        .map(|row| {
            Ok(TraceRecord {
                sequence: row.get::<_, i64>(0) as u64,
                event: serde_json::from_str(row.get::<_, &str>(1))?,
            })
        })
        .collect()
}
