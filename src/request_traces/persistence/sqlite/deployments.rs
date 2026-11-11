use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::platform_events::deployments::DeploymentEvent;

/// Only each project's latest deployment, matching ClickHouse's `latest_deployments`.
pub(super) fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS deployments (
            project_id TEXT PRIMARY KEY,
            at_ms INTEGER NOT NULL,
            contract TEXT
        );",
    )?;
    Ok(())
}

pub(super) fn record(connection: &mut Connection, event: &DeploymentEvent) -> Result<()> {
    connection.execute(
        "INSERT INTO deployments (project_id, at_ms, contract) VALUES (?1, ?2, ?3)
         ON CONFLICT (project_id) DO UPDATE SET at_ms = excluded.at_ms, contract = excluded.contract
         WHERE excluded.at_ms >= deployments.at_ms",
        params![
            event.project_id(),
            i64::try_from(event.at_ms())?,
            event.contract().map(Value::to_string),
        ],
    )?;
    Ok(())
}

pub(super) fn contract(connection: &mut Connection, project_id: &str) -> Result<Option<Value>> {
    let contract: Option<Option<String>> = connection
        .query_row(
            "SELECT contract FROM deployments WHERE project_id = ?1",
            [project_id],
            |row| row.get(0),
        )
        .optional()?;
    contract
        .flatten()
        .map(|contract| Ok(serde_json::from_str(&contract)?))
        .transpose()
}
