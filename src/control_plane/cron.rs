mod postgres;
mod scheduler;
mod sqlite;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::actor::ActorKey;

pub(super) use postgres::PostgresCronStore;
#[cfg(test)]
pub(super) use scheduler::CronDispatcher;
pub(super) use scheduler::{CronScheduler, HttpCronDispatcher};
pub(super) use sqlite::SqliteCronStore;

const LEASE_MS: i64 = 60_000;

#[async_trait]
pub(crate) trait CronStore: Send + Sync {
    async fn register(&self, actor: &ActorKey, region: &str, now: i64) -> Result<()>;
    async fn publish(&self, project: &str, definitions: &[CronDefinition], now: i64) -> Result<()>;
    async fn remove_project(&self, project: &str) -> Result<()>;
    async fn projects(&self) -> Result<Vec<String>>;
    async fn claim_reconciliation(&self, now: i64) -> Result<bool>;
    async fn claim(&self, now: i64, limit: i64) -> Result<Vec<Occurrence>>;
    async fn renew(&self, occurrence: &Occurrence, now: i64) -> Result<bool>;
    async fn finish(&self, occurrence: &Occurrence, now: i64, outcome: CronOutcome)
    -> Result<bool>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CronDefinition {
    pub actor_name: String,
    pub method: String,
    pub expression: String,
    pub retries: i32,
}

impl CronDefinition {
    fn same_schedule(&self, other: &Self) -> bool {
        self.actor_name == other.actor_name
            && self.method == other.method
            && self.expression == other.expression
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CronSchedule {
    pub method: String,
    pub expression: String,
    #[serde(default)]
    pub retries: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CronOutcome {
    Completed,
    HandlerFailed,
    DeliveryFailed,
}

#[derive(Clone, Debug)]
pub(crate) struct Occurrence {
    pub id: String,
    pub actor: ActorKey,
    pub region: String,
    pub method: String,
    pub expression: String,
    pub scheduled_time: i64,
    pub attempt: i32,
    pub failures: i32,
    pub retries: i32,
    pub token: String,
}

impl Occurrence {
    pub(super) fn request_id(&self) -> String {
        format!("cron.{}.{}", self.id, self.scheduled_time)
    }

    fn completion(&self, now: i64, outcome: CronOutcome) -> Result<(i64, i64, i32, i32)> {
        if outcome == CronOutcome::Completed
            || (outcome == CronOutcome::HandlerFailed && self.failures >= self.retries)
        {
            let next = next_after(&self.expression, self.scheduled_time)?;
            return Ok((next, next, 0, 0));
        }
        let delay = (1_000i64 << (self.attempt - 1).clamp(0, 9)).min(300_000);
        let failures = self.failures + i32::from(outcome == CronOutcome::HandlerFailed);
        Ok((self.scheduled_time, now + delay, self.attempt, failures))
    }
}

pub(super) fn next_after(expression: &str, after: i64) -> Result<i64> {
    ensure!(expression.len() <= 1024, "cron expression is too long");
    let cron = expression
        .parse::<saffron::Cron>()
        .map_err(|error| anyhow::anyhow!("invalid cron expression {expression:?}: {error}"))?;
    ensure!(cron.any(), "cron expression never occurs: {expression:?}");
    let time =
        chrono::DateTime::from_timestamp_millis(after).context("cron timestamp out of range")?;
    Ok(cron
        .next_after(time)
        .context("cron has no future occurrence")?
        .timestamp_millis())
}

const MISSING_SCHEDULES: &str = "
    SELECT i.actor_name, i.actor_id, i.region, d.method, d.expression, i.created_at, d.created_at
    FROM durable_actors_cron_instances i
    JOIN durable_actors_cron_definitions d USING (project_id, actor_name)
    LEFT JOIN durable_actors_crons c USING (project_id, actor_name, actor_id, method, expression)
    WHERE i.project_id = $1 AND c.id IS NULL
        AND (i.actor_name=$2 OR $2 IS NULL) AND (i.actor_id=$3 OR $3 IS NULL)";

const INSERT_SCHEDULE: &str = "
    INSERT INTO durable_actors_crons
        (id, project_id, actor_name, actor_id, region, method, expression, next_at, available_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8) ON CONFLICT DO NOTHING";

const REGISTER: &str = "
    INSERT INTO durable_actors_cron_instances (project_id, actor_name, actor_id, region, created_at)
    VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING";

const INSERT_DEFINITION: &str = "
    INSERT INTO durable_actors_cron_definitions (project_id, actor_name, method, expression, created_at, retries)
    VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (project_id, actor_name, method, expression)
    DO UPDATE SET retries=EXCLUDED.retries
    WHERE durable_actors_cron_definitions.retries<>EXCLUDED.retries";

const RENEW: &str = "UPDATE durable_actors_crons SET lease_until=$1
    WHERE id=$2 AND token=$3 AND lease_until>$4";

const FINISH: &str = "UPDATE durable_actors_crons
    SET next_at=$1, available_at=$2, attempts=$3, failures=$4, token=NULL, lease_until=0
    WHERE id=$5 AND token=$6 AND lease_until>$7";

const CLAIM_RECONCILIATION: &str = "UPDATE durable_actors_cron_reconciliation
    SET available_at=$1 WHERE id=1 AND available_at<=$2";

const PROJECTS: &str = "SELECT project_id FROM durable_actors_cron_instances
    UNION SELECT project_id FROM durable_actors_cron_definitions";

#[cfg(test)]
#[path = "../../tests/unit/control_plane/cron.rs"]
mod tests;
