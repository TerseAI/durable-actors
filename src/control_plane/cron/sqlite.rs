use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use rusqlite::{Connection, Transaction, params};

use super::*;

pub(crate) struct SqliteCronStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteCronStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
        )?;
        let initialized: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='durable_actors_crons')", [], |row| row.get(0))?;
        if !initialized {
            connection.execute_batch(concat!(
                "BEGIN IMMEDIATE;",
                include_str!("../../../migrations/V16__crons.sql"),
                "COMMIT;"
            ))?;
        }
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| anyhow::anyhow!("cron database lock poisoned"))?;
            operation(&mut connection)
        })
        .await?
    }
}

#[async_trait]
impl CronStore for SqliteCronStore {
    async fn register(&self, actor: &ActorKey, region: &str, now: i64) -> Result<()> {
        let actor = actor.clone();
        let region = region.to_owned();
        self.run(move |connection| {
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let inserted = transaction.execute(
                REGISTER,
                params![
                    actor.project_id,
                    actor.actor_name,
                    actor.actor_id,
                    region,
                    now
                ],
            )?;
            if inserted > 0 {
                populate_schedules(&transaction, &actor.project_id, Some(&actor))?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    async fn publish(&self, project: &str, definitions: &[CronDefinition], now: i64) -> Result<()> {
        let project = project.to_owned();
        let definitions = definitions.to_vec();
        self.run(move |connection| {
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if publish_definitions(&transaction, &project, &definitions, now)? {
                populate_schedules(&transaction, &project, None)?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    async fn remove_project(&self, project: &str) -> Result<()> {
        let project = project.to_owned();
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "DELETE FROM durable_actors_cron_definitions WHERE project_id=$1",
                [&project],
            )?;
            transaction.execute(
                "DELETE FROM durable_actors_cron_instances WHERE project_id=$1",
                [&project],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    async fn projects(&self) -> Result<Vec<String>> {
        self.run(|connection| {
            Ok(connection
                .prepare(PROJECTS)?
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?)
        })
        .await
    }

    async fn claim_reconciliation(&self, now: i64) -> Result<bool> {
        self.run(move |connection| {
            Ok(connection.execute(CLAIM_RECONCILIATION, params![now + 60_000, now])? > 0)
        })
        .await
    }

    async fn claim(&self, now: i64, limit: i64) -> Result<Vec<Occurrence>> {
        self.run(move |connection| {
            let transaction = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let occurrences = transaction.prepare("SELECT c.id, c.project_id, c.actor_name, c.actor_id, c.region, c.method, c.expression, c.next_at, c.attempts, c.failures, d.retries
                FROM durable_actors_crons c JOIN durable_actors_cron_definitions d USING (project_id, actor_name, method, expression)
                WHERE c.available_at<=$1 AND c.lease_until<=$1 ORDER BY c.available_at, c.id LIMIT $2")?
                .query_map(params![now, limit], occurrence)?.collect::<rusqlite::Result<Vec<_>>>()?;
            for occurrence in &occurrences {
                transaction.execute("UPDATE durable_actors_crons SET token=$1, lease_until=$2, attempts=$3 WHERE id=$4",
                    params![occurrence.token, now + LEASE_MS, occurrence.attempt, occurrence.id])?;
            }
            transaction.commit()?;
            Ok(occurrences)
        }).await
    }

    async fn renew(&self, occurrence: &Occurrence, now: i64) -> Result<bool> {
        let occurrence = occurrence.clone();
        self.run(move |connection| {
            Ok(connection.execute(
                RENEW,
                params![now + LEASE_MS, occurrence.id, occurrence.token, now],
            )? > 0)
        })
        .await
    }

    async fn finish(
        &self,
        occurrence: &Occurrence,
        now: i64,
        outcome: CronOutcome,
    ) -> Result<bool> {
        let (next, available, attempts, failures) = occurrence.completion(now, outcome)?;
        let occurrence = occurrence.clone();
        self.run(move |connection| {
            Ok(connection.execute(
                FINISH,
                params![
                    next,
                    available,
                    attempts,
                    failures,
                    occurrence.id,
                    occurrence.token,
                    now
                ],
            )? > 0)
        })
        .await
    }
}

fn publish_definitions(
    transaction: &Transaction<'_>,
    project: &str,
    definitions: &[CronDefinition],
    now: i64,
) -> Result<bool> {
    let existing = transaction.prepare("SELECT actor_name, method, expression, retries FROM durable_actors_cron_definitions WHERE project_id=$1")?
        .query_map([project], |row| Ok(CronDefinition { actor_name: row.get(0)?, method: row.get(1)?, expression: row.get(2)?, retries: row.get(3)? }))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for definition in existing
        .iter()
        .filter(|old| !definitions.iter().any(|new| old.same_schedule(new)))
    {
        transaction.execute("DELETE FROM durable_actors_cron_definitions WHERE project_id=$1 AND actor_name=$2 AND method=$3 AND expression=$4",
            params![project, definition.actor_name, definition.method, definition.expression])?;
    }
    for definition in definitions {
        next_after(&definition.expression, now)?;
        ensure!(definition.retries >= 0, "cron retries must be nonnegative");
        transaction.execute(
            INSERT_DEFINITION,
            params![
                project,
                definition.actor_name,
                definition.method,
                definition.expression,
                now,
                definition.retries
            ],
        )?;
    }
    Ok(definitions
        .iter()
        .any(|new| !existing.iter().any(|old| old.same_schedule(new))))
}

fn populate_schedules(
    transaction: &Transaction<'_>,
    project: &str,
    actor: Option<&ActorKey>,
) -> Result<()> {
    let mut statement = transaction.prepare(MISSING_SCHEDULES)?;
    let mut rows = statement.query(params![
        project,
        actor.map(|actor| actor.actor_name.as_str()),
        actor.map(|actor| actor.actor_id.as_str())
    ])?;
    while let Some(row) = rows.next()? {
        let actor: String = row.get(0)?;
        let id: String = row.get(1)?;
        let region: String = row.get(2)?;
        let method: String = row.get(3)?;
        let expression: String = row.get(4)?;
        let created = row.get::<_, i64>(5)?.max(row.get(6)?);
        let next = next_after(&expression, created)?;
        transaction.execute(
            INSERT_SCHEDULE,
            params![
                uuid::Uuid::new_v4().to_string(),
                project,
                actor,
                id,
                region,
                method,
                expression,
                next
            ],
        )?;
    }
    Ok(())
}

fn occurrence(row: &rusqlite::Row<'_>) -> rusqlite::Result<Occurrence> {
    Ok(Occurrence {
        id: row.get(0)?,
        actor: ActorKey {
            project_id: row.get(1)?,
            actor_name: row.get(2)?,
            actor_id: row.get(3)?,
        },
        region: row.get(4)?,
        method: row.get(5)?,
        expression: row.get(6)?,
        scheduled_time: row.get(7)?,
        attempt: row.get::<_, i32>(8)?.saturating_add(1),
        failures: row.get(9)?,
        retries: row.get(10)?,
        token: uuid::Uuid::new_v4().to_string(),
    })
}
