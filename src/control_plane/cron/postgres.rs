use crate::postgres::PostgresDatabase;
use tokio_postgres::Transaction;

use super::*;

pub(crate) struct PostgresCronStore {
    database: PostgresDatabase,
}

impl PostgresCronStore {
    pub(crate) fn new(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl CronStore for PostgresCronStore {
    async fn register(&self, actor: &ActorKey, region: &str, now: i64) -> Result<()> {
        let mut connection = self.database.connection().await?;
        let transaction = connection.transaction().await?;
        transaction.query_one(
            "SELECT pg_advisory_xact_lock_shared(hashtext('durable-actors-crons'), hashtext($1))",
            &[&actor.project_id],
        ).await?;
        let inserted = transaction
            .execute(
                REGISTER,
                &[
                    &actor.project_id,
                    &actor.actor_name,
                    &actor.actor_id,
                    &region,
                    &now,
                ],
            )
            .await?;
        if inserted > 0 {
            populate_schedules(&transaction, &actor.project_id, Some(actor)).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn publish(&self, project: &str, definitions: &[CronDefinition], now: i64) -> Result<()> {
        let mut connection = self.database.connection().await?;
        let transaction = connection.transaction().await?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtext('durable-actors-crons'), hashtext($1))",
                &[&project],
            )
            .await?;
        if publish_definitions(&transaction, project, definitions, now).await? {
            populate_schedules(&transaction, project, None).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn remove_project(&self, project: &str) -> Result<()> {
        let mut connection = self.database.connection().await?;
        let transaction = connection.transaction().await?;
        transaction
            .execute(
                "DELETE FROM durable_actors_cron_definitions WHERE project_id=$1",
                &[&project],
            )
            .await?;
        transaction
            .execute(
                "DELETE FROM durable_actors_cron_instances WHERE project_id=$1",
                &[&project],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn projects(&self) -> Result<Vec<String>> {
        Ok(self
            .database
            .connection()
            .await?
            .query(PROJECTS, &[])
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    async fn claim_reconciliation(&self, now: i64) -> Result<bool> {
        Ok(self
            .database
            .execute(CLAIM_RECONCILIATION, &[&(now + 60_000), &now])
            .await?
            > 0)
    }

    async fn claim(&self, now: i64, limit: i64) -> Result<Vec<Occurrence>> {
        let mut connection = self.database.connection().await?;
        let transaction = connection.transaction().await?;
        let rows = transaction.query("SELECT c.id, c.project_id, c.actor_name, c.actor_id, c.region, c.method, c.expression, c.next_at, c.attempts, c.failures, d.retries
            FROM durable_actors_crons c JOIN durable_actors_cron_definitions d USING (project_id, actor_name, method, expression)
            WHERE c.available_at<=$1 AND c.lease_until<=$1
            ORDER BY c.available_at, c.id LIMIT $2 FOR UPDATE OF c SKIP LOCKED", &[&now, &limit]).await?;
        let occurrences = rows.iter().map(occurrence).collect::<Vec<_>>();
        for occurrence in &occurrences {
            transaction.execute("UPDATE durable_actors_crons SET token=$1, lease_until=$2, attempts=$3 WHERE id=$4",
                &[&occurrence.token, &(now + LEASE_MS), &occurrence.attempt, &occurrence.id]).await?;
        }
        transaction.commit().await?;
        Ok(occurrences)
    }

    async fn renew(&self, occurrence: &Occurrence, now: i64) -> Result<bool> {
        Ok(self
            .database
            .execute(
                RENEW,
                &[&(now + LEASE_MS), &occurrence.id, &occurrence.token, &now],
            )
            .await?
            > 0)
    }

    async fn finish(
        &self,
        occurrence: &Occurrence,
        now: i64,
        outcome: CronOutcome,
    ) -> Result<bool> {
        let (next, available, attempts, failures) = occurrence.completion(now, outcome)?;
        Ok(self
            .database
            .execute(
                FINISH,
                &[
                    &next,
                    &available,
                    &attempts,
                    &failures,
                    &occurrence.id,
                    &occurrence.token,
                    &now,
                ],
            )
            .await?
            > 0)
    }
}

async fn publish_definitions(
    transaction: &Transaction<'_>,
    project: &str,
    definitions: &[CronDefinition],
    now: i64,
) -> Result<bool> {
    let rows = transaction.query("SELECT actor_name, method, expression, retries FROM durable_actors_cron_definitions WHERE project_id=$1", &[&project]).await?;
    let existing = rows
        .iter()
        .map(|row| CronDefinition {
            actor_name: row.get(0),
            method: row.get(1),
            expression: row.get(2),
            retries: row.get(3),
        })
        .collect::<Vec<_>>();
    for definition in existing
        .iter()
        .filter(|old| !definitions.iter().any(|new| old.same_schedule(new)))
    {
        transaction.execute("DELETE FROM durable_actors_cron_definitions WHERE project_id=$1 AND actor_name=$2 AND method=$3 AND expression=$4",
            &[&project, &definition.actor_name, &definition.method, &definition.expression]).await?;
    }
    for definition in definitions {
        next_after(&definition.expression, now)?;
        ensure!(definition.retries >= 0, "cron retries must be nonnegative");
        transaction
            .execute(
                INSERT_DEFINITION,
                &[
                    &project,
                    &definition.actor_name,
                    &definition.method,
                    &definition.expression,
                    &now,
                    &definition.retries,
                ],
            )
            .await?;
    }
    Ok(definitions
        .iter()
        .any(|new| !existing.iter().any(|old| old.same_schedule(new))))
}

async fn populate_schedules(
    transaction: &Transaction<'_>,
    project: &str,
    actor: Option<&ActorKey>,
) -> Result<()> {
    let name = actor.map(|actor| actor.actor_name.as_str());
    let id = actor.map(|actor| actor.actor_id.as_str());
    for row in transaction
        .query(MISSING_SCHEDULES, &[&project, &name, &id])
        .await?
    {
        let actor: String = row.get(0);
        let id: String = row.get(1);
        let region: String = row.get(2);
        let method: String = row.get(3);
        let expression: String = row.get(4);
        let created = row.get::<_, i64>(5).max(row.get(6));
        let next = next_after(&expression, created)?;
        transaction
            .execute(
                INSERT_SCHEDULE,
                &[
                    &uuid::Uuid::new_v4().to_string(),
                    &project,
                    &actor,
                    &id,
                    &region,
                    &method,
                    &expression,
                    &next,
                ],
            )
            .await?;
    }
    Ok(())
}

fn occurrence(row: &tokio_postgres::Row) -> Occurrence {
    Occurrence {
        id: row.get(0),
        actor: ActorKey {
            project_id: row.get(1),
            actor_name: row.get(2),
            actor_id: row.get(3),
        },
        region: row.get(4),
        method: row.get(5),
        expression: row.get(6),
        scheduled_time: row.get(7),
        attempt: row.get::<_, i32>(8).saturating_add(1),
        failures: row.get(9),
        retries: row.get(10),
        token: uuid::Uuid::new_v4().to_string(),
    }
}
