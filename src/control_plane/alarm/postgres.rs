use super::*;
use crate::postgres::PostgresDatabase;

pub(crate) struct PostgresAlarmStore {
    database: PostgresDatabase,
}
impl PostgresAlarmStore {
    pub(crate) fn new(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl AlarmStore for PostgresAlarmStore {
    async fn register(&self, actor: &ActorKey, region: &str, alarm: &Alarm) -> Result<()> {
        alarm.validate()?;
        self.database
            .execute(
                REGISTER,
                &[
                    &alarm.generation,
                    &actor.project_id,
                    &actor.actor_name,
                    &actor.actor_id,
                    &region,
                    &alarm.deadline,
                ],
            )
            .await?;
        Ok(())
    }
    async fn claim(&self, now: i64, limit: i64) -> Result<Vec<Delivery>> {
        let mut connection = self.database.connection().await?;
        let transaction = connection.transaction().await?;
        let rows = transaction.query("SELECT generation,project_id,actor_name,actor_id,region,deadline,attempts FROM durable_actors_alarms WHERE available_at<=$1 AND lease_until<=$1 ORDER BY available_at LIMIT $2 FOR UPDATE SKIP LOCKED", &[&now,&limit]).await?;
        let mut deliveries = Vec::new();
        for row in rows {
            let delivery = Delivery {
                actor: ActorKey {
                    project_id: row.get(1),
                    actor_name: row.get(2),
                    actor_id: row.get(3),
                },
                region: row.get(4),
                alarm: Alarm {
                    generation: row.get(0),
                    deadline: row.get(5),
                },
                token: uuid::Uuid::new_v4().to_string(),
                attempt: row.get::<_, i32>(6).saturating_add(1),
            };
            transaction.execute("UPDATE durable_actors_alarms SET token=$1,lease_until=$2,attempts=$3 WHERE generation=$4", &[&delivery.token,&(now+LEASE_MS),&delivery.attempt,&delivery.alarm.generation]).await?;
            deliveries.push(delivery);
        }
        transaction.commit().await?;
        Ok(deliveries)
    }
    async fn renew(&self, delivery: &Delivery, now: i64) -> Result<bool> {
        Ok(self
            .database
            .execute(
                RENEW,
                &[
                    &(now + LEASE_MS),
                    &delivery.alarm.generation,
                    &delivery.token,
                    &now,
                ],
            )
            .await?
            > 0)
    }
    async fn finish(&self, delivery: &Delivery, now: i64, completed: bool) -> Result<bool> {
        let changed = if completed {
            self.database
                .execute(
                    COMPLETE,
                    &[&delivery.alarm.generation, &delivery.token, &now],
                )
                .await?
        } else {
            self.database
                .execute(
                    RETRY,
                    &[
                        &delivery.retry_at(now),
                        &delivery.alarm.generation,
                        &delivery.token,
                        &now,
                    ],
                )
                .await?
        };
        Ok(changed > 0)
    }
}
