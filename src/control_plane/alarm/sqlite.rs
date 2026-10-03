use super::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) struct SqliteAlarmStore {
    database: Arc<Mutex<rusqlite::Connection>>,
}
impl SqliteAlarmStore {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        let database = rusqlite::Connection::open(path)?;
        database.busy_timeout(std::time::Duration::from_secs(5))?;
        database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        database.execute_batch(
            &include_str!("../../../migrations/V17__alarms.sql")
                .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
                .replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS "),
        )?;
        Ok(Self {
            database: Arc::new(Mutex::new(database)),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || operation(&mut database.lock().unwrap())).await?
    }
}

#[async_trait]
impl AlarmStore for SqliteAlarmStore {
    async fn register(&self, actor: &ActorKey, region: &str, alarm: &Alarm) -> Result<()> {
        alarm.validate()?;
        let (actor, region, alarm) = (actor.clone(), region.to_owned(), alarm.clone());
        self.run(move |database| {
            database.execute(
                REGISTER,
                rusqlite::params![
                    alarm.generation,
                    actor.project_id,
                    actor.actor_name,
                    actor.actor_id,
                    region,
                    alarm.deadline
                ],
            )?;
            Ok(())
        })
        .await
    }
    async fn claim(&self, now: i64, limit: i64) -> Result<Vec<Delivery>> {
        self.run(move |database| {
            let transaction = database.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let deliveries = transaction.prepare("SELECT generation,project_id,actor_name,actor_id,region,deadline,attempts FROM durable_actors_alarms WHERE available_at<=?1 AND lease_until<=?1 ORDER BY available_at LIMIT ?2")?.query_map([now,limit], |row| Ok(Delivery {
                actor: ActorKey {project_id: row.get(1)?,actor_name: row.get(2)?,actor_id: row.get(3)?},region: row.get(4)?,alarm: Alarm {generation: row.get(0)?,deadline: row.get(5)?},token: uuid::Uuid::new_v4().to_string(),attempt: row.get::<_,i32>(6)?.saturating_add(1)
            }))?.collect::<Result<Vec<_>,_>>()?;
            for delivery in &deliveries {
                transaction.execute("UPDATE durable_actors_alarms SET token=?1,lease_until=?2,attempts=?3 WHERE generation=?4",rusqlite::params![delivery.token,now+LEASE_MS,delivery.attempt,delivery.alarm.generation])?;
            }
            transaction.commit()?;
            Ok(deliveries)
        }).await
    }
    async fn renew(&self, delivery: &Delivery, now: i64) -> Result<bool> {
        let delivery = delivery.clone();
        self.run(move |database| {
            Ok(database.execute(
                RENEW,
                rusqlite::params![
                    now + LEASE_MS,
                    delivery.alarm.generation,
                    delivery.token,
                    now
                ],
            )? > 0)
        })
        .await
    }
    async fn finish(&self, delivery: &Delivery, now: i64, completed: bool) -> Result<bool> {
        let delivery = delivery.clone();
        self.run(move |database| {
            let changed = if completed {
                database.execute(
                    COMPLETE,
                    rusqlite::params![delivery.alarm.generation, delivery.token, now],
                )?
            } else {
                database.execute(
                    RETRY,
                    rusqlite::params![
                        delivery.retry_at(now),
                        delivery.alarm.generation,
                        delivery.token,
                        now
                    ],
                )?
            };
            Ok(changed > 0)
        })
        .await
    }
}
