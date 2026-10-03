mod postgres;
mod scheduler;
mod sqlite;

use crate::actor::ActorKey;
use anyhow::{Result, ensure};
use async_trait::async_trait;
pub(super) use postgres::PostgresAlarmStore;
pub(super) use scheduler::AlarmScheduler;
pub(super) use sqlite::SqliteAlarmStore;

const LEASE_MS: i64 = 60_000;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alarm {
    pub generation: String,
    pub deadline: i64,
}

impl Alarm {
    pub(crate) fn validate(&self) -> Result<()> {
        uuid::Uuid::parse_str(&self.generation)?;
        ensure!(
            (0..=9_007_199_254_740_991).contains(&self.deadline),
            "invalid alarm deadline"
        );
        Ok(())
    }
}

#[async_trait]
pub(crate) trait AlarmStore: Send + Sync {
    async fn register(&self, actor: &ActorKey, region: &str, alarm: &Alarm) -> Result<()>;
    async fn claim(&self, now: i64, limit: i64) -> Result<Vec<Delivery>>;
    async fn renew(&self, delivery: &Delivery, now: i64) -> Result<bool>;
    async fn finish(&self, delivery: &Delivery, now: i64, completed: bool) -> Result<bool>;
}

#[derive(Clone, Debug)]
pub(crate) struct Delivery {
    actor: ActorKey,
    region: String,
    alarm: Alarm,
    token: String,
    attempt: i32,
}

impl Delivery {
    fn retry_at(&self, now: i64) -> i64 {
        now + (1_000i64 << (self.attempt - 1).clamp(0, 8)).min(300_000)
    }
}

const REGISTER: &str = "INSERT INTO durable_actors_alarms
    (generation, project_id, actor_name, actor_id, region, deadline, available_at)
    VALUES ($1,$2,$3,$4,$5,$6,$6) ON CONFLICT(generation) DO NOTHING";
const RENEW: &str = "UPDATE durable_actors_alarms SET lease_until=$1 WHERE generation=$2 AND token=$3 AND lease_until>$4";
const COMPLETE: &str =
    "DELETE FROM durable_actors_alarms WHERE generation=$1 AND token=$2 AND lease_until>$3";
const RETRY: &str = "UPDATE durable_actors_alarms SET available_at=$1, token=NULL, lease_until=0 WHERE generation=$2 AND token=$3 AND lease_until>$4";

#[cfg(test)]
#[path = "../../tests/unit/control_plane/alarm.rs"]
mod tests;
