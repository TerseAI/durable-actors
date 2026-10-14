use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{actor::ActorKey, postgres::PostgresDatabase};

use super::ControlPlaneService;

pub(super) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(super) struct ClaimedAlarm {
    pub actor: ActorKey,
    pub revision: u64,
    timestamp_ms: i64,
}

#[async_trait]
pub(super) trait AlarmStore: Send + Sync {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>>;
    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()>;
    async fn claim_due(&self, now_ms: i64) -> Result<Option<ClaimedAlarm>>;
    async fn restore(&self, alarm: &ClaimedAlarm) -> Result<()>;
}

pub(super) fn start(
    service: ControlPlaneService,
    store: Arc<dyn AlarmStore>,
    poll_interval: Duration,
    stop: CancellationToken,
) {
    tokio::spawn(run(service, store, poll_interval, stop));
}

async fn run(
    service: ControlPlaneService,
    store: Arc<dyn AlarmStore>,
    poll_interval: Duration,
    stop: CancellationToken,
) {
    let client = reqwest::Client::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(poll_interval) => {}
        }
        let now_ms = match crate::clock::Clock::now_ms(&crate::clock::SystemClock)
            .and_then(|value| i64::try_from(value).context("alarm clock overflow"))
        {
            Ok(now_ms) => now_ms,
            Err(error) => {
                tracing::warn!(%error, "could not read alarm clock");
                continue;
            }
        };
        loop {
            let alarm = match store.claim_due(now_ms).await {
                Ok(Some(alarm)) => alarm,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "could not claim due alarm");
                    break;
                }
            };
            if super::invocation::invoke_alarm(&service, &client, &alarm).await {
                continue;
            }
            if let Err(error) = store.restore(&alarm).await {
                tracing::warn!(%error, "could not restore undelivered alarm");
            }
            break;
        }
    }
}

pub(super) struct PostgresAlarmStore {
    database: PostgresDatabase,
}

impl PostgresAlarmStore {
    pub(super) fn new(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl AlarmStore for PostgresAlarmStore {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>> {
        Ok(self
            .database
            .query_opt(
                "SELECT timestamp_ms
                 FROM durable_actors_alarm
                 WHERE project_id = $1 AND actor_name = $2 AND actor_id = $3",
                &[&actor.project_id, &actor.actor_name, &actor.actor_id],
            )
            .await?
            .and_then(|row| row.get("timestamp_ms")))
    }

    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()> {
        self.database
            .execute(
                "INSERT INTO durable_actors_alarm (
                    project_id, actor_name, actor_id, revision, timestamp_ms
                 ) VALUES ($1, $2, $3, 1, $4)
                 ON CONFLICT (project_id, actor_name, actor_id) DO UPDATE SET
                    revision = durable_actors_alarm.revision + 1,
                    timestamp_ms = EXCLUDED.timestamp_ms",
                &[
                    &actor.project_id,
                    &actor.actor_name,
                    &actor.actor_id,
                    &timestamp_ms,
                ],
            )
            .await?;
        Ok(())
    }

    async fn claim_due(&self, now_ms: i64) -> Result<Option<ClaimedAlarm>> {
        let Some(row) = self
            .database
            .query_opt(
                "WITH due AS (
                    SELECT project_id, actor_name, actor_id, timestamp_ms
                    FROM durable_actors_alarm
                    WHERE timestamp_ms <= $1
                    ORDER BY timestamp_ms
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                 )
                 UPDATE durable_actors_alarm AS alarm
                 SET timestamp_ms = NULL
                 FROM due
                 WHERE alarm.project_id = due.project_id
                   AND alarm.actor_name = due.actor_name
                   AND alarm.actor_id = due.actor_id
                 RETURNING alarm.project_id, alarm.actor_name, alarm.actor_id,
                           alarm.revision, due.timestamp_ms",
                &[&now_ms],
            )
            .await?
        else {
            return Ok(None);
        };
        let revision: i64 = row.get("revision");
        Ok(Some(ClaimedAlarm {
            actor: ActorKey {
                project_id: row.get("project_id"),
                actor_name: row.get("actor_name"),
                actor_id: row.get("actor_id"),
            },
            revision: u64::try_from(revision).context("negative alarm revision")?,
            timestamp_ms: row.get("timestamp_ms"),
        }))
    }

    async fn restore(&self, alarm: &ClaimedAlarm) -> Result<()> {
        let revision = i64::try_from(alarm.revision).context("alarm revision overflow")?;
        self.database
            .execute(
                "UPDATE durable_actors_alarm
                 SET timestamp_ms = $4
                 WHERE project_id = $1 AND actor_name = $2 AND actor_id = $3
                   AND revision = $5 AND timestamp_ms IS NULL",
                &[
                    &alarm.actor.project_id,
                    &alarm.actor.actor_name,
                    &alarm.actor.actor_id,
                    &alarm.timestamp_ms,
                    &revision,
                ],
            )
            .await?;
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct MemoryAlarmStore {
    alarms: Mutex<HashMap<ActorKey, MemoryAlarm>>,
}

struct MemoryAlarm {
    revision: u64,
    timestamp_ms: Option<i64>,
}

#[async_trait]
impl AlarmStore for MemoryAlarmStore {
    async fn get(&self, actor: &ActorKey) -> Result<Option<i64>> {
        Ok(self
            .alarms
            .lock()
            .unwrap()
            .get(actor)
            .and_then(|alarm| alarm.timestamp_ms))
    }

    async fn update(&self, actor: &ActorKey, timestamp_ms: Option<i64>) -> Result<()> {
        let mut alarms = self.alarms.lock().unwrap();
        let revision = match alarms.get(actor) {
            Some(alarm) => alarm
                .revision
                .checked_add(1)
                .context("alarm revision overflow")?,
            None => 1,
        };
        alarms.insert(
            actor.clone(),
            MemoryAlarm {
                revision,
                timestamp_ms,
            },
        );
        Ok(())
    }

    async fn claim_due(&self, now_ms: i64) -> Result<Option<ClaimedAlarm>> {
        let mut alarms = self.alarms.lock().unwrap();
        let Some((actor, alarm)) = alarms
            .iter_mut()
            .filter(|(_, alarm)| {
                alarm
                    .timestamp_ms
                    .is_some_and(|timestamp| timestamp <= now_ms)
            })
            .min_by_key(|(_, alarm)| alarm.timestamp_ms)
        else {
            return Ok(None);
        };
        let timestamp_ms = alarm.timestamp_ms.take().unwrap();
        Ok(Some(ClaimedAlarm {
            actor: actor.clone(),
            revision: alarm.revision,
            timestamp_ms,
        }))
    }

    async fn restore(&self, alarm: &ClaimedAlarm) -> Result<()> {
        let mut alarms = self.alarms.lock().unwrap();
        let Some(current) = alarms.get_mut(&alarm.actor) else {
            return Ok(());
        };
        if current.revision == alarm.revision && current.timestamp_ms.is_none() {
            current.timestamp_ms = Some(alarm.timestamp_ms);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/alarms.rs"]
mod tests;
