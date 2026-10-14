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

#[derive(Clone)]
pub(super) struct ClaimedAlarm {
    pub actor: ActorKey,
    pub state_version: u64,
    timestamp_ms: i64,
}

#[async_trait]
pub(super) trait AlarmStore: Send + Sync {
    async fn update(
        &self,
        actor: &ActorKey,
        state_version: u64,
        timestamp_ms: Option<i64>,
    ) -> Result<()>;
    async fn claim_due(&self, now_ms: i64) -> Result<Option<ClaimedAlarm>>;
    async fn restore(&self, alarm: &ClaimedAlarm) -> Result<()>;
}

pub(super) fn start(
    service: ControlPlaneService,
    store: Arc<dyn AlarmStore>,
    stop: CancellationToken,
) {
    tokio::spawn(run(service, store, stop));
}

async fn run(service: ControlPlaneService, store: Arc<dyn AlarmStore>, stop: CancellationToken) {
    let client = reqwest::Client::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
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
    async fn update(
        &self,
        actor: &ActorKey,
        state_version: u64,
        timestamp_ms: Option<i64>,
    ) -> Result<()> {
        let state_version = i64::try_from(state_version).context("alarm state version overflow")?;
        let actor_key = actor.storage_key();
        self.database
            .execute(
                "INSERT INTO durable_actors_alarm (
                    actor_key, project_id, actor_name, actor_id, state_version, timestamp_ms
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (actor_key) DO UPDATE SET
                    project_id = EXCLUDED.project_id,
                    actor_name = EXCLUDED.actor_name,
                    actor_id = EXCLUDED.actor_id,
                    state_version = EXCLUDED.state_version,
                    timestamp_ms = EXCLUDED.timestamp_ms
                 WHERE EXCLUDED.state_version > durable_actors_alarm.state_version",
                &[
                    &actor_key.as_str(),
                    &actor.project_id,
                    &actor.actor_name,
                    &actor.actor_id,
                    &state_version,
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
                    SELECT actor_key, timestamp_ms
                    FROM durable_actors_alarm
                    WHERE timestamp_ms <= $1
                    ORDER BY timestamp_ms
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                 )
                 UPDATE durable_actors_alarm AS alarm
                 SET timestamp_ms = NULL
                 FROM due
                 WHERE alarm.actor_key = due.actor_key
                 RETURNING alarm.project_id, alarm.actor_name, alarm.actor_id,
                           alarm.state_version, due.timestamp_ms",
                &[&now_ms],
            )
            .await?
        else {
            return Ok(None);
        };
        let state_version: i64 = row.get("state_version");
        Ok(Some(ClaimedAlarm {
            actor: ActorKey {
                project_id: row.get("project_id"),
                actor_name: row.get("actor_name"),
                actor_id: row.get("actor_id"),
            },
            state_version: u64::try_from(state_version).context("negative alarm state version")?,
            timestamp_ms: row.get("timestamp_ms"),
        }))
    }

    async fn restore(&self, alarm: &ClaimedAlarm) -> Result<()> {
        let state_version =
            i64::try_from(alarm.state_version).context("alarm state version overflow")?;
        let actor_key = alarm.actor.storage_key();
        self.database
            .execute(
                "UPDATE durable_actors_alarm
                 SET timestamp_ms = $2
                 WHERE actor_key = $1 AND state_version = $3 AND timestamp_ms IS NULL",
                &[&actor_key.as_str(), &alarm.timestamp_ms, &state_version],
            )
            .await?;
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct MemoryAlarmStore {
    alarms: Mutex<HashMap<String, MemoryAlarm>>,
}

struct MemoryAlarm {
    actor: ActorKey,
    state_version: u64,
    timestamp_ms: Option<i64>,
}

#[async_trait]
impl AlarmStore for MemoryAlarmStore {
    async fn update(
        &self,
        actor: &ActorKey,
        state_version: u64,
        timestamp_ms: Option<i64>,
    ) -> Result<()> {
        let mut alarms = self.alarms.lock().unwrap();
        let key = actor.storage_key().to_string();
        if alarms
            .get(&key)
            .is_some_and(|current| current.state_version >= state_version)
        {
            return Ok(());
        }
        alarms.insert(
            key,
            MemoryAlarm {
                actor: actor.clone(),
                state_version,
                timestamp_ms,
            },
        );
        Ok(())
    }

    async fn claim_due(&self, now_ms: i64) -> Result<Option<ClaimedAlarm>> {
        let mut alarms = self.alarms.lock().unwrap();
        let Some((_, alarm)) = alarms
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
            actor: alarm.actor.clone(),
            state_version: alarm.state_version,
            timestamp_ms,
        }))
    }

    async fn restore(&self, alarm: &ClaimedAlarm) -> Result<()> {
        let mut alarms = self.alarms.lock().unwrap();
        let Some(current) = alarms.get_mut(&alarm.actor.storage_key().to_string()) else {
            return Ok(());
        };
        if current.state_version == alarm.state_version && current.timestamp_ms.is_none() {
            current.timestamp_ms = Some(alarm.timestamp_ms);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/alarms.rs"]
mod tests;
