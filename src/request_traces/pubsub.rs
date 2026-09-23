use super::{
    RequestTrace,
    sink::{TraceScope, TraceSink},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_util::future::join_all;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use std::{sync::Arc, time::Duration};

#[async_trait]
pub(crate) trait EventPublisher: Send + Sync {
    async fn publish(&self, data: Vec<u8>) -> Result<()>;
}

pub(crate) struct GooglePublisher(google_cloud_pubsub::client::Publisher);
impl GooglePublisher {
    pub(crate) async fn new(topic: &str) -> Result<Self> {
        Ok(Self(
            google_cloud_pubsub::client::Publisher::builder(topic)
                .set_message_count_threshold(64)
                .set_byte_threshold(512 * 1024)
                .set_delay_threshold(Duration::from_millis(50))
                .with_retry_policy(
                    google_cloud_pubsub::retry_policy::RetryableErrors
                        .with_time_limit(Duration::from_secs(2))
                        .with_attempt_limit(3),
                )
                .build()
                .await?,
        ))
    }
}
#[async_trait]
impl EventPublisher for GooglePublisher {
    async fn publish(&self, data: Vec<u8>) -> Result<()> {
        self.0
            .publish(google_cloud_pubsub::model::Message::new().set_data(data))
            .await?;
        Ok(())
    }
}

pub(crate) struct PubSubTraceSink {
    publisher: Arc<dyn EventPublisher>,
    clock: Arc<dyn crate::clock::Clock>,
    environment: String,
    metadata_fields: Vec<String>,
    retention_days: u32,
    capacity: u32,
    pending: Arc<tokio::sync::Semaphore>,
}
impl PubSubTraceSink {
    pub(crate) fn new(
        publisher: Arc<dyn EventPublisher>,
        clock: Arc<dyn crate::clock::Clock>,
        environment: String,
        metadata_fields: Vec<String>,
        retention_days: u32,
        capacity: u32,
    ) -> Self {
        Self {
            publisher,
            clock,
            environment,
            metadata_fields,
            retention_days,
            capacity,
            pending: Arc::new(tokio::sync::Semaphore::new(capacity as usize)),
        }
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        let _permits = tokio::time::timeout(
            Duration::from_secs(5),
            self.pending.acquire_many(self.capacity),
        )
        .await??;
        Ok(())
    }

    fn encode(&self, scope: &TraceScope, traces: Vec<RequestTrace>) -> Result<Vec<Vec<u8>>> {
        let now = self.clock.now_ms()?;
        scope
            .events(traces)
            .into_iter()
            .map(|event| self.encode_event(event, now))
            .collect()
    }

    fn encode_event(&self, event: super::TraceEvent, now: u64) -> Result<Vec<u8>> {
        let trace = &event.trace;
        trace.validate()?;
        self.validate_timestamp(trace.started_at_ms, now)?;
        Ok(serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "event_type": "actor.request.finished", "event_id": trace.event_id,
            "project_id": event.project_id, "environment": self.environment,
            "host_id": event.host_id, "session_id": event.session_id, "region": event.region,
            "request_id": trace.request_id, "actor_name": trace.actor_name, "actor_id": trace.actor_id,
            "kind": trace.kind, "operation": trace.operation, "connection_id": trace.connection_id,
            "started_at": timestamp(trace.started_at_ms)?, "received_at": timestamp(now)?,
            "duration_ms": trace.duration_ms, "queue_wait_ms": trace.queue_wait_ms, "outcome": trace.outcome,
            "metadata": self.approved_metadata(trace.metadata.as_ref())
        }))?)
    }

    fn validate_timestamp(&self, started: u64, now: u64) -> Result<()> {
        ensure!(
            started <= now.saturating_add(300_000),
            "trace timestamp exceeds clock skew allowance"
        );
        // BigQuery expires whole UTC partitions, not individual event ages.
        let oldest_day = (now / 86_400_000).saturating_sub(u64::from(self.retention_days) - 1);
        ensure!(
            started / 86_400_000 >= oldest_day,
            "trace expired before publication"
        );
        Ok(())
    }

    fn approved_metadata(&self, metadata: Option<&serde_json::Value>) -> Option<String> {
        let object = metadata?.as_object()?;
        let approved: serde_json::Map<_, _> = object
            .iter()
            .filter(|(key, _)| self.metadata_fields.contains(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Some(serde_json::Value::Object(approved).to_string())
    }
}

#[async_trait]
impl TraceSink for PubSubTraceSink {
    async fn record(
        &self,
        scope: &TraceScope,
        traces: Vec<RequestTrace>,
        dropped: u64,
    ) -> Result<()> {
        if dropped > 0 {
            tracing::warn!(project_id = %scope.project_id, host_id = %scope.host_id, dropped, "host reports undelivered telemetry; timed out reports may have been accepted");
        }
        if traces.is_empty() {
            return Ok(());
        }
        let permit = self
            .pending
            .clone()
            .try_acquire_many_owned(traces.len() as u32)
            .map_err(|error| {
                tracing::warn!(
                    count = traces.len(),
                    "analytics publisher is full; events were not published"
                );
                anyhow::Error::new(error).context("analytics publisher is full")
            })?;
        let messages = self.encode(scope, traces)?;
        let publisher = self.publisher.clone();
        // A cancelled report must not free capacity while the SDK still owns its messages.
        tokio::spawn(async move {
            let _permit = permit;
            let results = join_all(messages.into_iter().map(|data| publisher.publish(data))).await;
            let mut failure = None;
            for result in results {
                if let Err(error) = result {
                    tracing::warn!(%error, "analytics publication failed; outcome may be unknown");
                    failure = Some(error);
                }
            }
            match failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })
        .await?
    }
}

fn timestamp(ms: u64) -> Result<String> {
    Ok(chrono::DateTime::from_timestamp_millis(i64::try_from(ms)?)
        .context("invalid trace timestamp")?
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

#[cfg(test)]
#[path = "../../tests/unit/request_traces/pubsub/tests.rs"]
mod tests;
