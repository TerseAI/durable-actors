use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};

pub(crate) const TRACE_CAPACITY: usize = 500;
pub(crate) const TRACE_BATCH_SIZE: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestTrace {
    pub request_id: String,
    pub actor_type: String,
    pub actor_id: String,
    pub kind: RequestKind,
    pub operation: String,
    pub connection_id: Option<String>,
    pub started_at_ms: u64,
    pub duration_ms: f64,
    pub queue_wait_ms: Option<f64>,
    pub outcome: RequestOutcome,
}

impl RequestTrace {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::actor::ActorKey {
            actor_type: self.actor_type.clone(),
            actor_id: self.actor_id.clone(),
        }
        .validate()?;
        ensure!(
            !self.request_id.is_empty() && self.request_id.len() <= 256,
            "invalid trace request ID"
        );
        ensure!(
            !self.operation.is_empty() && self.operation.len() <= 256,
            "invalid trace operation"
        );
        ensure!(
            self.connection_id.as_ref().is_none_or(|id| id.len() <= 256),
            "invalid trace connection ID"
        );
        ensure!(
            self.duration_ms.is_finite() && self.duration_ms >= 0.0,
            "invalid trace duration"
        );
        ensure!(
            self.queue_wait_ms
                .is_none_or(|ms| ms.is_finite() && ms >= 0.0 && ms <= self.duration_ms),
            "invalid trace queue wait"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RequestKind {
    Method,
    Websocket,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RequestOutcome {
    Completed,
    Failed,
    Rejected,
    Rerouted,
    Interrupted,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceRecord {
    pub sequence: u64,
    pub host_id: String,
    pub session_id: String,
    #[serde(flatten)]
    pub trace: RequestTrace,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TracePage {
    pub epoch: String,
    pub cursor: u64,
    pub capacity: usize,
    pub evicted: u64,
    pub dropped: u64,
    pub records: Vec<TraceRecord>,
}

#[derive(Clone)]
pub(crate) struct TraceStore {
    inner: Arc<Mutex<TraceHistory>>,
    pub changes: watch::Sender<()>,
}

struct TraceHistory {
    epoch: String,
    cursor: u64,
    dropped: u64,
    records: VecDeque<TraceRecord>,
}

impl Default for TraceStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TraceHistory {
                epoch: uuid::Uuid::new_v4().to_string(),
                cursor: 0,
                dropped: 0,
                records: VecDeque::new(),
            })),
            changes: watch::channel(()).0,
        }
    }
}

impl TraceStore {
    pub(crate) fn record(
        &self,
        host: &str,
        session: &str,
        traces: Vec<RequestTrace>,
        dropped: u64,
    ) {
        let mut history = self.inner.lock().unwrap();
        history.dropped = history.dropped.saturating_add(dropped);
        for trace in traces {
            history.cursor += 1;
            let sequence = history.cursor;
            history.records.push_back(TraceRecord {
                sequence,
                host_id: host.into(),
                session_id: session.into(),
                trace,
            });
            if history.records.len() > TRACE_CAPACITY {
                history.records.pop_front();
            }
        }
        drop(history);
        self.changes.send_replace(());
    }

    pub(crate) fn page(&self, after: u64) -> TracePage {
        let history = self.inner.lock().unwrap();
        TracePage {
            epoch: history.epoch.clone(),
            cursor: history.cursor,
            capacity: TRACE_CAPACITY,
            evicted: history.cursor.saturating_sub(TRACE_CAPACITY as u64),
            dropped: history.dropped,
            records: history
                .records
                .iter()
                .filter(|record| record.sequence > after)
                .cloned()
                .collect(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TraceSender {
    sender: mpsc::Sender<RequestTrace>,
    dropped: Arc<AtomicU64>,
}

impl TraceSender {
    #[cfg(test)]
    pub(crate) fn channel(capacity: usize) -> (Self, mpsc::Receiver<RequestTrace>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Self {
                sender,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            receiver,
        )
    }
    pub(crate) fn start(
        client: Arc<crate::control_plane::ControlPlaneClient>,
        stop: tokio_util::sync::CancellationToken,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(1024);
        let dropped = Arc::new(AtomicU64::new(0));
        let lost = dropped.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = interval.tick() => {} }
                let mut batch = Vec::new();
                while batch.len() < TRACE_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(trace) => batch.push(trace),
                        Err(_) => break,
                    }
                }
                let missing = lost.swap(0, Ordering::Relaxed);
                if batch.is_empty() && missing == 0 {
                    continue;
                }
                let count = batch.len() as u64;
                if !matches!(
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        client.report_traces(batch, missing)
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    lost.fetch_add(count.saturating_add(missing), Ordering::Relaxed);
                }
            }
        });
        Self { sender, dropped }
    }

    fn send(&self, trace: RequestTrace) {
        if self.sender.try_send(trace).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct RequestSpan {
    sender: TraceSender,
    started: Instant,
    trace: Option<RequestTrace>,
}

impl RequestSpan {
    pub(crate) fn new(
        sender: TraceSender,
        invocation: &crate::actor::ActorInvocation,
        kind: RequestKind,
        connection_id: Option<String>,
        started: Instant,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            sender,
            started,
            trace: Some(RequestTrace {
                request_id: invocation.request_id.clone(),
                actor_type: invocation.actor.actor_type.clone(),
                actor_id: invocation.actor.actor_id.clone(),
                kind,
                operation: invocation.method.clone(),
                connection_id,
                started_at_ms: now.saturating_sub(started.elapsed().as_millis() as u64),
                duration_ms: 0.0,
                queue_wait_ms: None,
                outcome: RequestOutcome::Interrupted,
            }),
        }
    }

    pub(crate) fn admitted(&mut self) {
        self.trace.as_mut().unwrap().queue_wait_ms =
            Some(self.started.elapsed().as_secs_f64() * 1000.0);
    }

    pub(crate) fn finish(&mut self, outcome: RequestOutcome) {
        if let Some(mut trace) = self.trace.take() {
            trace.outcome = outcome;
            trace.duration_ms = self.started.elapsed().as_secs_f64() * 1000.0;
            self.sender.send(trace);
        }
    }

    pub(crate) fn complete(&mut self, result: &Result<crate::actor::ActorExecutionResult>) {
        use crate::actor::ActorExecutionResult as R;
        self.finish(match result {
            Ok(R::Completed { .. }) => RequestOutcome::Completed,
            Ok(R::HostUnavailable) => RequestOutcome::Rejected,
            Ok(R::Reroute) => RequestOutcome::Rerouted,
            _ => RequestOutcome::Failed,
        });
    }
}

impl Drop for RequestSpan {
    fn drop(&mut self) {
        self.finish(RequestOutcome::Interrupted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(id: usize) -> RequestTrace {
        RequestTrace {
            request_id: id.to_string(),
            actor_type: "Counter".into(),
            actor_id: "one".into(),
            kind: RequestKind::Method,
            operation: "increment".into(),
            connection_id: None,
            started_at_ms: 1000,
            duration_ms: 12.0,
            queue_wait_ms: Some(5.0),
            outcome: RequestOutcome::Completed,
        }
    }

    #[test]
    fn history_keeps_distinct_requests_and_reports_expired_records() {
        let store = TraceStore::default();
        for id in 0..TRACE_CAPACITY + 2 {
            store.record("host", "session", vec![trace(id)], 0);
        }
        let page = store.page(0);
        assert_eq!(page.records.len(), TRACE_CAPACITY);
        assert_eq!(page.evicted, 2);
        assert_eq!(page.records[0].trace.request_id, "2");
        assert_eq!(page.cursor, (TRACE_CAPACITY + 2) as u64);
        assert!(store.page(page.cursor).records.is_empty());
        assert_eq!(store.page(page.cursor - 1).records.len(), 1);
    }

    #[test]
    fn trace_validation_rejects_invalid_timings() {
        let mut value = trace(1);
        assert!(value.validate().is_ok());
        value.queue_wait_ms = Some(13.0);
        assert!(value.validate().is_err());
        value.queue_wait_ms = None;
        value.duration_ms = -1.0;
        assert!(value.validate().is_err());
    }

    #[test]
    fn delivery_loss_is_visible_even_without_new_records() {
        let store = TraceStore::default();
        store.record("host", "session", vec![], 3);
        assert_eq!(store.page(0).dropped, 3);
    }

    #[test]
    fn backpressure_drops_telemetry_without_blocking_requests() {
        let (sender, _receiver) = TraceSender::channel(1);
        sender.send(trace(1));
        sender.send(trace(2));
        assert_eq!(sender.dropped.load(Ordering::Relaxed), 1);
    }
}
