use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};

pub(crate) mod persistence;
pub(crate) mod query;
pub(crate) mod replay;
use persistence::{SqliteTracePersistence, TracePersistence};
use replay::ReplayQuery;

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

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceEvent {
    #[serde(default = "new_event_id")]
    pub event_id: String,
    pub host_id: String,
    pub session_id: String,
    #[serde(flatten)]
    pub trace: RequestTrace,
}

fn new_event_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceRecord {
    pub sequence: u64,
    #[serde(flatten)]
    pub event: TraceEvent,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TracePage {
    pub epoch: String,
    pub cursor: u64,
    pub capacity: usize,
    pub evicted: u64,
    pub dropped: u64,
    pub persistence_failed: bool,
    pub records: Vec<TraceRecord>,
    pub next_cursor: Option<String>,
    pub resume_cursor: String,
    pub reset: bool,
}

#[derive(Clone)]
pub(crate) struct TraceStore {
    writer: Arc<tokio::sync::Mutex<()>>,
    pending: Arc<tokio::sync::Semaphore>,
    persistence: Arc<dyn TracePersistence>,
    dropped: Arc<AtomicU64>,
    persistence_failed: Arc<AtomicBool>,
    pub changes: watch::Sender<()>,
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new(Arc::new(SqliteTracePersistence::in_memory()))
    }
}

impl TraceStore {
    pub(crate) async fn open(persistence: Arc<dyn TracePersistence>) -> Result<Self> {
        persistence
            .replay(&ReplayQuery {
                limit: 1,
                ..Default::default()
            })
            .await?;
        Ok(Self::new(persistence))
    }

    pub(crate) async fn record(
        &self,
        host: &str,
        session: &str,
        traces: Vec<RequestTrace>,
        dropped: u64,
    ) -> Result<()> {
        self.dropped.fetch_add(dropped, Ordering::Relaxed);
        let events = traces
            .into_iter()
            .map(|trace| TraceEvent {
                event_id: new_event_id(),
                host_id: host.into(),
                session_id: session.into(),
                trace,
            })
            .collect();
        self.persist_detached(events).await
    }

    pub(crate) async fn replay(&self, query: &ReplayQuery) -> Result<TracePage> {
        query.validate()?;
        let mut page = self.persistence.replay(query).await?;
        page.dropped = self.dropped.load(Ordering::Relaxed);
        page.persistence_failed = self.persistence_failed.load(Ordering::Relaxed);
        Ok(page)
    }

    pub(crate) async fn query(&self, query: &query::SqlQuery) -> Result<query::SqlResult> {
        self.persistence.query(query).await
    }

    fn new(persistence: Arc<dyn TracePersistence>) -> Self {
        Self {
            writer: Arc::new(tokio::sync::Mutex::new(())),
            pending: Arc::new(tokio::sync::Semaphore::new(64)),
            persistence,
            dropped: Arc::new(AtomicU64::new(0)),
            persistence_failed: Arc::new(AtomicBool::new(false)),
            changes: watch::channel(()).0,
        }
    }

    async fn persist_detached(&self, events: Vec<TraceEvent>) -> Result<()> {
        if events.is_empty() {
            self.changes.send_replace(());
            return Ok(());
        }
        let permit = match self.pending.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(error) => {
                self.report_persistence_failure(error, events.len());
                return Ok(());
            }
        };
        let store = self.clone();
        // Finish the commit even if the reporting connection is cancelled.
        tokio::spawn(async move {
            let _permit = permit;
            let _writer = store.writer.lock().await;
            match store.persistence.append(&events).await {
                Ok(()) => {
                    store.changes.send_replace(());
                }
                Err(error) => store.report_persistence_failure(error, events.len()),
            }
        })
        .await?;
        Ok(())
    }

    fn report_persistence_failure(&self, error: impl std::fmt::Display, count: usize) {
        tracing::error!(%error, count, "request trace persistence failed");
        self.persistence_failed.store(true, Ordering::Relaxed);
        self.changes.send_replace(());
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
mod tests;
