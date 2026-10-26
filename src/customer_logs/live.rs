use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use axum::response::sse::Event;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use super::{LogPage, LogQuery, LogStore, persistence::LogReport};
use crate::redis_client::Redis;

const CHANNEL: &str = "durable-actors:v1:customer-logs";

#[derive(Clone)]
pub(crate) struct LiveLogFeed {
    origin: String,
    updates: broadcast::Sender<Arc<LiveLogUpdate>>,
    outgoing: Option<mpsc::Sender<Arc<LiveLogUpdate>>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum LiveLogUpdate {
    Batch(LogReport),
    Resync,
    Gap {
        id: String,
        project: String,
        dropped: u64,
    },
    Status {
        origin: String,
        failed: bool,
    },
}

impl Default for LiveLogFeed {
    fn default() -> Self {
        Self {
            origin: uuid::Uuid::new_v4().to_string(),
            updates: broadcast::channel(16).0,
            outgoing: None,
        }
    }
}

impl LiveLogFeed {
    pub(crate) fn start(redis: Option<Redis>, stop: CancellationToken) -> Self {
        let mut feed = Self::default();
        if let Some(redis) = redis {
            let origin = feed.origin.clone();
            let (sender, receiver) = mpsc::channel(16);
            feed.outgoing = Some(sender);
            tokio::spawn(publish(
                redis.clone(),
                origin.clone(),
                receiver,
                feed.updates.clone(),
                stop.clone(),
            ));
            tokio::spawn(subscribe(redis, origin, feed.updates.clone(), stop));
        }
        feed
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Arc<LiveLogUpdate>> {
        self.updates.subscribe()
    }

    pub(crate) fn publish(&self, report: &LogReport) {
        self.emit(LiveLogUpdate::Batch(report.clone()));
    }

    pub(crate) fn gap(&self, id: String, project: &str, dropped: u64) {
        self.emit(LiveLogUpdate::Gap {
            id,
            project: project.into(),
            dropped,
        });
    }

    pub(crate) fn status(&self, failed: bool) {
        self.emit(LiveLogUpdate::Status {
            origin: self.origin.clone(),
            failed,
        });
    }

    fn emit(&self, update: LiveLogUpdate) {
        let update = Arc::new(update);
        let _ = self.updates.send(update.clone());
        if let Some(outgoing) = &self.outgoing {
            if outgoing.try_send(update.clone()).is_err() {
                if let LiveLogUpdate::Batch(report) = update.as_ref() {
                    let _ = self.updates.send(Arc::new(LiveLogUpdate::Gap {
                        id: format!("{}:broadcast", report.id),
                        project: report.project.clone(),
                        dropped: report.records.len().max(1) as u64,
                    }));
                }
                tracing::warn!("Customer log broadcast queue full");
            }
        }
    }
}

pub(crate) async fn stream(
    store: LogStore,
    project: String,
    query: LogQuery,
    mut updates: broadcast::Receiver<Arc<LiveLogUpdate>>,
    sender: mpsc::Sender<Result<Event, Infallible>>,
) {
    let (mut page, mut seen) = store
        .snapshot(&project, &query)
        .unwrap_or_else(|_| (store.status(&project), Default::default()));
    page.next_cursor = None;
    let mut observed_dropped = page.dropped;
    let mut failed_writers = HashMap::new();
    if !send(&store, &sender, &page).await {
        return;
    }
    let mut status_tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        page.records.clear();
        tokio::select! {
            _ = sender.closed() => return,
            _ = store.cancelled() => return,
            _ = status_tick.tick() => {},
            update = updates.recv() => match update {
                Ok(update) => match update.as_ref() {
                    LiveLogUpdate::Batch(report) if report.project == project => {
                        if !unseen(&mut seen, &report.id) { continue; }
                        page.records = report.records.iter().filter(|log| log.project_id == project && query.matches(log)).cloned().collect();
                        observed_dropped = observed_dropped.saturating_add(report.dropped);
                    }
                    LiveLogUpdate::Batch(_) => continue,
                    LiveLogUpdate::Gap { id, project: affected, dropped } if *affected == project => {
                        if !unseen(&mut seen, id) { continue; }
                        observed_dropped = observed_dropped.saturating_add(*dropped);
                    }
                    LiveLogUpdate::Gap { .. } => continue,
                    LiveLogUpdate::Status { origin, failed } => {
                        if *failed { remember_failure(&mut failed_writers, origin); } else { failed_writers.remove(origin); }
                    }
                    LiveLogUpdate::Resync => { observed_dropped = observed_dropped.saturating_add(1); }
                },
                Err(broadcast::error::RecvError::Lagged(count)) => { observed_dropped = observed_dropped.saturating_add(count); }
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
        failed_writers.retain(|_, at| at.elapsed() < Duration::from_secs(90));
        let status = store.status(&project);
        page.dropped = observed_dropped.max(status.dropped);
        page.persistence_failed = status.persistence_failed || !failed_writers.is_empty();
        if !send(&store, &sender, &page).await {
            return;
        }
    }
}

fn remember_failure(failures: &mut HashMap<String, tokio::time::Instant>, origin: &str) {
    if failures.len() >= 4096 && !failures.contains_key(origin) {
        if let Some(oldest) = failures
            .iter()
            .min_by_key(|(_, at)| *at)
            .map(|(key, _)| key.clone())
        {
            failures.remove(&oldest);
        }
    }
    failures.insert(origin.into(), tokio::time::Instant::now());
}

fn unseen(seen: &mut HashSet<String>, id: &str) -> bool {
    if seen.contains(id) {
        return false;
    }
    if seen.len() >= 4096 {
        seen.clear();
    }
    seen.insert(id.into());
    true
}

async fn send(
    store: &LogStore,
    sender: &mpsc::Sender<Result<Event, Infallible>>,
    page: &LogPage,
) -> bool {
    let event = Event::default()
        .event("logs")
        .json_data(page)
        .expect("serializable log page");
    tokio::select! {
        _ = store.cancelled() => false,
        result = sender.send(Ok(event)) => result.is_ok(),
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    origin: String,
    update: LiveLogUpdate,
}

async fn publish(
    redis: Redis,
    origin: String,
    mut receiver: mpsc::Receiver<Arc<LiveLogUpdate>>,
    updates: broadcast::Sender<Arc<LiveLogUpdate>>,
    stop: CancellationToken,
) {
    let work = async {
        while let Some(update) = receiver.recv().await {
            let result = async {
                let payload = serde_json::to_string(
                    &(serde_json::json!({"origin": origin, "update": update.as_ref()})),
                )?;
                let mut command = redis::cmd("PUBLISH");
                command.arg(CHANNEL).arg(payload);
                redis.run::<usize>(command).await?;
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "Customer log broadcast failed");
                if let LiveLogUpdate::Batch(report) = update.as_ref() {
                    let _ = updates.send(Arc::new(LiveLogUpdate::Gap {
                        id: format!("{}:broadcast", report.id),
                        project: report.project.clone(),
                        dropped: report.records.len().max(1) as u64,
                    }));
                }
            }
        }
    };
    tokio::select! { _ = stop.cancelled() => {}, _ = work => {} }
}

async fn subscribe(
    redis: Redis,
    origin: String,
    updates: broadcast::Sender<Arc<LiveLogUpdate>>,
    stop: CancellationToken,
) {
    loop {
        let result = tokio::select! {
            _ = stop.cancelled() => return,
            result = listen(&redis, &origin, &updates) => result,
        };
        if let Err(error) = result {
            tracing::warn!(%error, "Customer log subscription disconnected");
            let _ = updates.send(Arc::new(LiveLogUpdate::Resync));
        }
        tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
}

async fn listen(
    redis: &Redis,
    origin: &str,
    updates: &broadcast::Sender<Arc<LiveLogUpdate>>,
) -> Result<()> {
    let mut subscriber = redis.pubsub().await?;
    tokio::time::timeout(Duration::from_secs(5), subscriber.subscribe(CHANNEL)).await??;
    let mut messages = subscriber.on_message();
    while let Some(message) = messages.next().await {
        let envelope: Envelope = serde_json::from_slice(message.get_payload_bytes())?;
        if envelope.origin != origin {
            let _ = updates.send(Arc::new(envelope.update));
        }
    }
    anyhow::bail!("customer log subscription closed")
}
