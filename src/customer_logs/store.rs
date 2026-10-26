use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    AttributedLog, CustomerLog, LOG_BATCH_SIZE, LogPage, LogQuery,
    live::LiveLogFeed,
    persistence::{LogPersistence, LogReport},
};
use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
};

const RECENT_CAPACITY: usize = 256;
const REPORT_CAPACITY: usize = 4096;
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub(crate) struct LogStore {
    pub live: LiveLogFeed,
    state: Arc<Mutex<State>>,
    failed: Arc<AtomicBool>,
    persistence: Option<Arc<dyn LogPersistence>>,
    sender: Option<mpsc::Sender<LogReport>>,
    stop: CancellationToken,
    writer_stop: CancellationToken,
    writer: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[derive(Default)]
struct State {
    recent: VecDeque<AttributedLog>,
    dropped: HashMap<String, (u64, Instant)>,
    reports: HashMap<String, Instant>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self {
            live: LiveLogFeed::default(),
            state: Arc::default(),
            failed: Arc::default(),
            persistence: None,
            sender: None,
            stop: CancellationToken::new(),
            writer_stop: CancellationToken::new(),
            writer: Arc::default(),
        }
    }
}

impl LogStore {
    pub(crate) fn start(
        persistence: Arc<dyn LogPersistence>,
        live: LiveLogFeed,
        stop: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(16);
        let failed = Arc::new(AtomicBool::new(false));
        let writer_stop = CancellationToken::new();
        let writer = tokio::spawn(write_reports(
            persistence.clone(),
            receiver,
            failed.clone(),
            live.clone(),
            writer_stop.clone(),
        ));
        Self {
            persistence: Some(persistence),
            sender: Some(sender),
            live,
            failed,
            state: Arc::default(),
            stop,
            writer_stop,
            writer: Arc::new(tokio::sync::Mutex::new(Some(writer))),
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.stop.cancel();
        self.writer_stop.cancel();
        if let Some(mut writer) = self.writer.lock().await.take() {
            if tokio::time::timeout(Duration::from_secs(4), &mut writer)
                .await
                .is_err()
            {
                writer.abort();
                let _ = writer.await;
            }
        }
    }

    pub(crate) async fn cancelled(&self) {
        self.stop.cancelled().await;
    }

    pub(crate) fn record_report(
        &self,
        report_id: &str,
        actor: &ActorKey,
        host: &str,
        session: &str,
        logs: Vec<CustomerLog>,
        dropped: u64,
    ) -> Result<()> {
        ensure!(
            uuid::Uuid::parse_str(report_id).is_ok(),
            "invalid log report ID"
        );
        actor.validate()?;
        ensure!(logs.len() <= LOG_BATCH_SIZE, "log batch exceeds limit");
        for log in &logs {
            log.validate()?;
        }
        let records: Vec<_> = logs
            .into_iter()
            .map(|log| AttributedLog {
                project_id: actor.project_id.clone(),
                actor_name: actor.actor_name.clone(),
                actor_id: actor.actor_id.clone(),
                host_id: host.into(),
                session_id: session.into(),
                log,
            })
            .collect();
        let report = LogReport {
            id: format!("{session}:{report_id}"),
            project: actor.project_id.clone(),
            records,
            dropped,
            received_at_ms: SystemClock.now_ms()?,
        };
        if !self.remember(&report) {
            return Ok(());
        }
        self.live.publish(&report);
        if let Some(sender) = &self.sender {
            if sender.try_send(report.clone()).is_err() {
                set_failed(&self.failed, &self.live, true);
                let gap_id = format!("{}:storage", report.id);
                self.add_dropped(&gap_id, &report.project, report.records.len() as u64);
                self.live
                    .gap(gap_id, &report.project, report.records.len() as u64);
            }
        }
        Ok(())
    }

    pub(crate) async fn history(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        query.validate()?;
        query.after(project)?;
        let mut page = match &self.persistence {
            Some(persistence) => {
                tokio::time::timeout(WRITE_TIMEOUT, persistence.history(project, query)).await??
            }
            None => self.recent(project, query)?,
        };
        let status = self.status(project);
        page.dropped = page.dropped.max(status.dropped);
        page.persistence_failed |= status.persistence_failed;
        Ok(page)
    }

    pub(crate) fn status(&self, project: &str) -> LogPage {
        LogPage {
            dropped: self
                .state
                .lock()
                .unwrap()
                .dropped
                .get(project)
                .map_or(0, |(count, _)| *count),
            persistence_failed: self.failed.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    pub(crate) fn recent(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        self.snapshot(project, query).map(|(page, _)| page)
    }

    pub(crate) fn snapshot(
        &self,
        project: &str,
        query: &LogQuery,
    ) -> Result<(LogPage, std::collections::HashSet<String>)> {
        let after = query.after(project)?;
        let state = self.state.lock().unwrap();
        let mut records: Vec<_> = state
            .recent
            .iter()
            .filter(|record| {
                record.project_id == project
                    && query.matches(record)
                    && after.as_ref().is_none_or(|cursor| {
                        (record.log.timestamp_ms, &record.log.event_id) < (cursor.time, &cursor.id)
                    })
            })
            .cloned()
            .collect();
        let seen = state.reports.keys().cloned().collect();
        let dropped = state.dropped.get(project).map_or(0, |(count, _)| *count);
        drop(state);
        records.sort_by(|a, b| {
            (b.log.timestamp_ms, &b.log.event_id).cmp(&(a.log.timestamp_ms, &a.log.event_id))
        });
        records.dedup_by(|a, b| a.log.event_id == b.log.event_id);
        let mut page = query.finish(project, records)?;
        page.dropped = dropped;
        page.persistence_failed = self.failed.load(Ordering::Relaxed);
        Ok((page, seen))
    }

    #[cfg(test)]
    pub(crate) fn record(
        &self,
        actor: &ActorKey,
        host: &str,
        session: &str,
        logs: Vec<CustomerLog>,
        dropped: u64,
    ) -> Result<()> {
        self.record_report(
            &uuid::Uuid::new_v4().to_string(),
            actor,
            host,
            session,
            logs,
            dropped,
        )
    }

    fn remember(&self, report: &LogReport) -> bool {
        let mut state = self.state.lock().unwrap();
        if !state.remember_id(&report.id) {
            return false;
        }
        state.add_dropped(&report.project, report.dropped);
        state.recent.extend(report.records.iter().cloned());
        while state.recent.len() > RECENT_CAPACITY {
            state.recent.pop_front();
        }
        true
    }

    fn add_dropped(&self, id: &str, project: &str, count: u64) {
        let mut state = self.state.lock().unwrap();
        state.remember_id(id);
        state.add_dropped(project, count);
    }
}

impl State {
    fn remember_id(&mut self, id: &str) -> bool {
        if self.reports.contains_key(id) {
            return false;
        }
        if self.reports.len() >= REPORT_CAPACITY {
            if let Some(oldest) = self
                .reports
                .iter()
                .min_by_key(|(_, at)| *at)
                .map(|(key, _)| key.clone())
            {
                self.reports.remove(&oldest);
            }
        }
        self.reports.insert(id.into(), Instant::now());
        true
    }

    fn add_dropped(&mut self, project: &str, count: u64) {
        if count == 0 {
            return;
        }
        self.dropped
            .retain(|_, (_, at)| at.elapsed() < Duration::from_secs(30 * 86400));
        if self.dropped.len() >= RECENT_CAPACITY && !self.dropped.contains_key(project) {
            if let Some(oldest) = self
                .dropped
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(key, _)| key.clone())
            {
                self.dropped.remove(&oldest);
            }
        }
        let (dropped, at) = self
            .dropped
            .entry(project.into())
            .or_insert((0, Instant::now()));
        *dropped = dropped.saturating_add(count);
        *at = Instant::now();
    }
}

async fn write_reports(
    persistence: Arc<dyn LogPersistence>,
    mut receiver: mpsc::Receiver<LogReport>,
    failed: Arc<AtomicBool>,
    live: LiveLogFeed,
    stop: CancellationToken,
) {
    let mut pending = None;
    let completed = {
        let work = async {
            retry(|| persistence.initialize(), &failed, &live).await;
            while let Some(report) = receiver.recv().await {
                pending = Some(report);
                retry(
                    || persistence.append(pending.as_ref().unwrap()),
                    &failed,
                    &live,
                )
                .await;
                pending = None;
            }
        };
        tokio::select! { _ = work => true, _ = stop.cancelled() => false }
    };
    if completed {
        return;
    }
    receiver.close();
    let drain = async {
        persistence.initialize().await?;
        if let Some(report) = pending {
            persistence.append(&report).await?;
        }
        while let Some(report) = receiver.recv().await {
            persistence.append(&report).await?;
        }
        anyhow::Ok(())
    };
    if !matches!(
        tokio::time::timeout(Duration::from_secs(3), drain).await,
        Ok(Ok(()))
    ) {
        set_failed(&failed, &live, true);
        tracing::warn!("Customer log shutdown flush did not complete");
    }
}

async fn retry<F, Fut>(mut operation: F, failed: &AtomicBool, live: &LiveLogFeed)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut delay = Duration::from_secs(1);
    loop {
        match tokio::time::timeout(WRITE_TIMEOUT, operation()).await {
            Ok(Ok(())) => {
                set_failed(failed, live, false);
                return;
            }
            _ => {
                if failed.load(Ordering::Relaxed) {
                    live.status(true);
                } else {
                    set_failed(failed, live, true);
                }
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

fn set_failed(failed: &AtomicBool, live: &LiveLogFeed, value: bool) {
    if failed.swap(value, Ordering::Relaxed) != value {
        live.status(value);
    }
}
