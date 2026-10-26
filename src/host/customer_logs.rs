use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Child,
    sync::mpsc,
};
use tokio_util::task::AbortOnDropHandle;

use crate::{
    clock::Clock,
    control_plane::ControlPlaneClient,
    customer_logs::{CustomerLog, LOG_BATCH_SIZE, LogStream, MAX_LOG_BYTES},
};

const LOG_QUEUE_SIZE: usize = 256;
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
const REPORT_TIMEOUT: Duration = Duration::from_secs(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

#[async_trait]
pub(super) trait LogSink: Send + Sync {
    async fn report(&self, report_id: &str, logs: Vec<CustomerLog>, dropped: u64) -> Result<()>;
}

pub(super) struct CustomerLogs {
    clock: Arc<dyn Clock>,
    sender: Option<mpsc::Sender<CustomerLog>>,
    receiver: Option<mpsc::Receiver<CustomerLog>>,
    dropped: Arc<AtomicU64>,
    readers: Vec<AbortOnDropHandle<()>>,
    forwards: Vec<AbortOnDropHandle<()>>,
    worker: Option<AbortOnDropHandle<()>>,
}

impl CustomerLogs {
    pub(super) fn new(clock: Arc<dyn Clock>) -> Self {
        let (sender, receiver) = mpsc::channel(LOG_QUEUE_SIZE);
        Self {
            clock,
            sender: Some(sender),
            receiver: Some(receiver),
            dropped: Arc::new(AtomicU64::new(0)),
            readers: Vec::new(),
            forwards: Vec::new(),
            worker: None,
        }
    }

    pub(super) fn capture(&mut self, child: &mut Child) -> Result<()> {
        let stdout = child
            .stdout
            .take()
            .context("executor stdout was not piped")?;
        let stderr = child
            .stderr
            .take()
            .context("executor stderr was not piped")?;
        self.capture_stream(stdout, tokio::io::stdout(), LogStream::Stdout);
        self.capture_stream(stderr, tokio::io::stderr(), LogStream::Stderr);
        Ok(())
    }

    pub(super) fn start(&mut self, sink: Arc<dyn LogSink>) {
        if let Some(receiver) = self.receiver.take() {
            self.worker = Some(AbortOnDropHandle::new(tokio::spawn(report_logs(
                receiver,
                self.dropped.clone(),
                sink,
            ))));
        }
    }

    pub(super) async fn finish(mut self) {
        self.sender.take();
        let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
            for task in &mut self.readers {
                let _ = task.await;
            }
            for task in &mut self.forwards {
                let _ = task.await;
            }
            if let Some(task) = &mut self.worker {
                let _ = task.await;
            }
        })
        .await;
    }

    fn capture_stream<R, W>(&mut self, reader: R, writer: W, stream: LogStream)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (forward, output) = mpsc::channel(64);
        self.forwards
            .push(AbortOnDropHandle::new(tokio::spawn(forward_output(
                writer, output,
            ))));
        self.readers
            .push(AbortOnDropHandle::new(tokio::spawn(read_output(
                reader,
                forward,
                stream,
                self.sender.as_ref().unwrap().clone(),
                self.dropped.clone(),
                self.clock.clone(),
            ))));
    }
}

#[async_trait]
impl LogSink for ControlPlaneClient {
    async fn report(&self, report_id: &str, logs: Vec<CustomerLog>, dropped: u64) -> Result<()> {
        self.report_logs(report_id, logs, dropped).await
    }
}

async fn report_logs(
    mut receiver: mpsc::Receiver<CustomerLog>,
    dropped: Arc<AtomicU64>,
    sink: Arc<dyn LogSink>,
) {
    let mut batch = Vec::with_capacity(LOG_BATCH_SIZE);
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let remaining = LOG_BATCH_SIZE - batch.len();
        tokio::select! {
            count = receiver.recv_many(&mut batch, remaining) => {
                if count == 0 {
                    send_batch(&mut batch, &dropped, sink.as_ref()).await;
                    return;
                }
                if batch.len() < LOG_BATCH_SIZE {
                    continue;
                }
            }
            _ = interval.tick() => {}
        }
        send_batch(&mut batch, &dropped, sink.as_ref()).await;
    }
}

async fn send_batch(batch: &mut Vec<CustomerLog>, dropped: &AtomicU64, sink: &dyn LogSink) {
    let lost = dropped.swap(0, Ordering::Relaxed);
    if batch.is_empty() && lost == 0 {
        return;
    }
    let report_id = uuid::Uuid::new_v4().to_string();
    let mut delay = FLUSH_INTERVAL;
    loop {
        if matches!(
            tokio::time::timeout(REPORT_TIMEOUT, sink.report(&report_id, batch.clone(), lost))
                .await,
            Ok(Ok(()))
        ) {
            batch.clear();
            return;
        }
        if delay == FLUSH_INTERVAL {
            tracing::warn!("customer log reporting unavailable; retrying the buffered batch");
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }
}

async fn read_output<R: AsyncRead + Unpin>(
    mut reader: R,
    forward: mpsc::Sender<Vec<u8>>,
    stream: LogStream,
    sender: mpsc::Sender<CustomerLog>,
    dropped: Arc<AtomicU64>,
    clock: Arc<dyn Clock>,
) {
    let mut buffer = [0; 8192];
    let mut line = Vec::new();
    let mut truncated = false;
    loop {
        let count = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        let _ = forward.try_send(buffer[..count].to_vec());
        for &byte in &buffer[..count] {
            if byte == b'\n' {
                enqueue(&line, truncated, stream, &sender, &dropped, clock.as_ref());
                line.clear();
                truncated = false;
            } else if line.len() < MAX_LOG_BYTES {
                line.push(byte);
            } else {
                truncated = true;
            }
        }
    }
    if !line.is_empty() {
        enqueue(&line, truncated, stream, &sender, &dropped, clock.as_ref());
    }
}

async fn forward_output<W: AsyncWrite + Unpin>(mut writer: W, mut output: mpsc::Receiver<Vec<u8>>) {
    while let Some(bytes) = output.recv().await {
        if writer.write_all(&bytes).await.is_err() {
            return;
        }
    }
    let _ = writer.flush().await;
}

fn enqueue(
    line: &[u8],
    truncated: bool,
    stream: LogStream,
    sender: &mpsc::Sender<CustomerLog>,
    dropped: &AtomicU64,
    clock: &dyn Clock,
) {
    let log = parse_log(line, truncated, stream, clock.now_ms().unwrap_or_default());
    if sender.try_send(log).is_err() {
        dropped.fetch_add(1, Ordering::Relaxed);
    }
}

fn parse_log(
    line: &[u8],
    mut truncated: bool,
    stream: LogStream,
    timestamp_ms: u64,
) -> CustomerLog {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let text = String::from_utf8_lossy(line);
    let mut fields = serde_json::from_str::<Map<String, Value>>(&text).unwrap_or_default();
    let mut message = fields
        .get("message")
        .or_else(|| fields.get("msg"))
        .and_then(Value::as_str)
        .unwrap_or(&text)
        .to_owned();
    let request_id = fields
        .get("requestId")
        .or_else(|| fields.get("request_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 256)
        .map(str::to_owned);
    let level = ["severity", "level"]
        .into_iter()
        .filter_map(|name| fields.get(name))
        .find_map(log_level)
        .unwrap_or(if stream == LogStream::Stderr {
            "ERROR"
        } else {
            "INFO"
        });
    if message.len() > MAX_LOG_BYTES {
        let mut end = MAX_LOG_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        truncated = true;
    }
    if truncated {
        fields.insert("truncated".into(), true.into());
    }
    if serde_json::to_vec(&fields).is_ok_and(|bytes| bytes.len() > MAX_LOG_BYTES) {
        fields.clear();
        fields.insert("truncated".into(), true.into());
    }
    CustomerLog {
        event_id: uuid::Uuid::new_v4().to_string(),
        timestamp_ms,
        stream,
        level: level.into(),
        message,
        fields: fields.into(),
        request_id,
    }
}

fn log_level(value: &Value) -> Option<&'static str> {
    let level = value.as_str().map(str::to_ascii_uppercase);
    match level.as_deref() {
        Some("TRACE") => Some("TRACE"),
        Some("DEBUG") => Some("DEBUG"),
        Some("INFO" | "NOTICE") => Some("INFO"),
        Some("WARN" | "WARNING") => Some("WARN"),
        Some("ERROR" | "ERR") => Some("ERROR"),
        Some("FATAL" | "CRITICAL" | "ALERT" | "EMERGENCY") => Some("FATAL"),
        _ => match value.as_u64() {
            Some(10) => Some("TRACE"),
            Some(20 | 100) => Some("DEBUG"),
            Some(30 | 200 | 300) => Some("INFO"),
            Some(40 | 400) => Some("WARN"),
            Some(50 | 500) => Some("ERROR"),
            Some(60 | 600 | 700 | 800) => Some("FATAL"),
            _ => None,
        },
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/customer_logs.rs"]
mod tests;
