use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::Child,
    sync::mpsc,
    task::JoinHandle,
};

use crate::{
    actor_logs::{LOG_BATCH_LINES, LOG_LINE_LIMIT, LogLine, LogStream},
    clock::{Clock, SystemClock},
    control_plane::ControlPlaneClient,
};

const BUFFERED_LINES: usize = 4096;
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const REPORT_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) struct ExecutorOutput {
    lines: mpsc::Receiver<LogLine>,
    counters: Arc<OutputCounters>,
}

pub(super) struct LogExport(JoinHandle<()>);

impl ExecutorOutput {
    pub(super) fn capture(child: &mut Child) -> Self {
        let (sender, lines) = mpsc::channel(BUFFERED_LINES);
        let counters = Arc::new(OutputCounters::default());
        let tap = OutputTap {
            sender,
            counters: counters.clone(),
        };
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(
                tap.clone()
                    .forward(stdout, tokio::io::stdout(), LogStream::Stdout),
            );
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(tap.forward(stderr, tokio::io::stderr(), LogStream::Stderr));
        }
        Self { lines, counters }
    }

    pub(super) fn export(self, client: Arc<ControlPlaneClient>) -> LogExport {
        LogExport(tokio::spawn(self.run(client)))
    }

    async fn run(mut self, client: Arc<ControlPlaneClient>) {
        let mut batch = Vec::with_capacity(LOG_BATCH_LINES);
        while self.lines.recv_many(&mut batch, LOG_BATCH_LINES - 1).await > 0 {
            self.mark_dropped(&mut batch);
            self.report(&client, std::mem::take(&mut batch)).await;
            tokio::time::sleep(FLUSH_INTERVAL).await;
        }
    }

    fn mark_dropped(&self, batch: &mut Vec<LogLine>) {
        let dropped = self.counters.dropped.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            let notice = format!("{dropped} log lines were dropped");
            batch.push(self.counters.next_line(LogStream::System, notice));
        }
    }

    async fn report(&self, client: &ControlPlaneClient, lines: Vec<LogLine>) {
        let count = lines.len() as u64;
        let sent = tokio::time::timeout(REPORT_TIMEOUT, client.report_logs(lines)).await;
        if !matches!(sent, Ok(Ok(()))) {
            self.counters.dropped.fetch_add(count, Ordering::Relaxed);
        }
    }
}

impl LogExport {
    pub(super) async fn finish(self, timeout: Duration) {
        let mut task = self.0;
        if tokio::time::timeout(timeout, &mut task).await.is_err() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct OutputTap {
    sender: mpsc::Sender<LogLine>,
    counters: Arc<OutputCounters>,
}

impl OutputTap {
    async fn forward(
        self,
        source: impl AsyncRead + Unpin,
        mut mirror: impl AsyncWrite + Unpin,
        stream: LogStream,
    ) {
        let mut reader = BufReader::new(source);
        let mut line = Vec::new();
        loop {
            line.clear();
            match (&mut reader)
                .take(LOG_LINE_LIMIT as u64)
                .read_until(b'\n', &mut line)
                .await
            {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            let _ = mirror.write_all(&line).await;
            self.publish(stream, &line);
        }
    }

    fn publish(&self, stream: LogStream, line: &[u8]) {
        let line = self.counters.next_line(stream, message(line));
        if self.sender.try_send(line).is_err() {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct OutputCounters {
    sequence: AtomicU64,
    dropped: AtomicU64,
}

impl OutputCounters {
    fn next_line(&self, stream: LogStream, message: String) -> LogLine {
        LogLine {
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            timestamp_ms: SystemClock.now_ms().unwrap_or_default(),
            stream,
            message,
        }
    }
}

fn message(line: &[u8]) -> String {
    let mut text = String::from_utf8_lossy(line)
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    let mut end = text.len().min(LOG_LINE_LIMIT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

#[cfg(test)]
#[path = "../../tests/unit/host/executor_output.rs"]
mod tests;
