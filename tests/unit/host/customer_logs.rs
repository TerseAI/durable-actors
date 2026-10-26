use super::*;
use std::sync::Mutex;

#[test]
fn structured_logs_preserve_values_and_only_explicit_request_ids() -> Result<()> {
    let log = parse_log(
        br#"{"message":"started","severity":"WARNING","requestId":"request-1","count":3,"data":{"ready":true}}"#,
        false, LogStream::Stdout, 42,
    );
    log.validate()?;
    assert_eq!(log.timestamp_ms, 42);
    assert_eq!(log.message, "started");
    assert_eq!(log.level, "WARN");
    assert_eq!(log.request_id.as_deref(), Some("request-1"));
    assert_eq!(log.fields["count"], 3);
    assert_eq!(log.fields["data"]["ready"], true);
    let plain = parse_log(b"failure\r", false, LogStream::Stderr, 43);
    assert_eq!(plain.message, "failure");
    assert_eq!(plain.level, "ERROR");
    assert_eq!(plain.fields, serde_json::json!({}));
    assert_eq!(plain.request_id, None);
    Ok(())
}

#[tokio::test]
async fn startup_logs_are_buffered_until_assignment_and_exported_while_executor_runs() -> Result<()>
{
    let (mut executor, reader) = tokio::io::duplex(1024);
    let (console, mut output) = tokio::io::duplex(1024);
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    capture.capture_stream(reader, console, LogStream::Stdout);
    executor.write_all(b"module loaded\n").await?;
    wait_for(|| capture.receiver.as_ref().unwrap().len() == 1).await;
    let (sink, mut reports) = channel_sink();
    capture.start(sink);
    let (logs, lost) = tokio::time::timeout(Duration::from_secs(1), reports.recv())
        .await?
        .unwrap();
    assert_eq!(lost, 0);
    assert_eq!(logs[0].message, "module loaded");
    assert_eq!(logs[0].request_id, None);
    executor.write_all(b"background output").await?;
    executor.shutdown().await?;
    capture.finish().await;
    let (logs, _) = reports.recv().await.unwrap();
    assert_eq!(logs[0].message, "background output");
    let mut forwarded = String::new();
    output.read_to_string(&mut forwarded).await?;
    assert_eq!(forwarded, "module loaded\nbackground output");
    Ok(())
}

#[tokio::test]
async fn an_unavailable_collector_does_not_block_output_and_overflow_is_reported() -> Result<()> {
    let (mut executor, reader) = tokio::io::duplex(1024);
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    capture.capture_stream(reader, tokio::io::sink(), LogStream::Stdout);
    let count = LOG_QUEUE_SIZE + 20;
    tokio::time::timeout(
        Duration::from_secs(1),
        executor.write_all("line\n".repeat(count).as_bytes()),
    )
    .await??;
    executor.shutdown().await?;
    wait_for(|| capture.dropped.load(Ordering::Relaxed) == 20).await;
    assert_eq!(capture.receiver.as_ref().unwrap().len(), LOG_QUEUE_SIZE);
    let (sink, mut reports) = channel_sink();
    capture.start(sink);
    capture.finish().await;
    let mut exported = 0;
    let mut dropped = 0;
    while let Some((logs, lost)) = reports.recv().await {
        assert!(logs.len() <= LOG_BATCH_SIZE);
        exported += logs.len();
        dropped += lost;
    }
    assert_eq!(exported, LOG_QUEUE_SIZE);
    assert_eq!(dropped, 20);
    Ok(())
}

#[tokio::test]
async fn oversized_lines_are_bounded_without_corrupting_following_lines() -> Result<()> {
    let (mut executor, reader) = tokio::io::duplex(1024);
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    capture.capture_stream(reader, tokio::io::sink(), LogStream::Stderr);
    let (sink, mut reports) = channel_sink();
    capture.start(sink);
    executor
        .write_all("é".repeat(MAX_LOG_BYTES).as_bytes())
        .await?;
    executor.write_all(b"\nnext\n").await?;
    executor.shutdown().await?;
    capture.finish().await;
    let mut entries = Vec::new();
    while let Some((logs, _)) = reports.recv().await {
        entries.extend(logs);
    }
    assert_eq!(entries.len(), 2);
    entries[0].validate()?;
    assert_eq!(entries[0].message.len(), MAX_LOG_BYTES);
    assert_eq!(entries[0].fields["truncated"], true);
    assert_eq!(entries[1].message, "next");
    Ok(())
}

#[tokio::test]
async fn retries_keep_event_ids_stable() {
    let sink = Arc::new(RetrySink::default());
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    let log = parse_log(b"retry me", false, LogStream::Stdout, 42);
    capture
        .sender
        .as_ref()
        .unwrap()
        .try_send(log.clone())
        .unwrap();
    capture.start(sink.clone());
    capture.finish().await;
    let attempts = sink.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    assert_eq!(attempts[0].1, vec![log]);
}

#[tokio::test]
async fn a_stalled_report_does_not_backpressure_the_executor() -> Result<()> {
    let (mut executor, reader) = tokio::io::duplex(1024);
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    capture.capture_stream(reader, tokio::io::sink(), LogStream::Stdout);
    capture.start(Arc::new(StalledSink));
    tokio::time::timeout(
        Duration::from_millis(200),
        executor.write_all("line\n".repeat(2000).as_bytes()),
    )
    .await??;
    wait_for(|| capture.dropped.load(Ordering::Relaxed) > 0).await;
    Ok(())
}

#[test]
fn structured_levels_override_stream_defaults() -> Result<()> {
    for (value, expected) in [
        (serde_json::json!({"level": "info"}), "INFO"),
        (serde_json::json!({"severity": "CRITICAL"}), "FATAL"),
        (serde_json::json!({"level": 40}), "WARN"),
        (serde_json::json!({"severity": 400}), "WARN"),
        (
            serde_json::json!({"severity": null, "level": "debug"}),
            "DEBUG",
        ),
    ] {
        let log = parse_log(&serde_json::to_vec(&value)?, false, LogStream::Stderr, 42);
        log.validate()?;
        assert_eq!(log.level, expected);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_is_bounded_when_reporting_never_finishes() {
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    capture
        .sender
        .as_ref()
        .unwrap()
        .try_send(parse_log(b"pending", false, LogStream::Stdout, 42))
        .unwrap();
    capture.start(Arc::new(StalledSink));
    let started = tokio::time::Instant::now();
    capture.finish().await;
    assert!(started.elapsed() <= SHUTDOWN_TIMEOUT);
}

#[tokio::test]
async fn dropped_only_reports_keep_identity_and_count_across_retries() {
    let sink = Arc::new(RetrySink::default());
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    capture.dropped.store(7, Ordering::Relaxed);
    capture.start(sink.clone());
    capture.finish().await;
    let attempts = sink.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    assert!(attempts[0].1.is_empty());
    assert_eq!(attempts[0].2, 7);
}

#[tokio::test(start_paused = true)]
async fn lost_acknowledgements_keep_the_pending_report_separate_from_new_logs_and_drops() {
    let (attempts, mut received) = mpsc::unbounded_channel();
    let sink = Arc::new(LostAcknowledgements {
        attempts,
        remaining: AtomicU64::new(3),
    });
    let mut capture = CustomerLogs::new(Arc::new(FixedClock));
    let first = parse_log(b"first", false, LogStream::Stdout, 42);
    capture
        .sender
        .as_ref()
        .unwrap()
        .try_send(first.clone())
        .unwrap();
    capture.start(sink);
    let pending = received.recv().await.unwrap();
    let next = parse_log(b"next", false, LogStream::Stdout, 43);
    capture
        .sender
        .as_ref()
        .unwrap()
        .try_send(next.clone())
        .unwrap();
    capture.dropped.store(7, Ordering::Relaxed);
    capture.finish().await;
    assert_eq!(pending.1, vec![first]);
    assert_eq!(pending.2, 0);
    for _ in 0..3 {
        assert_eq!(received.recv().await.unwrap(), pending);
    }
    let mut following_logs = Vec::new();
    let mut following_dropped = 0;
    let mut ids = std::collections::HashSet::new();
    while let Some((id, logs, dropped)) = received.recv().await {
        assert_ne!(id, pending.0);
        assert!(ids.insert(id));
        following_logs.extend(logs);
        following_dropped += dropped;
    }
    assert_eq!(following_logs, vec![next]);
    assert_eq!(following_dropped, 7);
}

struct LostAcknowledgements {
    attempts: mpsc::UnboundedSender<(String, Vec<CustomerLog>, u64)>,
    remaining: AtomicU64,
}

#[async_trait]
impl LogSink for LostAcknowledgements {
    async fn report(&self, report_id: &str, logs: Vec<CustomerLog>, dropped: u64) -> Result<()> {
        self.attempts.send((report_id.into(), logs, dropped))?;
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining > 0 {
            self.remaining.store(remaining - 1, Ordering::Relaxed);
            anyhow::bail!("acknowledgement lost");
        }
        Ok(())
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(42)
    }
}

struct ChannelSink(mpsc::UnboundedSender<(Vec<CustomerLog>, u64)>);

#[async_trait]
impl LogSink for ChannelSink {
    async fn report(&self, _: &str, logs: Vec<CustomerLog>, dropped: u64) -> Result<()> {
        self.0.send((logs, dropped))?;
        Ok(())
    }
}

fn channel_sink() -> (
    Arc<dyn LogSink>,
    mpsc::UnboundedReceiver<(Vec<CustomerLog>, u64)>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (Arc::new(ChannelSink(sender)), receiver)
}

#[derive(Default)]
struct RetrySink {
    attempts: Mutex<Vec<(String, Vec<CustomerLog>, u64)>>,
}

#[async_trait]
impl LogSink for RetrySink {
    async fn report(&self, report_id: &str, logs: Vec<CustomerLog>, dropped: u64) -> Result<()> {
        let mut attempts = self.attempts.lock().unwrap();
        attempts.push((report_id.into(), logs, dropped));
        anyhow::ensure!(attempts.len() > 1, "unavailable");
        Ok(())
    }
}

struct StalledSink;

#[async_trait]
impl LogSink for StalledSink {
    async fn report(&self, _: &str, _: Vec<CustomerLog>, _: u64) -> Result<()> {
        std::future::pending().await
    }
}

async fn wait_for(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
