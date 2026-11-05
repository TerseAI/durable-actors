use std::{
    fs::File,
    io::{self, ErrorKind, Read, Write},
    os::fd::{AsFd, OwnedFd},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use rustix::{
    fs::{Mode, OFlags},
    io::{FdFlags, fcntl_setfd},
    pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt},
    termios::{
        OptionalActions, OutputModes, isatty, tcgetattr, tcgetwinsize, tcsetattr, tcsetwinsize,
    },
};
use tokio::{
    process::Command,
    signal::unix::{SignalKind, signal},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    actor_logs::{LOG_BATCH_LINES, LOG_LINE_LIMIT, LogLine, LogStream},
    batch_export::{self, BatchSink},
    clock::{Clock, SystemClock},
    control_plane::ControlPlaneClient,
};

const READ_CHUNK_BYTES: usize = 8192;

/// The executor's ends of its captured stdout and stderr.
pub(super) struct ExecutorStdio {
    stdout: Stdio,
    stderr: Stdio,
}

pub(super) struct ExecutorLogs(mpsc::UnboundedReceiver<LogLine>);

pub(super) struct LogExport(JoinHandle<()>);

pub(super) struct LogReporter(pub Arc<ControlPlaneClient>);

pub(super) trait Terminal: Write + AsFd + Send + 'static {}

impl<T: Write + AsFd + Send + 'static> Terminal for T {}

pub(super) fn capture_terminal() -> io::Result<(ExecutorStdio, ExecutorLogs)> {
    capture(Arc::new(SystemClock), io::stdout(), io::stderr())
}

/// Executor output is copied to `stdout` and `stderr` as it arrives and collected as log lines.
/// A stream that reaches a terminal gets a pseudo-terminal, so the executor still sees a TTY.
pub(super) fn capture(
    clock: Arc<dyn Clock>,
    stdout: impl Terminal,
    stderr: impl Terminal,
) -> io::Result<(ExecutorStdio, ExecutorLogs)> {
    let (sender, lines) = mpsc::unbounded_channel();
    let tap = OutputTap {
        sender,
        stamps: Arc::new(LineStamps::new(clock)),
    };
    let stdio = ExecutorStdio {
        stdout: tap.clone().start(stdout, LogStream::Stdout)?,
        stderr: tap.start(stderr, LogStream::Stderr)?,
    };
    Ok((stdio, ExecutorLogs(lines)))
}

impl ExecutorStdio {
    pub(super) fn attach(self, command: &mut Command) {
        command.stdout(self.stdout).stderr(self.stderr);
    }
}

impl ExecutorLogs {
    pub(super) fn export(self, sink: impl BatchSink<LogLine> + 'static) -> LogExport {
        let lines = UnboundedReceiverStream::new(self.0);
        LogExport(tokio::spawn(batch_export::export(
            lines,
            LOG_BATCH_LINES,
            sink,
        )))
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

#[async_trait]
impl BatchSink<LogLine> for LogReporter {
    async fn report(&self, lines: Vec<LogLine>) {
        // Appends are idempotent, so a batch is retried until it is stored.
        let retry = ExponentialBuilder::default()
            .with_max_delay(Duration::from_secs(5))
            .without_max_times();
        let _ = (|| self.0.report_logs(lines.clone())).retry(retry).await;
    }
}

#[derive(Clone)]
struct OutputTap {
    sender: mpsc::UnboundedSender<LogLine>,
    stamps: Arc<LineStamps>,
}

impl OutputTap {
    fn start(self, terminal: impl Terminal, stream: LogStream) -> io::Result<Stdio> {
        let (source, executor_end) = output_channel(&terminal)?;
        std::thread::spawn(move || self.forward(source, terminal, stream));
        Ok(executor_end)
    }

    fn forward(self, mut source: File, mut terminal: impl Write, stream: LogStream) {
        let mut chunk = vec![0; READ_CHUNK_BYTES];
        let mut lines = LineBuffer::default();
        loop {
            let read = match source.read(&mut chunk) {
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                // A pseudo-terminal reports EIO instead of EOF once the executor closes it.
                Ok(0) | Err(_) => break,
                Ok(read) => &chunk[..read],
            };
            let _ = terminal.write_all(read).and_then(|()| terminal.flush());
            lines.push(read, |line| self.publish(stream, line));
        }
        lines.finish(|line| self.publish(stream, line));
    }

    fn publish(&self, stream: LogStream, line: &[u8]) {
        let _ = self.sender.send(self.stamps.next(stream, message(line)));
    }
}

struct LineStamps {
    clock: Arc<dyn Clock>,
    next: Mutex<(u64, u64)>,
}

impl LineStamps {
    fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            next: Mutex::new((0, 0)),
        }
    }

    /// Orders both streams in one sequence whose timestamps never go backwards.
    fn next(&self, stream: LogStream, message: String) -> LogLine {
        let mut next = self.next.lock().unwrap();
        let (sequence, last_ms) = *next;
        let timestamp_ms = self.clock.now_ms().unwrap_or_default().max(last_ms);
        *next = (sequence + 1, timestamp_ms);
        LogLine {
            sequence,
            timestamp_ms,
            stream,
            message,
        }
    }
}

#[derive(Default)]
struct LineBuffer(Vec<u8>);

impl LineBuffer {
    fn push(&mut self, bytes: &[u8], mut emit: impl FnMut(&[u8])) {
        self.0.extend_from_slice(bytes);
        let mut start = 0;
        while let Some(length) = record_length(&self.0[start..]) {
            emit(&self.0[start..start + length]);
            start += length;
        }
        self.0.drain(..start);
    }

    fn finish(self, mut emit: impl FnMut(&[u8])) {
        if !self.0.is_empty() {
            emit(&self.0);
        }
    }
}

/// A record is one line, or the longest prefix within the limit that ends on a character.
fn record_length(bytes: &[u8]) -> Option<usize> {
    let newline = bytes
        .iter()
        .take(LOG_LINE_LIMIT + 1)
        .position(|byte| *byte == b'\n');
    match newline {
        Some(newline) => Some(newline + 1),
        None if bytes.len() > LOG_LINE_LIMIT => Some(char_boundary(bytes, LOG_LINE_LIMIT)),
        None => None,
    }
}

fn char_boundary(bytes: &[u8], limit: usize) -> usize {
    let continues_character = |byte: u8| byte & 0b1100_0000 == 0b1000_0000;
    (limit - 3..=limit)
        .rev()
        .find(|&end| !continues_character(bytes[end]))
        .unwrap_or(limit)
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

fn output_channel(terminal: &impl AsFd) -> io::Result<(File, Stdio)> {
    if isatty(terminal) {
        pseudo_terminal(terminal)
    } else {
        let (reader, writer) = io::pipe()?;
        Ok((File::from(OwnedFd::from(reader)), writer.into()))
    }
}

fn pseudo_terminal(terminal: &impl AsFd) -> io::Result<(File, Stdio)> {
    let (controller, user) = open_pseudo_terminal()?;
    let mut settings = tcgetattr(terminal)?;
    // The real terminal still applies its own output processing to the copied bytes.
    settings.output_modes.remove(OutputModes::OPOST);
    tcsetattr(&user, OptionalActions::Now, &settings)?;
    tcsetwinsize(&user, tcgetwinsize(terminal)?)?;
    tokio::spawn(follow_resizes(
        terminal.as_fd().try_clone_to_owned()?,
        controller.try_clone()?,
    ));
    Ok((File::from(controller), user.into()))
}

fn open_pseudo_terminal() -> io::Result<(OwnedFd, OwnedFd)> {
    let controller = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY)?;
    fcntl_setfd(&controller, FdFlags::CLOEXEC)?;
    grantpt(&controller)?;
    unlockpt(&controller)?;
    let user = rustix::fs::open(
        ptsname(&controller, Vec::new())?.as_c_str(),
        OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok((controller, user))
}

async fn follow_resizes(terminal: OwnedFd, pseudo_terminal: OwnedFd) {
    let Ok(mut resized) = signal(SignalKind::window_change()) else {
        return;
    };
    while resized.recv().await.is_some() {
        if let Ok(size) = tcgetwinsize(&terminal) {
            let _ = tcsetwinsize(&pseudo_terminal, size);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/executor_output.rs"]
mod tests;
