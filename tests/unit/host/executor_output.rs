use super::*;
use std::process::Stdio;

#[tokio::test]
async fn captures_both_streams_in_one_sequence() -> anyhow::Result<()> {
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", "echo out; echo err >&2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut output = ExecutorOutput::capture(&mut child);
    child.wait().await?;
    let mut lines = Vec::new();
    while let Some(line) = output.lines.recv().await {
        lines.push(line);
    }
    lines.sort_by_key(|line| line.sequence);
    let sequences: Vec<_> = lines.iter().map(|line| line.sequence).collect();
    assert_eq!(sequences, [0, 1]);
    let captured: Vec<_> = lines
        .iter()
        .map(|line| (line.stream, line.message.as_str()))
        .collect();
    assert!(captured.contains(&(LogStream::Stdout, "out")));
    assert!(captured.contains(&(LogStream::Stderr, "err")));
    Ok(())
}

#[test]
fn invalid_bytes_stay_within_the_line_limit() {
    let text = message(&vec![0xff; LOG_LINE_LIMIT]);
    assert!(text.len() <= LOG_LINE_LIMIT);
    assert!(text.starts_with('\u{fffd}'));
}
