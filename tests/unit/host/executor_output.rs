use super::*;

#[tokio::test]
async fn captures_output_and_keeps_the_terminal() -> anyhow::Result<()> {
    let (_controller, terminal) = open_pseudo_terminal()?;
    let (mut copied, stderr) = io::pipe()?;
    let (stdio, mut logs) = capture(Arc::new(SystemClock), File::from(terminal), stderr)?;
    let mut command = Command::new("sh");
    command.args(["-c", "[ -t 1 ] && echo terminal; printf partial >&2"]);
    stdio.attach(&mut command);
    command.status().await?;
    drop(command);
    let mut lines = Vec::new();
    while let Some(line) = logs.0.recv().await {
        lines.push((line.stream, line.message));
    }
    lines.sort_by(|left, right| left.1.cmp(&right.1));
    assert_eq!(
        lines,
        [
            (LogStream::Stderr, "partial".into()),
            (LogStream::Stdout, "terminal".into())
        ]
    );
    let mut text = String::new();
    copied.read_to_string(&mut text)?;
    assert_eq!(text, "partial");
    Ok(())
}
