use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

#[tokio::test]
async fn local_requests_are_logged_in_the_terminal_by_default() -> Result<()> {
    let runtime = LocalRuntime::start(None).await?;
    let client = reqwest::Client::new();
    for (method, path, status) in [
        (reqwest::Method::GET, "/.well-known/jwks.json", 200),
        (reqwest::Method::POST, "/v1/session-scoped-token", 401),
        (reqwest::Method::GET, "/v1/objects", 400),
        (reqwest::Method::GET, "/missing", 404),
    ] {
        let response = client
            .request(
                method,
                format!("{}{path}?token=query-secret", runtime.origin),
            )
            .bearer_auth("header-secret")
            .json(&serde_json::json!({
                "executionId": "body-secret",
                "deadlineUnixMs": 1_900_000_000_000_i64,
                "storageRegion": "north-america-east"
            }))
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), status, "{path}");
    }
    let output = runtime.stop().await?;
    let requests = request_logs(&output);
    assert_eq!(requests.len(), 4, "missing terminal request logs: {output}");
    for (log, (method, path, status)) in requests.iter().zip([
        ("GET", "/.well-known/jwks.json", 200),
        ("POST", "/v1/session-scoped-token", 401),
        ("GET", "/v1/objects", 400),
        ("GET", "/missing", 404),
    ]) {
        assert_eq!(log["level"], "INFO");
        assert_eq!(log["span"]["method"], method);
        assert_eq!(log["span"]["path"], path);
        assert_eq!(log["status"], status);
        assert!(log["latency"].is_string());
    }
    for secret in ["query-secret", "header-secret", "body-secret"] {
        assert!(!output.contains(secret), "request log leaked {secret}");
    }
    Ok(())
}

#[tokio::test]
async fn local_request_logs_respect_rust_log() -> Result<()> {
    let runtime = LocalRuntime::start(Some("warn")).await?;
    reqwest::get(format!("{}/.well-known/jwks.json", runtime.origin))
        .await?
        .error_for_status()?;
    let output = runtime.stop().await?;
    assert!(request_logs(&output).is_empty(), "{output}");
    Ok(())
}

fn request_logs(output: &str) -> Vec<Value> {
    output
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|log| log["message"] == "finished processing request")
        .collect()
}

struct LocalRuntime {
    _project: tempfile::TempDir,
    child: Child,
    output: BufReader<ChildStdout>,
    origin: String,
}

impl LocalRuntime {
    async fn start(filter: Option<&str>) -> Result<Self> {
        let project = tempfile::tempdir()?;
        std::fs::write(project.path().join("actors.ts"), "export {}\n")?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_little-actors"));
        command
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .arg("--project")
            .arg(project.path())
            .env("DURABLE_OBJECT_PARENT_LIFETIME_STDIN", "1")
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(filter) = filter {
            command.env("RUST_LOG", filter);
        }
        let mut child = command.spawn()?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        timeout(Duration::from_secs(5), wait_until_ready(&mut output)).await??;
        let connection: Value = serde_json::from_slice(&std::fs::read(
            project.path().join(".little-actors/runtime.json"),
        )?)?;
        let origin = connection["controlPlaneUrl"]
            .as_str()
            .context("runtime origin is missing")?
            .to_owned();
        Ok(Self {
            _project: project,
            child,
            output,
            origin,
        })
    }

    async fn stop(mut self) -> Result<String> {
        drop(self.child.stdin.take());
        let mut output = String::new();
        let (status, _) = timeout(Duration::from_secs(5), async {
            tokio::try_join!(self.child.wait(), self.output.read_to_string(&mut output))
        })
        .await??;
        ensure!(status.success(), "runtime exited with {status}: {output}");
        Ok(output)
    }
}

async fn wait_until_ready(output: &mut BufReader<ChildStdout>) -> Result<()> {
    let mut line = String::new();
    loop {
        ensure!(
            output.read_line(&mut line).await? != 0,
            "runtime exited before readiness: {line}"
        );
        if line.contains("Local actors ready at") {
            return Ok(());
        }
        line.clear();
    }
}
