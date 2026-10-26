#[path = "support/local_project.rs"]
mod local_project;

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn local_requests_are_concise_and_human_readable_by_default() -> Result<()> {
    let runtime = LocalRuntime::start(None).await?;
    let client = reqwest::Client::new();
    for (method, path, status) in [
        (reqwest::Method::GET, "/healthz", 200),
        (
            reqwest::Method::POST,
            "/v1/projects/default/actors/Counter/one/find-actor",
            401,
        ),
        (
            reqwest::Method::GET,
            "/v1/projects/default/observe/actors",
            200,
        ),
        (reqwest::Method::GET, "/missing", 404),
    ] {
        let response = client
            .request(
                method,
                format!("{}{path}?token=query-secret", runtime.origin),
            )
            .bearer_auth("header-secret")
            .json(&serde_json::json!({
                "homeRegion": "body-secret"
            }))
            .send()
            .await?;
        assert_eq!(response.status().as_u16(), status, "{path}");
    }
    let output = runtime.stop().await?;
    let requests = request_logs(&output);
    assert_eq!(requests.len(), 4, "missing terminal request logs: {output}");
    for (log, (method, path, status)) in requests.iter().zip([
        ("GET", "/healthz", 200),
        (
            "POST",
            "/v1/projects/default/actors/Counter/one/find-actor",
            401,
        ),
        ("GET", "/v1/projects/default/observe/actors", 200),
        ("GET", "/missing", 404),
    ]) {
        assert!(log.contains("INFO "), "missing level: {log}");
        assert!(
            log.contains(&format!("method={method}")),
            "missing method: {log}"
        );
        assert!(log.contains(&format!("path={path}")), "missing path: {log}");
        assert!(
            log.contains(&format!("status={status}")),
            "missing status: {log}"
        );
        assert!(log.contains("latency_ms="), "missing latency: {log}");
        assert!(
            !log.trim_start().starts_with('{'),
            "development log is JSON: {log}"
        );
    }
    for secret in ["query-secret", "header-secret", "body-secret"] {
        assert!(!output.contains(secret), "request log leaked {secret}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn local_request_logs_respect_rust_log() -> Result<()> {
    let runtime = LocalRuntime::start(Some("warn")).await?;
    reqwest::get(format!("{}/healthz", runtime.origin))
        .await?
        .error_for_status()?;
    let output = runtime.stop().await?;
    assert!(request_logs(&output).is_empty(), "{output}");
    Ok(())
}

#[tokio::test]
async fn service_process_logs_remain_structured() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_durable-actors"))
        .env("DURABLE_ACTORS_PROCESS_ROLE", "invalid")
        .env_remove("DURABLE_ACTORS_LOG_MODE")
        .env_remove("RUST_LOG")
        .output()
        .await?;
    assert!(!output.status.success());
    let log: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(log["level"], "ERROR");
    assert_eq!(log["message"], "durable-actors process failed");
    assert!(log["error"].as_str().unwrap().contains("unsupported"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires bun run --bun --cwd sdk build and Bun"]
async fn customer_logs_arrive_before_request_completion_and_survive_failure() -> Result<()> {
    let runtime = LocalRuntime::start_with_actor(None, r#"
        async hold(gate: string): Promise<void> {
            console.log(JSON.stringify({ message: "customer-started", level: "warn",
                requestId: "customer-request", count: 42, nested: { ready: true }, projectId: "spoofed" }));
            console.error("customer-stderr");
            await fetch(gate);
            throw new Error("customer-failed");
        }
    "#).await?;
    let result = check_customer_logs(&runtime.origin).await;
    let output = runtime.stop().await?;
    result?;
    ensure!(
        output.contains("customer-started"),
        "customer stdout was not forwarded"
    );
    Ok(())
}

async fn check_customer_logs(origin: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let logs = format!("{origin}/v1/projects/default/observe/logs");
    let mut live = client
        .get(format!("{logs}/events"))
        .send()
        .await?
        .error_for_status()?;
    let gate = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let request = client
        .post(format!(
            "{origin}/v1/projects/default/actors/Counter/one/invoke"
        ))
        .bearer_auth("test-key")
        .json(
            &serde_json::json!({"requestId": "customer-request", "method": "hold",
            "args": [format!("http://{}", gate.local_addr()?)]}),
        );
    let invocation = tokio::spawn(request.send());
    let (mut gate, _) = timeout(Duration::from_secs(10), gate.accept()).await??;
    let record = timeout(
        Duration::from_secs(5),
        read_customer_log(&mut live, "customer-started"),
    )
    .await??;
    ensure!(
        !invocation.is_finished(),
        "logs waited for invocation completion"
    );
    assert_eq!(record["projectId"], "default");
    assert_eq!(record["actorName"], "Counter");
    assert_eq!(record["actorId"], "one");
    assert_eq!(record["level"], "WARN");
    assert_eq!(record["fields"]["count"], 42);
    assert_eq!(record["fields"]["nested"]["ready"], true);
    assert_eq!(record["fields"]["projectId"], "spoofed");
    assert!(record.get("stateVersion").is_none());
    let history = timeout(
        Duration::from_secs(5),
        wait_for_log_history(&client, &logs, "customer-stderr"),
    )
    .await??;
    let error = history["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["message"] == "customer-stderr")
        .unwrap();
    assert_eq!(error["stream"], "stderr");
    assert!(
        error
            .get("requestId")
            .is_none_or(serde_json::Value::is_null)
    );
    ensure!(
        !invocation.is_finished(),
        "history waited for invocation completion"
    );
    gate.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await?;
    drop(gate);
    let response: serde_json::Value = timeout(Duration::from_secs(5), invocation)
        .await???
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(response["outcome"]["type"], "failed");
    let history = timeout(
        Duration::from_secs(5),
        wait_for_log_history(&client, &logs, "customer-started"),
    )
    .await??;
    assert!(
        history["records"]
            .as_array()
            .unwrap()
            .iter()
            .any(|saved| saved["eventId"] == record["eventId"])
    );
    Ok(())
}

async fn read_customer_log(
    response: &mut reqwest::Response,
    message: &str,
) -> Result<serde_json::Value> {
    let mut pending = String::new();
    while let Some(chunk) = response.chunk().await? {
        pending.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(end) = pending.find('\n') {
            let line = pending[..end].to_owned();
            pending.drain(..=end);
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let page: serde_json::Value = serde_json::from_str(data.trim())?;
            if let Some(record) = page["records"]
                .as_array()
                .and_then(|records| records.iter().find(|record| record["message"] == message))
            {
                return Ok(record.clone());
            }
        }
    }
    anyhow::bail!("log stream ended before {message}")
}

async fn wait_for_log_history(
    client: &reqwest::Client,
    url: &str,
    message: &str,
) -> Result<serde_json::Value> {
    loop {
        let page: serde_json::Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if page["records"]
            .as_array()
            .is_some_and(|records| records.iter().any(|record| record["message"] == message))
        {
            return Ok(page);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn request_logs(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter(|line| line.contains("request completed"))
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
        Self::start_with_actor(filter, "async read(): Promise<number> { return 1 }").await
    }

    async fn start_with_actor(filter: Option<&str>, actor: &str) -> Result<Self> {
        let project = tempfile::tempdir()?;
        local_project::write_actor(project.path(), actor)?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_durable-actors"));
        command
            .args([
                "dev",
                "--project-id",
                "default",
                "--port",
                "0",
                "--entrypoint",
                "actors.ts",
            ])
            .arg("--sdk-host")
            .arg(local_project::sdk_host())
            .arg("--project")
            .arg(project.path())
            .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
            .env("DURABLE_ACTORS_SECRET", "test-key")
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(filter) = filter {
            command.env("RUST_LOG", filter);
        }
        let mut child = command.spawn()?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        let origin = timeout(Duration::from_secs(20), wait_until_ready(&mut output)).await??;
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

async fn wait_until_ready(output: &mut BufReader<ChildStdout>) -> Result<String> {
    let mut line = String::new();
    loop {
        ensure!(
            output.read_line(&mut line).await? != 0,
            "runtime exited before readiness: {line}"
        );
        if let Some((_, origin)) = line.split_once("  Ready  ") {
            return Ok(origin.trim().to_owned());
        }
        line.clear();
    }
}
