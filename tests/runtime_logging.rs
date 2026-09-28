#[path = "support/local_project.rs"]
mod local_project;

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
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
#[ignore = "requires pnpm --dir sdk build and Bun"]
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
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn project_exports_capture_actor_console_without_copying_to_runtime_output() -> Result<()> {
    use axum::{Router, body::Bytes, http::HeaderMap, routing::post};
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/v1/logs", listener.local_addr()?);
    let records = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = records.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/v1/logs",
                post(move |headers: HeaderMap, body: Bytes| {
                    let captured = captured.clone();
                    async move {
                        assert_eq!(headers["authorization"], "Bearer log-secret");
                        assert_eq!(headers["content-type"], "application/x-protobuf");
                        captured
                            .lock()
                            .unwrap()
                            .push(ExportLogsServiceRequest::decode(body).unwrap());
                        (
                            [("content-type", "application/x-protobuf")],
                            Vec::<u8>::new(),
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let runtime = LocalRuntime::start_with_actor(Some("info"), r#"async read(): Promise<number> { console.log("customer-stdout"); console.error("customer-stderr"); process.stdout.write("customer-partial"); return 1 }"#).await?;
    let client = reqwest::Client::new();
    let deployment_url = format!("{}/v1/projects/default/deployment", runtime.origin);
    let mut deployment: serde_json::Value = client
        .get(&deployment_url)
        .bearer_auth("test-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    deployment["logExport"] =
        serde_json::json!({"endpoint":endpoint,"headersEnv":"TEST_LOG_HEADERS"});
    let registered = client
        .put(&deployment_url)
        .bearer_auth("test-key")
        .json(&deployment)
        .send()
        .await?;
    assert!(
        registered.status().is_success(),
        "{}",
        registered.text().await?
    );
    let response = client
        .post(format!(
            "{}/v1/projects/default/actors/Counter/one/invoke",
            runtime.origin
        ))
        .bearer_auth("test-key")
        .json(&serde_json::json!({"method":"read","args":[],"requestId":"request-1"}))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    assert!(status.is_success(), "{status}: {body}");
    let output = runtime.stop().await?;
    let exported = format!("{:?}", records.lock().unwrap());
    for marker in ["customer-stdout", "customer-stderr", "customer-partial"] {
        assert!(exported.contains(marker), "missing {marker}: {exported}");
        assert!(!output.contains(marker), "runtime output leaked {marker}");
    }
    for marker in [
        "durable-actors host stopped",
        "durable_actors.project_id",
        "durable_actors.actor_id",
        "Counter",
        "cloud.region",
    ] {
        assert!(exported.contains(marker), "missing {marker}: {exported}");
    }
    assert!(!exported.contains("durable_actors::control_plane"));
    server.abort();
    Ok(())
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
    errors: tokio::process::ChildStderr,
    origin: String,
}

impl LocalRuntime {
    async fn start(filter: Option<&str>) -> Result<Self> {
        Self::start_with_actor(filter, "async read(): Promise<number> { return 1 }").await
    }

    async fn start_with_actor(filter: Option<&str>, body: &str) -> Result<Self> {
        let project = tempfile::tempdir()?;
        local_project::write_actor(project.path(), body)?;
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
            .env(
                "TEST_LOG_HEADERS",
                r#"{"authorization":"Bearer log-secret"}"#,
            )
            .env(
                "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
                "authorization=Bearer wrong-secret",
            )
            .env(
                "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
                "http://127.0.0.1:1/wrong",
            )
            .env_remove("RUST_LOG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(filter) = filter {
            command.env("RUST_LOG", filter);
        }
        let mut child = command.spawn()?;
        let errors = child.stderr.take().context("capture runtime errors")?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        let origin = timeout(Duration::from_secs(20), wait_until_ready(&mut output)).await??;
        Ok(Self {
            errors,
            _project: project,
            child,
            output,
            origin,
        })
    }

    async fn stop(mut self) -> Result<String> {
        drop(self.child.stdin.take());
        let mut output = String::new();
        let mut errors = String::new();
        let (status, _, _) = timeout(Duration::from_secs(10), async {
            tokio::try_join!(
                self.child.wait(),
                self.output.read_to_string(&mut output),
                self.errors.read_to_string(&mut errors)
            )
        })
        .await??;
        ensure!(status.success(), "runtime exited with {status}: {output}");
        output.push_str(&errors);
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
