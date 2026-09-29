#[path = "support/local_project.rs"]
mod local_project;

use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    time::timeout,
};

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun; waits for two UTC minute boundaries"]
async fn dev_crons_wake_registered_instances_after_restart() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(
        project.path(),
        r#"
        @Persisted events: {method: string; cron: string; scheduledTime: number}[] = []
        @Cron("* * * * *") async refresh(event: CronEvent): Promise<void> {
            this.events.push({method: "refresh", ...event})
        }
        @Cron("* * * * *") async cleanup(event: CronEvent): Promise<void> {
            this.events.push({method: "cleanup", ...event})
        }
        @Cron("* * * * *") async fail(event: CronEvent): Promise<void> {
            throw new Error("expected cron handler failure")
        }
        async read() { return this.events }
    "#,
    )?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    assert!(read_cron_events(&runtime).await?.is_empty());
    let events = timeout(Duration::from_secs(90), async {
        loop {
            let events = read_cron_events(&runtime).await?;
            if events.len() >= 2
                && failed_cron_advanced(
                    project.path(),
                    events[0]["scheduledTime"].as_i64().unwrap(),
                )?
            {
                break anyhow::Ok(events);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await??;
    assert_eq!(events[0]["cron"], "* * * * *");
    assert_eq!(events[0]["scheduledTime"], events[1]["scheduledTime"]);
    assert_ne!(events[0]["method"], events[1]["method"]);
    let next = events[0]["scheduledTime"].as_u64().unwrap() + 60_000;
    runtime.stop().await?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    tokio::time::sleep(Duration::from_millis(next.saturating_sub(now) + 3_000)).await;
    let events = read_cron_events(&runtime).await?;
    assert!(
        events.len() >= 4,
        "cron wakeups were not restored: {events:?}"
    );
    assert!(
        events
            .iter()
            .filter(|event| event["scheduledTime"] == next)
            .count()
            >= 2
    );
    runtime.stop().await
}

async fn read_cron_events(runtime: &LocalRuntime) -> Result<Vec<Value>> {
    let response = reqwest::Client::new()
        .post(format!("{}/v1/projects/local/actors/Counter/one/invoke", runtime.origin))
        .json(&serde_json::json!({"requestId":uuid::Uuid::new_v4().to_string(),"method":"read","args":[]}))
        .send().await?;
    let status = response.status();
    let response: Value = response.json().await?;
    ensure!(
        status.is_success(),
        "cron read failed with {status}: {response}"
    );
    serde_json::from_value(response["outcome"]["result"].clone()).context("read cron events")
}

fn failed_cron_advanced(project: &Path, scheduled_time: i64) -> Result<bool> {
    let database = rusqlite::Connection::open(project.join(".durable-actors/crons.sqlite3"))?;
    Ok(database.query_row("SELECT next_at > ?1 AND attempts=0 AND failures=0 FROM durable_actors_crons WHERE method='fail'", [scheduled_time], |row| row.get(0))?)
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_publishes_the_compiled_contract_before_readiness_and_refreshes_it_on_restart()
-> Result<()> {
    let project = tempfile::Builder::new()
        .prefix("actor's project ")
        .tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let first: Value = runtime.contract().await?.error_for_status()?.json().await?;
    assert_eq!(first["contract"]["actors"][0]["actorName"], "Counter");
    assert_eq!(
        first["contract"]["actors"][0]["rpc"]["methods"][0]["name"],
        "read"
    );
    runtime.stop().await?;

    local_project::write_actor(
        project.path(),
        "async reset(): Promise<number> { return 0 }",
    )?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let second: Value = runtime.contract().await?.error_for_status()?.json().await?;
    assert_eq!(
        second["contract"]["actors"][0]["rpc"]["methods"][0]["name"],
        "reset"
    );
    assert_ne!(second["contractHash"], first["contractHash"]);
    runtime.stop().await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_observability_is_open_while_application_routes_enforce_the_secret() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let runtime = LocalRuntime::start(project.path(), Some("optional-secret")).await?;
    assert_eq!(runtime.contract().await?.status(), 401);
    let url = format!("{}/v1/projects/local/deployment/contract", runtime.origin);
    let client = reqwest::Client::new();
    assert_eq!(
        client.get(&url).bearer_auth("wrong").send().await?.status(),
        401
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth("optional-secret")
            .send()
            .await?
            .status(),
        200
    );
    for endpoint in [
        "actors",
        "events",
        "requests",
        "requests/events",
        "metrics",
        "queue-waits",
        "websockets",
    ] {
        let url = format!("{}/v1/projects/local/observe/{endpoint}", runtime.origin);
        for credential in [None, Some("stale-secret")] {
            let mut request = client.get(&url).timeout(Duration::from_secs(5));
            if let Some(credential) = credential {
                request = request.bearer_auth(credential);
            }
            assert_eq!(request.send().await?.status(), 200, "{endpoint}");
        }
    }
    runtime.stop().await
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_rejects_an_invalid_actor_contract_before_publishing_readiness() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(
        project.path(),
        "async read(): Promise<Date> { return new Date() }",
    )?;
    let output = timeout(
        Duration::from_secs(20),
        Command::new(env!("CARGO_BIN_EXE_durable-actors"))
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .env("DURABLE_ACTORS_SECRET", "test-key")
            .arg("--sdk-host")
            .arg(local_project::sdk_host())
            .arg("--project-id")
            .arg("default")
            .arg("--project")
            .arg(project.path())
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    assert!(!output.status.success());
    let logs = String::from_utf8_lossy(&output.stdout);
    assert!(logs.contains("JSON-compatible"), "{logs}");
    assert!(!logs.contains("  Ready  "), "{logs}");
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn dev_supports_backend_rpc_and_cli_generation_without_credentials() -> Result<()> {
    let project = tempfile::tempdir()?;
    local_project::write_actor(project.path(), "async read(): Promise<number> { return 1 }")?;
    let runtime = LocalRuntime::start(project.path(), None).await?;
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist");
    let backend = format!(
        "import {{ createActorTransport }} from {}; const client = createActorTransport({{ controlPlaneUrl: process.argv[1] }}); if (await client.invoke('Counter', 'one', 'read', []) !== 1) throw new Error('unexpected result');",
        serde_json::to_string(&sdk.join("backend.js"))?
    );
    let output = timeout(
        Duration::from_secs(30),
        Command::new("node")
            .args(["--input-type=module", "--eval", &backend, &runtime.origin])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        output.status.success(),
        "backend failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let consumer = tempfile::tempdir()?;
    let output = Command::new("node")
        .arg(sdk.join("cli.js"))
        .args(["generate"])
        .current_dir(consumer.path())
        .env_remove("DURABLE_ACTORS_PROJECT_ID")
        .env_remove("DURABLE_ACTORS_SECRET")
        .env_remove("DURABLE_ACTORS_API_KEY")
        .env("DURABLE_ACTORS_CONTROL_PLANE_URL", &runtime.origin)
        .kill_on_drop(true)
        .output()
        .await?;
    ensure!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(consumer.path().join("generated/index.js").exists());
    assert!(consumer.path().join("generated/index.d.ts").exists());
    runtime.stop().await
}

struct LocalRuntime {
    child: Child,
    output: tokio::task::JoinHandle<std::io::Result<String>>,
    origin: String,
}

impl LocalRuntime {
    async fn start(project: &Path, api_key: Option<&str>) -> Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_durable-actors"));
        command
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .arg("--sdk-host")
            .arg(local_project::sdk_host())
            .arg("--project")
            .arg(project)
            .env_remove("DURABLE_ACTORS_PROJECT_ID")
            .env_remove("DURABLE_ACTORS_SECRET")
            .env("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(api_key) = api_key {
            command.env("DURABLE_ACTORS_SECRET", api_key);
        }
        let mut child = command.spawn()?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        let origin = timeout(Duration::from_secs(20), async {
            let mut line = String::new();
            let mut origin = None;
            loop {
                ensure!(
                    output.read_line(&mut line).await? != 0,
                    "runtime exited before readiness: {line}"
                );
                if let Some((_, value)) = line.split_once("  Ready  ") {
                    origin = Some(value.trim().to_owned());
                }
                if line.contains("durable-actors generate") {
                    return origin.context("missing origin");
                }
                line.clear();
            }
        })
        .await??;
        let output = tokio::spawn(async move {
            let mut captured = String::new();
            let mut line = String::new();
            while output.read_line(&mut line).await? != 0 {
                eprint!("{line}");
                captured.push_str(&line);
                line.clear();
            }
            Ok(captured)
        });
        Ok(Self {
            child,
            output,
            origin,
        })
    }

    async fn contract(&self) -> Result<reqwest::Response> {
        Ok(reqwest::Client::new()
            .get(format!(
                "{}/v1/projects/local/deployment/contract",
                self.origin
            ))
            .send()
            .await?)
    }

    async fn stop(mut self) -> Result<()> {
        drop(self.child.stdin.take());
        let (status, output) = timeout(Duration::from_secs(5), async {
            tokio::join!(self.child.wait(), self.output)
        })
        .await?;
        let status = status?;
        let output = output??;
        ensure!(status.success(), "runtime exited with {status}: {output}");
        Ok(())
    }
}
