#[path = "support/local_project.rs"]
mod local_project;

use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

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
async fn dev_enforces_a_secret_set_in_the_environment() -> Result<()> {
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
    assert!(consumer.path().join("generated/index.ts").exists());
    runtime.stop().await
}

struct LocalRuntime {
    child: Child,
    output: BufReader<ChildStdout>,
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
        let mut output = String::new();
        let (status, _) = timeout(Duration::from_secs(5), async {
            tokio::try_join!(self.child.wait(), self.output.read_to_string(&mut output))
        })
        .await??;
        ensure!(status.success(), "runtime exited with {status}: {output}");
        Ok(())
    }
}
