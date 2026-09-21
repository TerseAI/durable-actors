use std::{os::unix::fs::PermissionsExt, path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

#[tokio::test]
async fn dev_publishes_the_contract_before_readiness_and_refreshes_it_on_restart() -> Result<()> {
    let project = tempfile::Builder::new()
        .prefix("actor's project ")
        .tempdir()?;
    std::fs::write(project.path().join("actors.ts"), "export {}\n")?;
    let file = project.path().join("contract.json");
    let contract: Value =
        serde_json::from_str(include_str!("../sdk/tests/fixtures/public-contract.json"))?;
    std::fs::write(&file, serde_json::to_vec(&contract)?)?;
    let runtime = LocalRuntime::start(project.path(), Some(&file)).await?;
    let first_api_key = runtime.api_key.clone();
    let first: Value = runtime
        .contract("")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["contract"], contract);
    let revision = first["codeRevision"].as_str().context("missing revision")?;
    let pinned: Value = runtime
        .contract(&format!("?revision={revision}"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(pinned, first);
    runtime.stop().await?;

    let mut changed = contract.clone();
    changed["actors"][0]["rpc"]["methods"][0]["name"] = "reset".into();
    std::fs::write(&file, serde_json::to_vec(&changed)?)?;
    let runtime = LocalRuntime::start(project.path(), Some(&file)).await?;
    assert_ne!(runtime.api_key, first_api_key);
    let second: Value = runtime
        .contract("")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(second["contract"], changed);
    assert_ne!(second["codeRevision"], first["codeRevision"]);
    assert_ne!(second["contractHash"], first["contractHash"]);
    assert_eq!(
        runtime
            .contract(&format!("?revision={revision}"))
            .await?
            .status(),
        404
    );
    runtime.stop().await?;

    let runtime = LocalRuntime::start(project.path(), None).await?;
    assert_eq!(runtime.contract("").await?.status(), 404);
    runtime.stop().await
}

#[tokio::test]
async fn dev_rejects_an_invalid_contract_before_publishing_readiness() -> Result<()> {
    let project = tempfile::tempdir()?;
    std::fs::write(project.path().join("actors.ts"), "export {}\n")?;
    let file = project.path().join("contract.json");
    std::fs::write(&file, r#"{"version":99,"actors":[]}"#)?;
    let output = timeout(
        Duration::from_secs(5),
        Command::new(env!("CARGO_BIN_EXE_little-actors"))
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .env("DURABLE_OBJECT_PROJECT_ID", "default")
            .env("DURABLE_OBJECT_API_KEY", "test-key")
            .arg("--project-id")
            .arg("default")
            .arg("--project")
            .arg(project.path())
            .arg("--contract")
            .arg(file)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    assert!(!output.status.success());
    let logs = String::from_utf8_lossy(&output.stdout);
    assert!(
        logs.contains("unsupported public actor contract version"),
        "{logs}"
    );
    assert!(!project.path().join(".little-actors/runtime.json").exists());
    Ok(())
}

struct LocalRuntime {
    child: Child,
    output: BufReader<ChildStdout>,
    origin: String,
    api_key: String,
}

impl LocalRuntime {
    async fn start(project: &Path, contract: Option<&Path>) -> Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_little-actors"));
        command
            .args(["dev", "--port", "0", "--entrypoint", "actors.ts"])
            .arg("--project-id")
            .arg("default")
            .arg("--project")
            .arg(project)
            .env_remove("DURABLE_OBJECT_API_KEY")
            .env("DURABLE_OBJECT_PARENT_LIFETIME_STDIN", "1")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(contract) = contract {
            command.arg("--contract").arg(contract);
        }
        let mut child = command.spawn()?;
        let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
        let (origin, startup_output) = timeout(Duration::from_secs(5), async {
            let mut line = String::new();
            let mut startup_output = String::new();
            loop {
                ensure!(
                    output.read_line(&mut line).await? != 0,
                    "runtime exited before readiness: {line}"
                );
                startup_output.push_str(&line);
                if let Some((_, origin)) = line.split_once("  Ready  ") {
                    return Ok::<_, anyhow::Error>((origin.trim().to_owned(), startup_output));
                }
                line.clear();
            }
        })
        .await??;
        let key_file = project.join(".little-actors/api-key");
        let api_key = std::fs::read_to_string(&key_file)?;
        assert_eq!(
            std::fs::metadata(&key_file)?.permissions().mode() & 0o777,
            0o600
        );
        assert!(!startup_output.contains(&api_key));
        let export = startup_output
            .lines()
            .find(|line| line.starts_with("export DURABLE_OBJECT_API_KEY="))
            .context("missing generated API key instruction")?;
        let loaded = Command::new("sh")
            .arg("-c")
            .arg(format!("{export}\nprintf '%s' \"$DURABLE_OBJECT_API_KEY\""))
            .output()
            .await?;
        assert!(loaded.status.success());
        assert_eq!(String::from_utf8(loaded.stdout)?, api_key);
        ensure!(api_key.len() >= 32, "generated API key is too short");
        assert!(!project.join(".little-actors/runtime.json").exists());
        Ok(Self {
            child,
            output,
            origin,
            api_key,
        })
    }

    async fn contract(&self, query: &str) -> Result<reqwest::Response> {
        Ok(reqwest::Client::new()
            .get(format!(
                "{}/v1/projects/default/deployment/contract{query}",
                self.origin
            ))
            .bearer_auth(&self.api_key)
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
        assert!(!output.contains(&self.api_key));
        Ok(())
    }
}
