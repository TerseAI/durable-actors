#![cfg(unix)]

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    time::timeout,
};

#[tokio::test]
async fn interrupt_exits_while_the_parent_stdin_pipe_is_open() -> Result<()> {
    assert_shutdown(Some("-INT")).await
}

#[tokio::test]
async fn terminate_exits_while_the_parent_stdin_pipe_is_open() -> Result<()> {
    assert_shutdown(Some("-TERM")).await
}

#[tokio::test]
async fn closing_parent_stdin_stops_the_runtime() -> Result<()> {
    assert_shutdown(None).await
}

async fn assert_shutdown(signal: Option<&str>) -> Result<()> {
    let project = tempfile::tempdir()?;
    std::fs::write(project.path().join("actors.ts"), "export {}\n")?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_little-actors"))
        .args([
            "dev",
            "--port",
            "0",
            "--entrypoint",
            "actors.ts",
            "--api-key",
            "test-key",
        ])
        .arg("--project")
        .arg(project.path())
        .env("DURABLE_OBJECT_PARENT_LIFETIME_STDIN", "1")
        .env("RUST_LOG", "info")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut parent_stdin = child.stdin.take();
    let mut output = BufReader::new(child.stdout.take().context("capture runtime output")?);
    timeout(Duration::from_secs(5), wait_until_ready(&mut output)).await??;
    tokio::time::sleep(Duration::from_millis(50)).await;

    if let Some(signal) = signal {
        let status = Command::new("kill")
            .args([
                signal,
                &child.id().context("runtime exited early")?.to_string(),
            ])
            .status()
            .await?;
        ensure!(status.success(), "send runtime shutdown signal");
    } else {
        drop(parent_stdin.take());
    }

    let status = timeout(Duration::from_secs(3), child.wait())
        .await
        .context("runtime did not exit after shutdown")??;
    ensure!(status.success(), "runtime exited with {status}");
    drop(parent_stdin);
    let mut remaining = String::new();
    output.read_to_string(&mut remaining).await?;
    let message = if signal.is_some() {
        "shutdown signal received"
    } else {
        "parent process exited"
    };
    ensure!(
        remaining.contains(message),
        "shutdown was not handled: {remaining}"
    );
    Ok(())
}

async fn wait_until_ready(output: &mut BufReader<tokio::process::ChildStdout>) -> Result<String> {
    let mut line = String::new();
    loop {
        ensure!(
            output.read_line(&mut line).await? != 0,
            "runtime exited before readiness"
        );
        if let Some((_, origin)) = line.split_once("  Ready  ") {
            return Ok(origin.trim().to_owned());
        }
        line.clear();
    }
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn environment_configured_local_runtime_recovers_after_restart() -> Result<()> {
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let project = tempfile::tempdir_in(&sdk)?;
    let shell_directory = tempfile::tempdir()?;
    std::fs::write(
        project.path().join("actors.ts"),
        format!(
            r#"
        import {{ Actor, Persisted }} from {};
        export class Counter extends Actor {{
            @Persisted value = 0;
            async read() {{ return this.value; }}
            async increment() {{ return ++this.value; }}
        }}
    "#,
            serde_json::to_string(&sdk.join("dist/index.js"))?
        ),
    )?;
    std::fs::write(
        project.path().join("tsconfig.json"),
        r#"{"compilerOptions":{"target":"ES2022","module":"NodeNext","moduleResolution":"NodeNext","strict":true,"skipLibCheck":true},"include":["actors.ts"]}"#,
    )?;
    let script = project.path().join("invoke.mjs");
    std::fs::write(
        &script,
        format!(
            r#"
        import assert from 'node:assert/strict';
        import {{ RemoteActorClient }} from {};
        const client = new RemoteActorClient();
        const before = Number(process.argv[2]);
        assert.equal(await client.invoke('Counter', 'one', 'read', []), before);
        assert.equal(await client.invoke('Counter', 'one', 'increment', []), before + 1);
        assert.equal(await client.invoke('Counter', 'one', 'read', []), before + 1);
    "#,
            serde_json::to_string(&sdk.join("dist/client/remoteClient.js"))?
        ),
    )?;

    for before in 0..2 {
        let mut runtime = Command::new(env!("CARGO_BIN_EXE_little-actors"))
            .args(["dev", "--sdk-host"])
            .arg(sdk.join("dist/host.js"))
            .current_dir(shell_directory.path())
            .env("DURABLE_OBJECT_PROJECT", project.path())
            .env("DURABLE_OBJECT_ENTRYPOINT", "actors.ts")
            .env("DURABLE_OBJECT_PORT", "0")
            .env("DURABLE_OBJECT_DATA_DIR", "state")
            .env("DURABLE_OBJECT_STORAGE", "local")
            .env("DURABLE_OBJECT_API_KEY", "local-test-key")
            .env("DURABLE_OBJECT_PARENT_LIFETIME_STDIN", "1")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut output = BufReader::new(runtime.stdout.take().context("runtime stdout")?);
        let origin = timeout(Duration::from_secs(5), wait_until_ready(&mut output)).await??;
        assert!(!shell_directory.path().join("state/runtime.json").exists());
        let result = timeout(
            Duration::from_secs(30),
            Command::new("node")
                .arg(&script)
                .arg(before.to_string())
                .env("DURABLE_OBJECT_CONTROL_PLANE_URL", origin)
                .env("DURABLE_OBJECT_API_KEY", "local-test-key")
                .env("DURABLE_OBJECT_TELEMETRY", "0")
                .kill_on_drop(true)
                .output(),
        )
        .await?;
        drop(runtime.stdin.take());
        let status = timeout(Duration::from_secs(10), runtime.wait()).await??;
        ensure!(status.success(), "local runtime shutdown failed: {status}");
        let result = result?;
        ensure!(
            result.status.success(),
            "actor calls failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert!(
        shell_directory
            .path()
            .join("state/objects/little-actors/v2")
            .is_dir()
    );
    Ok(())
}
