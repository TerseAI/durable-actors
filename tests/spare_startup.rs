#![cfg(unix)]

use anyhow::Result;
use std::{os::unix::fs::PermissionsExt, process::Stdio, time::Duration};
use tokio::{net::TcpListener, process::Command};

#[tokio::test]
#[ignore = "requires Bun and pnpm --dir sdk build"]
async fn spare_readiness_requires_a_working_replication_process() -> Result<()> {
    let directory = tempfile::tempdir_in("/tmp")?;
    let litestream = directory.path().join("litestream");
    std::fs::write(&litestream, "#!/bin/sh\nexit 37\n")?;
    std::fs::set_permissions(&litestream, std::fs::Permissions::from_mode(0o755))?;
    let ready = directory.path().join("ready");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, axum::Router::new()).await });
    let mut command = Command::new(env!("CARGO_BIN_EXE_durable-actors"));
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("DURABLE_ACTORS_"))
        {
            command.env_remove(name);
        }
    }
    let mut child = command
        .env(
            "PATH",
            format!("{}:{}", directory.path().display(), std::env::var("PATH")?),
        )
        .env("DURABLE_ACTORS_PROCESS_ROLE", "spare")
        .env("DURABLE_ACTORS_CONTROL_PLANE_URL", origin)
        .env(
            "DURABLE_ACTORS_SPARE_TOKEN",
            "test-spare-token-with-at-least-32-bytes",
        )
        .env("DURABLE_ACTORS_SPARE_BIND", "127.0.0.1:0")
        .env("DURABLE_ACTORS_HOST_BIND", "127.0.0.1:0")
        .env(
            "DURABLE_ACTORS_EXECUTOR_SOCKET",
            directory.path().join("executor.sock"),
        )
        .env("DURABLE_ACTORS_SPARE_READY_FILE", &ready)
        .env(
            "DURABLE_ACTORS_SDK_HOST",
            concat!(env!("CARGO_MANIFEST_DIR"), "/sdk/dist/host.js"),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ready.exists() {
                return Ok::<_, anyhow::Error>(false);
            }
            if let Some(status) = child.try_wait()? {
                return Ok(!status.success());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if child.try_wait()?.is_none() {
        Command::new("kill")
            .arg("-TERM")
            .arg(child.id().unwrap().to_string())
            .status()
            .await?;
        tokio::time::timeout(Duration::from_secs(5), child.wait()).await??;
    }
    let output = child.wait_with_output().await?;
    server.abort();
    assert!(
        result??,
        "spare advertised readiness without a working replication process"
    );
    assert!(!ready.exists());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Litestream exited before becoming ready")
    );
    Ok(())
}
