use std::time::Duration;

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build and Bun"]
async fn commit_safe_delivery_and_recovery_work_end_to_end() -> anyhow::Result<()> {
    let result = tokio::time::timeout(
        Duration::from_secs(240),
        tokio::process::Command::new("node")
            .arg("--test")
            .arg("sdk/tests/scripts/commit-delivery.test.mjs")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env(
                "DURABLE_ACTORS_BINARY",
                env!("CARGO_BIN_EXE_durable-actors"),
            )
            .kill_on_drop(true)
            .status(),
    )
    .await??;
    anyhow::ensure!(
        result.success(),
        "commit-safe delivery integration test failed"
    );
    Ok(())
}
