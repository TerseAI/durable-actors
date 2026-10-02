use super::storage::LtxFile;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};

#[async_trait]
pub(crate) trait LtxCompactor: Send + Sync {
    async fn compact(&self, files: &[LtxFile]) -> Result<LtxFile>;
}

pub(crate) struct CompactCommand(pub PathBuf);

#[async_trait]
impl LtxCompactor for CompactCommand {
    async fn compact(&self, files: &[LtxFile]) -> Result<LtxFile> {
        let last = files.last().context("empty checkpoint")?.last;
        let mut child = Command::new(&self.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("start LTX compactor")?;
        let output = tokio::time::timeout(Duration::from_secs(120), async {
            let mut input = child.stdin.take().context("compactor input missing")?;
            input.write_all(&serde_json::to_vec(files)?).await?;
            drop(input);
            anyhow::Ok(child.wait_with_output().await?)
        })
        .await
        .context("LTX compaction timed out")??;
        ensure!(
            output.status.success(),
            "LTX compaction failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let file: LtxFile = serde_json::from_slice(&output.stdout)?;
        file.validate()?;
        ensure!(
            file.first == 1 && file.last == last && file.level == 9,
            "compacted checkpoint transaction mismatch"
        );
        Ok(file)
    }
}
