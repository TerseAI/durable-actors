#[cfg(test)]
#[path = "../tests/unit/artifacts.rs"]
mod tests;
use std::path::{Component, Path};

use anyhow::{Context, Result, ensure};
use aws_lc_rs::digest::{Context as Digest, SHA256};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactManifest {
    pub bucket: String,
    pub files: Vec<ArtifactFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactFile {
    pub path: String,
    pub object: String,
    pub generation: i64,
    pub sha256: String,
}

impl ArtifactManifest {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "gcs:{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        let value: Self = serde_json::from_slice(
            &URL_SAFE_NO_PAD.decode(
                encoded
                    .strip_prefix("gcs:")
                    .context("GCS artifact reference required")?,
            )?,
        )?;
        value.validate()?;
        Ok(value)
    }

    pub async fn verify(&self, root: &Path) -> Result<()> {
        self.validate()?;
        let started = std::time::Instant::now();
        futures_util::future::try_join_all(self.files.iter().map(|file| async {
            ensure!(
                cached_file_matches(root, file).await?,
                "prepared artifact is missing or corrupt: {}",
                file.path
            );
            Ok::<_, anyhow::Error>(())
        }))
        .await?;
        tracing::info!(
            event = "actor_code_verified",
            files = self.files.len(),
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "prepared actor code verified"
        );
        Ok(())
    }

    pub fn entrypoint(&self) -> Result<&str> {
        let names: Vec<_> = self
            .files
            .iter()
            .filter(|file| matches!(file.path.as_str(), "actors.mjs" | "actors.pyz"))
            .collect();
        ensure!(
            names.len() == 1,
            "actor artifact requires exactly one compiled entrypoint"
        );
        Ok(&names[0].path)
    }

    fn validate(&self) -> Result<()> {
        crate::storage::validate_bucket(&self.bucket)?;
        let mut paths = std::collections::HashSet::new();
        for file in &self.files {
            validate_path(&file.path)?;
            ensure!(paths.insert(&file.path), "duplicate artifact path");
            ensure!(
                file.generation > 0
                    && !file.object.is_empty()
                    && URL_SAFE_NO_PAD.decode(&file.sha256)?.len() == 32,
                "invalid artifact identity"
            );
        }
        self.entrypoint()?;
        Ok(())
    }
}

async fn cached_file_matches(root: &Path, artifact: &ArtifactFile) -> Result<bool> {
    let path = root.join(&artifact.path);
    match tokio::fs::symlink_metadata(&path).await {
        Ok(metadata) if !metadata.is_file() => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }
    let mut file = tokio::fs::File::open(path).await?;
    let mut digest = Digest::new(&SHA256);
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let size = file.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        digest.update(&buffer[..size]);
    }
    Ok(URL_SAFE_NO_PAD.encode(digest.finish().as_ref()) == artifact.sha256)
}

pub(crate) async fn install_file(
    root: &Path,
    artifact: &ArtifactFile,
    chunks: impl Stream<Item = Result<Bytes>>,
) -> Result<()> {
    validate_path(&artifact.path)?;
    let destination = root.join(&artifact.path);
    let parent = destination.parent().context("artifact parent missing")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut file = tokio::fs::File::from_std(temporary.reopen()?);
    let mut hash = Digest::new(&SHA256);
    futures_util::pin_mut!(chunks);
    while let Some(chunk) = chunks.try_next().await? {
        hash.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    ensure!(
        URL_SAFE_NO_PAD.encode(hash.finish().as_ref()) == artifact.sha256,
        "artifact digest mismatch"
    );
    drop(file);
    temporary.persist(destination)?;
    Ok(())
}

pub(crate) fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && Path::new(path)
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "artifact path must stay inside the customer directory"
    );
    Ok(())
}
