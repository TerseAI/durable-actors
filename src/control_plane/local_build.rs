use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tempfile::TempDir;
use tokio::process::Command;

use super::{
    admin::{DeploymentSource, HostLaunchSpec},
    contracts::PublicActorContract,
};

#[async_trait]
pub(super) trait LocalCodeCompiler: Send + Sync {
    async fn compile(
        &self,
        project: &Path,
        entrypoint: &str,
        output: &Path,
    ) -> Result<PublicActorContract>;
}

pub(super) struct LocalBuilds {
    project: PathBuf,
    directory: PathBuf,
    compiler: Arc<dyn LocalCodeCompiler>,
}

impl LocalBuilds {
    pub fn new(project: PathBuf, directory: PathBuf, compiler: Arc<dyn LocalCodeCompiler>) -> Self {
        Self {
            project,
            directory,
            compiler,
        }
    }

    pub async fn prepare(&self, source: &HostLaunchSpec) -> Result<PreparedLocalBuild> {
        ensure!(
            source.image_ref == "local",
            "local control plane requires a local deployment"
        );
        ensure!(
            Path::new(&source.working_directory) == self.project,
            "local deployments must use the current project directory"
        );
        ensure!(
            source.secret_refs.is_empty(),
            "Modal secret references are unavailable in local mode"
        );
        let root = self.directory.join(&source.project_id);
        tokio::fs::create_dir_all(&root).await?;
        let directory = tempfile::Builder::new().prefix("build-").tempdir_in(root)?;
        let entrypoint = source
            .actor_entrypoint
            .as_deref()
            .unwrap_or("src/actors.ts");
        let contract = self
            .compiler
            .compile(&self.project, entrypoint, directory.path())
            .await?;
        let mut spec = source.clone();
        spec.source = Some(DeploymentSource::from(source));
        spec.actor_entrypoint = Some(directory.path().join("actors.mjs").display().to_string());
        Ok(PreparedLocalBuild {
            spec,
            contract,
            directory,
        })
    }
}

pub(super) struct PreparedLocalBuild {
    pub spec: HostLaunchSpec,
    pub contract: PublicActorContract,
    directory: TempDir,
}

impl PreparedLocalBuild {
    pub async fn commit(self) {
        let current = self.directory.keep();
        // Publication follows host retirement; only the active bundle is retained.
        let cleanup = tokio::task::spawn_blocking(move || -> Result<()> {
            for entry in std::fs::read_dir(current.parent().context("local build directory")?)? {
                let entry = entry?;
                if entry.path() != current
                    && entry.file_name().to_string_lossy().starts_with("build-")
                {
                    std::fs::remove_dir_all(entry.path())?;
                }
            }
            Ok(())
        })
        .await;
        if !matches!(cleanup, Ok(Ok(()))) {
            tracing::warn!(?cleanup, "failed to remove replaced local actor build");
        }
    }
}

pub(super) struct BunCodeCompiler {
    sdk_host: Option<PathBuf>,
}

impl BunCodeCompiler {
    pub fn new(sdk_host: Option<PathBuf>) -> Self {
        Self { sdk_host }
    }
}

#[async_trait]
impl LocalCodeCompiler for BunCodeCompiler {
    async fn compile(
        &self,
        project: &Path,
        entrypoint: &str,
        output: &Path,
    ) -> Result<PublicActorContract> {
        let mut command = Command::new("bun");
        command.current_dir(project)
            .args(["--eval", "await import(new URL('compiler/deployment-build.js', import.meta.resolve(process.env.DURABLE_ACTORS_SDK_HOST ?? 'durable-actors/host')).href)"])
            .arg("durable-actors-build").arg(project).arg(entrypoint).arg(output).arg("local")
            .kill_on_drop(true);
        if let Some(host) = &self.sdk_host {
            command.env("DURABLE_ACTORS_SDK_HOST", host);
        }
        let result = tokio::time::timeout(Duration::from_secs(120), command.output())
            .await
            .context("local actor build timed out")??;
        ensure!(
            result.status.success(),
            "local actor build failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        PublicActorContract::new(
            serde_json::from_slice(&result.stdout).context("read compiled actor contract")?,
        )
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/local_build.rs"]
mod tests;
