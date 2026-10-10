use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use walkdir::{DirEntry, WalkDir};

type Snapshot = BTreeMap<PathBuf, (SystemTime, u64)>;

/// Poll only source metadata. This also works on mounted filesystems and avoids
/// allocating OS watches for dependency trees or virtual environments.
pub(super) struct Sources {
    project: PathBuf,
    data: PathBuf,
    published: Snapshot,
}

impl Sources {
    pub fn new(project: &Path, data: &Path) -> Result<Self> {
        let mut sources = Self {
            project: project.canonicalize()?,
            data: data.canonicalize()?,
            published: Snapshot::new(),
        };
        sources.published = sources.snapshot()?;
        Ok(sources)
    }

    fn included(&self, entry: &DirEntry) -> bool {
        if entry.depth() == 0 {
            return true;
        }
        let name = entry.file_name().to_string_lossy();
        !entry.path().starts_with(&self.data)
            && !matches!(
                name.as_ref(),
                ".git"
                    | "node_modules"
                    | ".venv"
                    | "venv"
                    | "__pycache__"
                    | ".mypy_cache"
                    | ".pytest_cache"
                    | ".ruff_cache"
                    | ".durable-actors"
                    | "generated"
                    | "target"
                    | "dist"
            )
            && !(entry.file_type().is_dir() && entry.path().join("pyvenv.cfg").is_file())
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let mut files = Snapshot::new();
        for entry in WalkDir::new(&self.project)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| self.included(e))
        {
            let entry = entry.context("scan actor sources")?;
            if entry.file_type().is_file() && is_source(entry.path()) {
                // A file may disappear while an editor atomically replaces it.
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(error)
                        if error
                            .io_error()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                files.insert(entry.into_path(), (metadata.modified()?, metadata.len()));
            }
        }
        Ok(files)
    }

    pub async fn watch<F: Future<Output = Result<()>>>(mut self, mut refresh: impl FnMut() -> F) {
        let mut candidate = self.published.clone();
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let scan = tokio::task::spawn_blocking(move || {
                let snapshot = self.snapshot();
                (self, snapshot)
            })
            .await;
            let snapshot;
            (self, snapshot) = scan.expect("source scan panicked");
            let snapshot = match snapshot {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    anstream::eprintln!(
                        "Actor source watcher failed: {error:#}. Automatic reload is disabled; restart to apply changes."
                    );
                    std::future::pending::<()>().await;
                    return;
                }
            };
            // Two identical scans debounce saves; updates arriving during a build
            // are picked up by the next scan. Builds are always serialized.
            if snapshot == candidate && snapshot != self.published {
                self.published = snapshot.clone();
                if let Err(error) = refresh().await {
                    anstream::eprintln!("Actor source update failed: {error:#}");
                }
            }
            candidate = snapshot;
        }
    }
}

fn is_source(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|s| s.to_str()),
        Some(
            "ts" | "tsx"
                | "mts"
                | "cts"
                | "js"
                | "jsx"
                | "mjs"
                | "cjs"
                | "json"
                | "yaml"
                | "yml"
                | "py"
                | "toml"
        )
    ) || matches!(
        path.file_name().and_then(|s| s.to_str()),
        Some("bun.lock" | "uv.lock" | "requirements.txt")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prunes_dependencies_generated_state_and_custom_venvs() -> Result<()> {
        let root = tempfile::tempdir()?;
        for directory in [
            "state",
            "src",
            "node_modules/package",
            "generated",
            "custom-env/lib",
            "src/__pycache__",
        ] {
            std::fs::create_dir_all(root.path().join(directory))?;
            std::fs::write(root.path().join(directory).join("actors.py"), "")?;
        }
        std::fs::write(root.path().join("custom-env/pyvenv.cfg"), "")?;
        std::fs::write(root.path().join("uv.lock"), "")?;
        let sources = Sources::new(root.path(), &root.path().join("state"))?;
        let files: Vec<_> = sources
            .published
            .keys()
            .map(|p| p.strip_prefix(&sources.project).unwrap().to_path_buf())
            .collect();
        assert_eq!(
            files,
            [PathBuf::from("src/actors.py"), PathBuf::from("uv.lock")]
        );
        std::fs::remove_file(root.path().join("src/actors.py"))?;
        assert_ne!(sources.snapshot()?, sources.published);
        Ok(())
    }
}
