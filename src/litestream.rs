use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::Deserialize;
use std::{
    collections::HashMap,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use terse_litestream::{Database, FileStore, Options};
use tokio::{net::UnixListener, task::JoinHandle};

pub(crate) mod compaction;
pub(crate) mod storage;

#[async_trait]
pub(crate) trait Replicator: DatabaseRestore {
    fn socket(&self) -> &Path;
    async fn register(&self, path: &Path, replica: &Path) -> Result<()>;
    async fn sync(&self, path: &Path) -> Result<u64>;
    async fn unregister(&self, path: &Path) -> Result<()>;
}

#[async_trait]
pub(crate) trait DatabaseRestore: Send + Sync {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()>;
}

pub(crate) struct EmbeddedRestore;

pub(crate) struct Litestream {
    registry: Arc<Registry>,
    socket: PathBuf,
    server: JoinHandle<()>,
    _directory: tempfile::TempDir,
}

impl Litestream {
    pub(crate) async fn start() -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("terse-sqlite-")
            .tempdir_in("/tmp")?;
        let socket = directory.path().join("control.sock");
        let listener = UnixListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        let registry = Arc::new(Registry::default());
        let router = Router::new()
            .route("/sync", post(sync_request))
            .with_state(registry.clone());
        let server = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router).await {
                tracing::warn!(%error, "SQLite capture server stopped");
            }
        });
        Ok(Self {
            registry,
            socket,
            server,
            _directory: directory,
        })
    }
}

#[async_trait]
impl Replicator for Litestream {
    fn socket(&self) -> &Path {
        &self.socket
    }

    async fn register(&self, path: &Path, replica: &Path) -> Result<()> {
        let (registry, path, replica) =
            (self.registry.clone(), path.to_owned(), replica.to_owned());
        tokio::task::spawn_blocking(move || registry.register(path, replica)).await?
    }

    async fn sync(&self, path: &Path) -> Result<u64> {
        let (registry, path) = (self.registry.clone(), path.to_owned());
        tokio::task::spawn_blocking(move || registry.sync(&path)).await?
    }

    async fn unregister(&self, path: &Path) -> Result<()> {
        let (registry, path) = (self.registry.clone(), path.to_owned());
        tokio::task::spawn_blocking(move || registry.unregister(&path)).await?
    }
}

#[async_trait]
impl DatabaseRestore for Litestream {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()> {
        EmbeddedRestore.restore(replica, path, txid).await
    }
}

#[async_trait]
impl DatabaseRestore for EmbeddedRestore {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()> {
        ensure!(txid > 0, "cannot restore an empty SQLite position");
        let (replica, path) = (replica.to_owned(), path.to_owned());
        tokio::task::spawn_blocking(move || {
            terse_litestream::restore(&FileStore::new(replica), path, Some(txid))?;
            anyhow::Ok(())
        })
        .await?
    }
}

impl Drop for Litestream {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Deserialize)]
struct SyncRequest {
    path: PathBuf,
}

async fn sync_request(
    State(registry): State<Arc<Registry>>,
    Json(request): Json<SyncRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let result = tokio::task::spawn_blocking(move || {
        let txid = registry.sync(&request.path)?;
        anyhow::Ok(Json(
            serde_json::json!({"path":request.path,"txid":txid,"replicated_txid":txid}),
        ))
    })
    .await;
    result
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

#[derive(Default)]
struct Registry {
    databases: Mutex<HashMap<PathBuf, Arc<Mutex<Option<Capture>>>>>,
}

impl Registry {
    fn register(&self, path: PathBuf, replica: PathBuf) -> Result<()> {
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?;
        ensure!(
            !databases.contains_key(&path),
            "SQLite database already registered"
        );
        let database =
            Database::with_store(&path, Options::default(), Arc::new(FileStore::new(replica)))?;
        databases.insert(
            path,
            Arc::new(Mutex::new(Some(Capture {
                database,
                snapshot_at: Instant::now(),
            }))),
        );
        Ok(())
    }

    fn sync(&self, path: &Path) -> Result<u64> {
        let database = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?
            .get(path)
            .cloned()
            .context("SQLite database is not registered")?;
        let mut capture = database
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite capture poisoned"))?;
        capture
            .as_mut()
            .context("SQLite database was unregistered")?
            .sync()
    }

    fn unregister(&self, path: &Path) -> Result<()> {
        let database = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?
            .remove(path)
            .context("SQLite database is not registered")?;
        let capture = database
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite capture poisoned"))?
            .take()
            .context("SQLite database was unregistered")?;
        capture.database.close()?;
        Ok(())
    }
}

struct Capture {
    database: Database,
    snapshot_at: Instant,
}

impl Capture {
    fn sync(&mut self) -> Result<u64> {
        let captured = self.database.sync()?;
        if captured.changed && self.snapshot_at.elapsed() >= Duration::from_secs(60) {
            self.database.snapshot()?;
            self.snapshot_at = Instant::now();
        }
        Ok(self.database.position())
    }
}

#[cfg(test)]
#[path = "../tests/unit/litestream.rs"]
mod tests;
