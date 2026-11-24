//! Rebuilds hosts' Rapid databases in their slots, outside the sandbox. A host prepares as its
//! assignment arrives, so the newest base downloads while it activates, then asks for the
//! database at its verified head. The node finds and downloads bases with its own warm client;
//! the history after a base is read with the host's forwarded token.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use serde::{Deserialize, Serialize};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use super::slots::{DATABASE, DATABASE_FILE};
use crate::{
    actor::ActorKey,
    bucket::{BaseName, Bucket, SnapshotStoreConfig, SqliteBase, recovery::CheckpointProgress},
    litestream::storage::{restore_database, restoring},
    storage::{SnapshotRef, VerifiedSnapshot},
};

/// Downloads and restores at once per node, each with its own budget: a restore can wait for its
/// prepared download, so they must not share permits.
const CONCURRENT_DOWNLOADS: usize = 4;
const CONCURRENT_RESTORES: usize = 4;
/// A prepared download no restore claims within this long is dropped.
const PREPARED_TTL: Duration = Duration::from_secs(60);
const PREPARED_DIR: &str = "restore";
const PREPARED_FILE: &str = "base.sqlite";

/// What a host forwards so the node reads its actor's history with the host's own scope.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SlotStorage {
    pub bucket: String,
    pub persistence: SnapshotStoreConfig,
    pub access_token: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SlotPrepare {
    pub storage: SlotStorage,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SlotRestore {
    pub storage: SlotStorage,
    pub head: SnapshotRef,
    /// The head's bytes, base64, when the host sends them so the node need not read them again.
    #[serde(default)]
    pub head_bytes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SlotRestored {
    pub txid: u64,
    /// `base`, or `history` when no base applied.
    pub source: String,
    pub base_bytes: Option<u64>,
    pub duration_ms: f64,
    /// Opening the actor's store, reading its head, and waiting for the prepared download.
    #[serde(default)]
    pub open_ms: f64,
    #[serde(default)]
    pub head_ms: f64,
    #[serde(default)]
    pub wait_ms: f64,
    /// What the restored history holds since its base, for the host's next due-base decision.
    #[serde(default)]
    pub progress: CheckpointProgress,
    #[serde(default)]
    pub base: Option<BaseName>,
}

pub(super) struct Restores {
    /// The archive as the node itself reads it; none means no prepared downloads.
    bases: Option<Arc<dyn Bucket>>,
    prepared: tokio::sync::Mutex<HashMap<String, Prepared>>,
    downloads: Arc<tokio::sync::Semaphore>,
    restores: tokio::sync::Semaphore,
}

/// The actor's store, opened once for its prepare and restore.
type Store =
    Shared<BoxFuture<'static, Result<Arc<dyn crate::bucket::SnapshotStore>, Arc<anyhow::Error>>>>;

/// The newest base, downloading into a slot before its host knows its head. `listed` holds that
/// base once listed; the task returns its key, `None` when the actor has no base.
struct Prepared {
    actor: ActorKey,
    store: Store,
    listed: Arc<OnceLock<SqliteBase>>,
    file: PathBuf,
    started: Instant,
    task: AbortOnDropHandle<Result<Option<String>>>,
}

impl Restores {
    pub fn new(bases: Option<Arc<dyn Bucket>>) -> Self {
        Self {
            bases,
            prepared: tokio::sync::Mutex::new(HashMap::new()),
            downloads: Arc::new(tokio::sync::Semaphore::new(CONCURRENT_DOWNLOADS)),
            restores: tokio::sync::Semaphore::new(CONCURRENT_RESTORES),
        }
    }

    /// Starts downloading `actor`'s newest base into `slot`, unless one is already coming.
    pub async fn prepare(
        &self,
        actor: &ActorKey,
        slot: &str,
        directory: &Path,
        storage: SlotStorage,
    ) -> Result<()> {
        self.expire().await;
        let mut prepared = self.prepared.lock().await;
        if prepared.contains_key(slot) {
            return Ok(());
        }
        let store = open(storage).boxed().shared();
        let listed = Arc::new(OnceLock::new());
        let file = directory.join(PREPARED_DIR).join(PREPARED_FILE);
        let task = AbortOnDropHandle::new(tokio::spawn(download_newest(
            self.bases.clone(),
            store.clone(),
            actor.clone(),
            listed.clone(),
            file.clone(),
            self.downloads.clone(),
        )));
        prepared.insert(
            slot.to_owned(),
            Prepared {
                actor: actor.clone(),
                store,
                listed,
                file,
                started: Instant::now(),
                task,
            },
        );
        Ok(())
    }

    /// Writes `actor`'s database at `head` into the slot's database directory and returns its
    /// SQLite transaction.
    pub async fn restore(
        &self,
        actor: &ActorKey,
        slot: &str,
        directory: &Path,
        request: SlotRestore,
    ) -> Result<SlotRestored> {
        let started = Instant::now();
        let prepared = self.take(slot, actor).await;
        let _permit = self.restores.acquire().await?;
        let store = match &prepared {
            Some(prepared) => prepared.store.clone().await,
            None => open(request.storage.clone()).await,
        }
        .map_err(|error| anyhow::anyhow!("{error:#}"))?;
        let open_ms = elapsed_ms(started);
        let head = read_head(store.as_ref(), request.head, request.head_bytes).await?;
        let head_ms = elapsed_ms(started) - open_ms;
        let database = directory.join(DATABASE);
        let restored = restore_into(store.as_ref(), head, prepared, &database).await;
        remove_prepared(directory).await;
        let restored = restored?;
        open_to_host(&database.join(DATABASE_FILE)).await?;
        Ok(SlotRestored {
            txid: restored.txid,
            source: if restored.base_bytes.is_some() {
                "base"
            } else {
                "history"
            }
            .into(),
            base_bytes: restored.base_bytes,
            duration_ms: elapsed_ms(started),
            open_ms,
            head_ms,
            wait_ms: restored.wait_ms,
            progress: restored.progress,
            base: restored.base,
        })
    }

    /// Drops the slot's prepared download, when its host reopened a parked database or left.
    pub async fn cancel(&self, slot: &str, directory: &Path) {
        if self.prepared.lock().await.remove(slot).is_some() {
            remove_prepared(directory).await;
        }
    }

    async fn take(&self, slot: &str, actor: &ActorKey) -> Option<Prepared> {
        self.prepared
            .lock()
            .await
            .remove(slot)
            .filter(|prepared| &prepared.actor == actor)
    }

    /// Drops prepared downloads nobody claimed. Their files go with their slots.
    async fn expire(&self) {
        self.prepared
            .lock()
            .await
            .retain(|_, prepared| prepared.started.elapsed() < PREPARED_TTL);
    }
}

/// The head the host verified, from the bytes it sent or else read again.
async fn read_head(
    store: &dyn crate::bucket::SnapshotStore,
    head: SnapshotRef,
    sent: Option<String>,
) -> Result<VerifiedSnapshot> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let bytes = match sent {
        Some(sent) => STANDARD.decode(sent)?.into(),
        None => store
            .get(&head.object)
            .await?
            .context("actor head is missing")?,
    };
    VerifiedSnapshot::matching(head, bytes)
}

struct Restored {
    txid: u64,
    base_bytes: Option<u64>,
    wait_ms: f64,
    progress: CheckpointProgress,
    base: Option<BaseName>,
}

/// Uses the prepared base when the head's history starts from it, and otherwise, or after any
/// failure, restores the way a host did: download beside the history read, with fallbacks.
async fn restore_into(
    store: &dyn crate::bucket::SnapshotStore,
    head: VerifiedSnapshot,
    prepared: Option<Prepared>,
    database: &Path,
) -> Result<Restored> {
    tokio::fs::create_dir_all(database).await?;
    match restore_prepared(store, head.clone(), prepared, database).await {
        Ok(restored) => Ok(restored),
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "prepared restore failed, restoring again");
            clear(database).await?;
            let restored = store
                .restore_sqlite(head, None, Some(&restoring(database)))
                .await?;
            finish(database, restored, 0.0).await
        }
    }
}

async fn restore_prepared(
    store: &dyn crate::bucket::SnapshotStore,
    head: VerifiedSnapshot,
    prepared: Option<Prepared>,
    database: &Path,
) -> Result<Restored> {
    let listed = prepared
        .as_ref()
        .and_then(|prepared| prepared.listed.get().cloned());
    let mut restored = store.restore_sqlite(head, listed, None).await?;
    let waited = Instant::now();
    if let (Some(base), Some(prepared)) = (restored.base.as_mut(), prepared)
        && prepared.task.await??.as_deref() == Some(base.key.as_str())
    {
        let partial = restoring(database);
        tokio::fs::rename(&prepared.file, &partial).await?;
        base.downloaded = Some(partial);
    }
    finish(database, restored, elapsed_ms(waited)).await
}

async fn finish(
    database: &Path,
    restored: crate::bucket::RestoredSqlite,
    wait_ms: f64,
) -> Result<Restored> {
    restore_database(
        &crate::litestream::EmbeddedRestore,
        database,
        restored.base.as_ref(),
        &restored.sqlite.files,
        restored.sqlite.txid,
    )
    .await?;
    Ok(Restored {
        txid: restored.sqlite.txid,
        base_bytes: restored.base.as_ref().map(|base| base.name.bytes),
        wait_ms,
        progress: restored.progress,
        base: restored.base.map(|base| base.name),
    })
}

/// Opens the actor's store beside finding and downloading its newest base, so the restore finds
/// both ready. The store's own errors surface when the restore awaits it.
async fn download_newest(
    bases: Option<Arc<dyn Bucket>>,
    store: Store,
    actor: ActorKey,
    listed: Arc<OnceLock<SqliteBase>>,
    file: PathBuf,
    permits: Arc<tokio::sync::Semaphore>,
) -> Result<Option<String>> {
    let newest = async {
        let Some(bases) = bases else {
            return Ok(None);
        };
        let Some(base) = crate::bucket::newest_base_of(&bases, &actor).await? else {
            return Ok(None);
        };
        let _ = listed.set(base.clone());
        download(base.clone(), file, permits).await?;
        anyhow::Ok(Some(base.key))
    };
    let (_, newest) = tokio::join!(store, newest);
    newest
}

async fn download(
    base: SqliteBase,
    file: PathBuf,
    permits: Arc<tokio::sync::Semaphore>,
) -> Result<()> {
    let _permit = permits.acquire_owned().await?;
    let directory = file.parent().context("prepared base has no directory")?;
    tokio::fs::create_dir_all(directory).await?;
    if tokio::fs::try_exists(&file).await? {
        tokio::fs::remove_file(&file).await?;
    }
    let started = Instant::now();
    base.download_to(&file).await?;
    tracing::info!(
        event = "sqlite_base_prepared",
        bytes = base.name.bytes,
        duration_ms = started.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

/// The actor's Rapid store, read with the token its host forwarded. It names no actor, so it
/// opens no write segment: only the host writes.
async fn open(
    storage: SlotStorage,
) -> Result<Arc<dyn crate::bucket::SnapshotStore>, Arc<anyhow::Error>> {
    let opened = async {
        let state = crate::bucket::GcsBucket::for_restore(
            &storage.bucket,
            crate::bucket::forwarded_token(storage.access_token.clone()),
        )
        .await?;
        let store: Arc<dyn crate::bucket::SnapshotStore> =
            Arc::new(crate::bucket::RapidSnapshots::gcs(
                &storage.persistence,
                state.clients(),
                CancellationToken::new(),
                None,
            )?);
        anyhow::Ok(store)
    };
    opened.await.map_err(Arc::new)
}

/// The node writes as another user, so the host gets read and write access to the file.
async fn open_to_host(file: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(file, std::fs::Permissions::from_mode(0o666)).await?;
    Ok(())
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

async fn clear(database: &Path) -> Result<()> {
    for file in [restoring(database), database.join(DATABASE_FILE)] {
        if tokio::fs::try_exists(&file).await? {
            tokio::fs::remove_file(&file).await?;
        }
    }
    Ok(())
}

async fn remove_prepared(directory: &Path) {
    let _ = tokio::fs::remove_dir_all(directory.join(PREPARED_DIR)).await;
}

#[cfg(test)]
#[path = "../../tests/unit/node_cache/restores.rs"]
mod tests;
