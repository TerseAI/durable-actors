use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::digest::{SHA256, digest};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use super::{ReplicaStream, SessionHead, StreamHead};
use crate::clock::{Clock, SystemClock};

#[async_trait]
pub trait ReplicaStore: Send + Sync {
    async fn initialize_session(&self, session: &str) -> Result<()>;
    async fn append(&self, stream: &ReplicaStream, archive_url: &str, bytes: &[u8]) -> Result<()>;
    async fn stream_head(&self, stream: &ReplicaStream) -> Result<StreamHead>;
    async fn seal_session(&self, session: &str) -> Result<SessionHead>;
    async fn put(&self, object: &str, archive_url: &str, bytes: &[u8]) -> Result<()>;
    async fn read(&self, object: &str) -> Result<Option<Vec<u8>>>;
    async fn pending(&self, limit: u32) -> Result<Vec<PendingSnapshot>>;
    async fn archived(&self, object: &str) -> Result<()>;
    async fn attempted(&self, object: &str) -> Result<()>;
}

pub struct PendingSnapshot {
    pub object: String,
    pub archive_url: String,
    pub bytes: Vec<u8>,
    pub created_at_ms: i64,
}

pub struct FileReplicaStore {
    state: Arc<Mutex<StoreState>>,
}

struct StoreState {
    directory: PathBuf,
    capacity: u64,
    used: u64,
    attempts: u64,
    snapshots: HashMap<String, Metadata>,
    streams: HashMap<String, StreamHead>,
    sessions: HashMap<String, SessionHead>,
    _lock: File,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    object: String,
    archive_url: String,
    created_at_ms: i64,
    size: u64,
    #[serde(skip)]
    last_attempt: u64,
}

impl FileReplicaStore {
    pub async fn open(directory: PathBuf, capacity: u64) -> Result<Self> {
        ensure!(capacity > 0, "replica spool capacity must be positive");
        let state =
            tokio::task::spawn_blocking(move || StoreState::open(directory, capacity)).await??;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut StoreState) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let state = self.state.clone();
        // Keep mutations and accounting together even if the caller is cancelled.
        tokio::task::spawn_blocking(move || {
            let mut state = state
                .lock()
                .map_err(|_| anyhow::anyhow!("replica store poisoned"))?;
            operation(&mut state)
        })
        .await?
    }
}

#[async_trait]
impl ReplicaStore for FileReplicaStore {
    async fn initialize_session(&self, session: &str) -> Result<()> {
        let session = session.to_owned();
        self.run(move |state| {
            let mut head = state.session(&session);
            ensure!(!head.sealed, "replica session is sealed");
            head.initialized = true;
            state.save_session(head)
        })
        .await
    }

    async fn append(&self, stream: &ReplicaStream, archive_url: &str, bytes: &[u8]) -> Result<()> {
        let stream = stream.clone();
        let bytes = bytes.to_vec();
        let archive_url = archive_url.to_owned();
        self.run(move |state| state.append(&stream, archive_url, &bytes))
            .await
    }

    async fn stream_head(&self, stream: &ReplicaStream) -> Result<StreamHead> {
        let stream = stream.clone();
        self.run(move |state| {
            ensure!(
                state.session(&stream.session).initialized,
                "replica session is absent"
            );
            state.head(&stream)
        })
        .await
    }

    async fn seal_session(&self, session: &str) -> Result<SessionHead> {
        let session = session.to_owned();
        self.run(move |state| {
            let mut head = state.session(&session);
            head.sealed = true;
            state.save_session(head.clone())?;
            head.streams = state
                .streams
                .values()
                .filter(|head| head.stream.session == session)
                .cloned()
                .collect();
            head.streams
                .sort_by(|a, b| a.stream.prefix.cmp(&b.stream.prefix));
            Ok(head)
        })
        .await
    }

    async fn put(&self, object: &str, archive_url: &str, bytes: &[u8]) -> Result<()> {
        let metadata = Metadata {
            object: object.into(),
            archive_url: archive_url.into(),
            created_at_ms: i64::try_from(SystemClock.now_ms()?)?,
            size: u64::try_from(bytes.len())?,
            last_attempt: 0,
        };
        let bytes = bytes.to_vec();
        self.run(move |state| state.put(metadata, &bytes)).await
    }

    async fn read(&self, object: &str) -> Result<Option<Vec<u8>>> {
        let object = object.to_owned();
        self.run(move |state| {
            state
                .snapshots
                .get(&object)
                .map(|metadata| read_blob(&state.path(&object), metadata.size))
                .transpose()
        })
        .await
    }

    async fn pending(&self, limit: u32) -> Result<Vec<PendingSnapshot>> {
        self.run(move |state| state.pending(limit)).await
    }

    async fn archived(&self, object: &str) -> Result<()> {
        let object = object.to_owned();
        self.run(move |state| state.archived(&object)).await
    }

    async fn attempted(&self, object: &str) -> Result<()> {
        let object = object.to_owned();
        self.run(move |state| {
            state.attempts += 1;
            if let Some(metadata) = state.snapshots.get_mut(&object) {
                metadata.last_attempt = state.attempts;
            }
            Ok(())
        })
        .await
    }
}

impl StoreState {
    fn open(directory: PathBuf, capacity: u64) -> Result<Self> {
        create_directory(&directory)?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(directory.join(".lock"))?;
        lock.try_lock()
            .context("replica directory is already open")?;
        let mut state = Self {
            directory,
            capacity,
            used: 0,
            attempts: 0,
            snapshots: HashMap::new(),
            streams: HashMap::new(),
            sessions: HashMap::new(),
            _lock: lock,
        };
        state.restore()?;
        Ok(state)
    }

    fn restore(&mut self) -> Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".pending-"))
            {
                fs::remove_file(path)?;
                continue;
            }
            if path
                .extension()
                .is_some_and(|extension| extension == "session")
            {
                let head: SessionHead = serde_json::from_reader(File::open(&path)?)?;
                ensure!(
                    path == self.path(&head.session).with_extension("session"),
                    "invalid session metadata"
                );
                self.sessions.insert(head.session.clone(), head);
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "blob") {
                if path
                    .extension()
                    .is_some_and(|extension| extension == "stream")
                {
                    let head: StreamHead = serde_json::from_reader(File::open(&path)?)?;
                    ensure!(
                        path == self.path(&head.stream.prefix).with_extension("stream"),
                        "invalid stream metadata"
                    );
                    self.streams.insert(head.stream.prefix.clone(), head);
                }
                continue;
            }
            let mut file = BufReader::new(File::open(&path)?);
            let mut header = String::new();
            let header_size = file.read_line(&mut header)?;
            let metadata: Metadata = serde_json::from_str(&header)?;
            ensure!(
                header.ends_with('\n') && path == self.path(&metadata.object),
                "invalid replica blob metadata"
            );
            ensure!(
                file.get_ref().metadata()?.len() == header_size as u64 + metadata.size,
                "incomplete replica blob"
            );
            self.used = self
                .used
                .checked_add(metadata.size)
                .context("replica size overflow")?;
            self.snapshots.insert(metadata.object.clone(), metadata);
        }
        Ok(())
    }

    fn put(&mut self, metadata: Metadata, bytes: &[u8]) -> Result<()> {
        let path = self.path(&metadata.object);
        if let Some(existing) = self.snapshots.get(&metadata.object) {
            ensure!(
                read_blob(&path, existing.size)? == bytes,
                "conflicting immutable snapshot"
            );
            return File::open(&self.directory)?.sync_all().map_err(Into::into);
        }
        let used = self
            .used
            .checked_add(metadata.size)
            .context("replica size overflow")?;
        ensure!(used <= self.capacity, "replica spool is full");
        let mut temporary = tempfile::Builder::new()
            .prefix(".pending-")
            .tempfile_in(&self.directory)?;
        serde_json::to_writer(&mut temporary, &metadata)?;
        temporary.write_all(b"\n")?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        temporary.persist_noclobber(path)?;
        self.used = used;
        self.snapshots.insert(metadata.object.clone(), metadata);
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn head(&self, stream: &ReplicaStream) -> Result<StreamHead> {
        if let Some(head) = self.streams.get(&stream.prefix) {
            ensure!(head.stream == *stream, "replica stream identity mismatch");
            return Ok(head.clone());
        }
        Ok(StreamHead {
            stream: stream.clone(),
            latest: None,
        })
    }

    fn append(&mut self, stream: &ReplicaStream, archive_url: String, bytes: &[u8]) -> Result<()> {
        let mut head = self.head(stream)?;
        let session = self.session(&stream.session);
        ensure!(
            session.initialized && !session.sealed,
            "replica session is absent or sealed"
        );
        let snapshot = stream.snapshot(bytes)?;
        if let Some(latest) = &head.latest {
            ensure!(
                snapshot.state_version >= latest.state_version,
                "replica write is out of order"
            );
            if snapshot.state_version == latest.state_version {
                ensure!(snapshot == *latest, "conflicting immutable snapshot");
                return File::open(&self.directory)?.sync_all().map_err(Into::into);
            }
        }
        // Full snapshots include intervening writes that used the bucket fallback.
        self.put(
            Metadata {
                object: snapshot.object.clone(),
                archive_url,
                created_at_ms: i64::try_from(SystemClock.now_ms()?)?,
                size: bytes.len() as u64,
                last_attempt: 0,
            },
            bytes,
        )?;
        head.latest = Some(snapshot);
        self.save_head(head)
    }

    fn session(&self, session: &str) -> SessionHead {
        self.sessions
            .get(session)
            .cloned()
            .unwrap_or_else(|| SessionHead {
                session: session.into(),
                initialized: false,
                sealed: false,
                streams: Vec::new(),
            })
    }

    fn save_session(&mut self, head: SessionHead) -> Result<()> {
        let mut temporary = tempfile::Builder::new()
            .prefix(".pending-")
            .tempfile_in(&self.directory)?;
        serde_json::to_writer(&mut temporary, &head)?;
        temporary.as_file().sync_all()?;
        temporary.persist(self.path(&head.session).with_extension("session"))?;
        self.sessions.insert(head.session.clone(), head);
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn save_head(&mut self, head: StreamHead) -> Result<()> {
        let mut temporary = tempfile::Builder::new()
            .prefix(".pending-")
            .tempfile_in(&self.directory)?;
        serde_json::to_writer(&mut temporary, &head)?;
        temporary.as_file().sync_all()?;
        temporary.persist(self.path(&head.stream.prefix).with_extension("stream"))?;
        self.streams.insert(head.stream.prefix.clone(), head);
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn pending(&self, limit: u32) -> Result<Vec<PendingSnapshot>> {
        let mut pending: Vec<_> = self.snapshots.values().collect();
        pending.sort_unstable_by_key(|metadata| {
            (
                metadata.last_attempt,
                metadata.created_at_ms,
                &metadata.object,
            )
        });
        pending
            .into_iter()
            .take(limit as usize)
            .map(|metadata| {
                Ok(PendingSnapshot {
                    object: metadata.object.clone(),
                    archive_url: metadata.archive_url.clone(),
                    bytes: read_blob(&self.path(&metadata.object), metadata.size)?,
                    created_at_ms: metadata.created_at_ms,
                })
            })
            .collect()
    }

    fn archived(&mut self, object: &str) -> Result<()> {
        if let Some(metadata) = self.snapshots.get(object) {
            fs::remove_file(self.path(object))?;
            self.used -= metadata.size;
            self.snapshots.remove(object);
        }
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    fn path(&self, object: &str) -> PathBuf {
        let name = URL_SAFE_NO_PAD.encode(digest(&SHA256, object.as_bytes()).as_ref());
        self.directory.join(format!("{name}.blob"))
    }
}

fn read_blob(path: &Path, size: u64) -> Result<Vec<u8>> {
    let mut file = BufReader::new(File::open(path)?);
    file.skip_until(b'\n')?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 == size, "incomplete replica blob");
    Ok(bytes)
}

fn create_directory(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    create_directory(parent)?;
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(error.into()),
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}
