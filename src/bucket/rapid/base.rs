//! Bases: a whole SQLite file at one published version, stored in the archive bucket beside the
//! actor's history as `durable-actors-v3-snapshots-<actor>~bases~<owner epoch>~<version>~<txid>~
//! <bytes>.sqlite`. Inside the actor's snapshot names, so host tokens need no further rule, and
//! the only `.sqlite` objects there. A restore starts from the newest base at or below its head.

use super::*;
use crate::storage::SnapshotRef;

const SNAPSHOTS: &str = "durable-actors-v3-snapshots-";
const BASES: &str = "~bases~";

/// What a base's name records about it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BaseName {
    pub owner_epoch: u64,
    pub version: u64,
    pub txid: u64,
    pub bytes: u64,
}

/// A base a restore can start from.
#[derive(Clone)]
pub(crate) struct SqliteBase {
    pub key: String,
    pub name: BaseName,
    pub bucket: Arc<dyn Bucket>,
    /// Where the restore downloaded it while it read the changes since, if it did.
    pub downloaded: Option<std::path::PathBuf>,
}

impl SqliteBase {
    /// Downloads the base into a new file at `path`.
    pub async fn download_to(&self, path: &std::path::Path) -> Result<()> {
        let bytes = self
            .bucket
            .download_to(&self.key, path)
            .await?
            .with_context(|| format!("SQLite base {} disappeared", self.key))?;
        ensure!(
            bytes == self.name.bytes,
            "SQLite base {} is truncated",
            self.key
        );
        Ok(())
    }
}

/// The base object for the database at `snapshot`: SQLite transaction `txid`, `bytes` long.
pub(crate) fn key(snapshot: &SnapshotRef, txid: u64, bytes: u64) -> Result<String> {
    let name = object_name(&snapshot.object)?;
    let (actor, version) = name
        .strip_suffix(".json")
        .and_then(|name| name.split_once('~'))
        .context("snapshot object is not a version")?;
    Ok(format!("{actor}{BASES}{version}~{txid}~{bytes}.sqlite"))
}

/// The prefix every base of the actor whose snapshots live under `snapshots` starts with.
pub(crate) fn prefix(snapshots: &str) -> Result<String> {
    let name = object_name(snapshots)?;
    let actor = name
        .strip_suffix('~')
        .filter(|actor| !actor.contains('~'))
        .context("invalid snapshot prefix")?;
    Ok(format!("{actor}{BASES}"))
}

impl LogStorage {
    /// The newest base of the actor that owns `head` at or below `head`'s version.
    pub(super) async fn newest_base(&self, head: &SnapshotRef) -> Result<Option<SqliteBase>> {
        let keys = self.archive.list(&actor_prefix(head)?).await?;
        Ok(keys
            .into_iter()
            .filter_map(|key| Some((parse(&key)?, key)))
            .filter(|(name, _)| name.version <= head.state_version)
            .max_by_key(|(name, _)| name.version)
            .map(|(name, key)| SqliteBase {
                key,
                name,
                bucket: self.archive.clone(),
                downloaded: None,
            }))
    }
}

impl LogStorage {
    /// Stores the closed database at `path` as the base of `snapshot` at SQLite `txid`. A base of
    /// the same version, transaction and size already stored is that base.
    pub(super) async fn upload_base(
        &self,
        snapshot: &SnapshotRef,
        txid: u64,
        path: &std::path::Path,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        let file = tokio::fs::File::open(path).await?.into_std().await;
        let bytes = file.metadata()?.len();
        let key = key(snapshot, txid, bytes)?;
        let payload = Payload::from_file(Arc::new(file), usize::try_from(bytes)?);
        bounded_transfer(bytes, replace(self.archive.as_ref(), &key, None, payload)).await?;
        tracing::info!(
            event = "sqlite_base_uploaded",
            state_version = snapshot.state_version,
            bytes,
            uploader = "host",
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }
}

fn actor_prefix(head: &SnapshotRef) -> Result<String> {
    let (epoch, _) = head
        .object
        .rsplit_once('/')
        .context("invalid snapshot name")?;
    let (actor, _) = epoch.rsplit_once('/').context("invalid snapshot prefix")?;
    prefix(&format!("{actor}/"))
}

pub(crate) fn parse(key: &str) -> Option<BaseName> {
    let (_, name) = key
        .strip_prefix(SNAPSHOTS)?
        .strip_suffix(".sqlite")?
        .split_once(BASES)?;
    let mut parts = name.split('~');
    let name = BaseName {
        owner_epoch: u64::from_str_radix(parts.next()?, 16).ok()?,
        version: parts.next()?.parse().ok()?,
        txid: parts.next()?.parse().ok()?,
        bytes: parts.next()?.parse().ok()?,
    };
    parts.next().is_none().then_some(name)
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid/base.rs"]
mod tests;
