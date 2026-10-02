use super::SnapshotStore;
use crate::{
    state_log::{SqliteSnapshot, StateSnapshot},
    storage::SnapshotRef,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub source: SnapshotRef,
    pub sqlite: SqliteSnapshot,
}

#[async_trait]
pub(crate) trait SnapshotHistory: Send {
    async fn read(&mut self, object: &str) -> Result<Bytes>;
    async fn checkpoint(&mut self, object: &str) -> Result<Option<Bytes>>;
}

pub(crate) async fn resolve(
    history: &mut dyn SnapshotHistory,
    object: &str,
    bytes: Bytes,
) -> Result<SqliteSnapshot> {
    let mut current = StateSnapshot::decode(&bytes)?;
    current.validate_object(object)?;
    let txid = current.sqlite.txid;
    let mut reference = SnapshotRef::new(object.into(), &current, &bytes);
    let mut segments = Vec::new();
    loop {
        if current.sqlite.parent.is_some() {
            if let Some(bytes) = history.checkpoint(&reference.object).await? {
                let sqlite = Checkpoint::decode(&bytes, &reference, current.sqlite.txid)?;
                segments.push(sqlite.files);
                break;
            }
        }
        let first = current.sqlite.files[0].first;
        segments.push(current.sqlite.files);
        let Some(parent) = current.sqlite.parent else {
            break;
        };
        let bytes = history.read(&parent.object).await?;
        parent.verify(&bytes)?;
        current = StateSnapshot::decode(&bytes)?;
        current.validate_object(&parent.object)?;
        ensure!(
            current.state_version == parent.state_version
                && current.request_id == parent.request_id,
            "SQLite dependency identity mismatch"
        );
        ensure!(
            current.sqlite.txid.checked_add(1) == Some(first),
            "SQLite dependency transaction gap"
        );
        reference = parent;
    }
    Ok(SqliteSnapshot {
        txid,
        parent: None,
        files: segments.into_iter().rev().flatten().collect(),
    })
}

impl Checkpoint {
    pub fn decode(bytes: &[u8], source: &SnapshotRef, txid: u64) -> Result<SqliteSnapshot> {
        let checkpoint: Self = serde_json::from_slice(bytes)?;
        ensure!(&checkpoint.source == source, "checkpoint source mismatch");
        checkpoint.sqlite.validate(source.state_version)?;
        ensure!(
            checkpoint.sqlite.parent.is_none() && checkpoint.sqlite.txid == txid,
            "checkpoint transaction mismatch"
        );
        Ok(checkpoint.sqlite)
    }
}

pub(super) struct StoredHistory<'a, T: ?Sized>(pub &'a T);

#[async_trait]
impl<T: SnapshotStore + ?Sized> SnapshotHistory for StoredHistory<'_, T> {
    async fn read(&mut self, object: &str) -> Result<Bytes> {
        self.0
            .get(object)
            .await?
            .context("SQLite dependency missing")
    }
    async fn checkpoint(&mut self, object: &str) -> Result<Option<Bytes>> {
        self.0.get(&format!("{object}.checkpoint")).await
    }
}
