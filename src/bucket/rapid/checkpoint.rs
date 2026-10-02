use super::*;
use crate::bucket::recovery::{Checkpoint, SnapshotHistory, resolve};
use crate::state_log::{SqliteSnapshot, StateSnapshot};
use crate::storage::SnapshotRef;

pub(super) const CHECKPOINT_INTERVAL: u64 = 32;

impl LogStorage {
    pub async fn restore(&self, object: &str, bytes: Bytes) -> Result<SqliteSnapshot> {
        resolve(
            &mut LogHistory {
                storage: self,
                epochs: BTreeMap::new(),
            },
            object,
            bytes,
        )
        .await
    }

    pub async fn checkpoint(&self, object: &str, bytes: Bytes) -> Result<()> {
        let started = std::time::Instant::now();
        let snapshot = StateSnapshot::decode(&bytes)?;
        if snapshot.sqlite.parent.is_none() {
            return Ok(());
        }
        let source = SnapshotRef::new(object.into(), &snapshot, &bytes);
        if let Some(bytes) = self.snapshots.get(&format!("{object}.checkpoint")).await? {
            Checkpoint::decode(&bytes, &source, snapshot.sqlite.txid)?;
            return Ok(());
        }
        let sqlite = self.restore(object, bytes).await?;
        let input_files = sqlite.files.len();
        let file = self.compactor.compact(&sqlite.files).await?;
        let checkpoint = Checkpoint {
            source,
            sqlite: SqliteSnapshot {
                txid: sqlite.txid,
                parent: None,
                files: vec![file],
            },
        };
        let bytes = serde_json::to_vec(&checkpoint)?;
        let size = bytes.len();
        bounded(
            self.snapshots
                .put(&format!("{object}.checkpoint"), bytes.into()),
        )
        .await?;
        tracing::info!(
            event = "sqlite_checkpoint",
            state_version = snapshot.state_version,
            input_files,
            bytes = size,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }
}

struct LogHistory<'a> {
    storage: &'a LogStorage,
    epochs: BTreeMap<String, EpochHistory>,
}

struct EpochHistory {
    records: BTreeMap<String, Bytes>,
    checkpoints: std::collections::BTreeSet<String>,
}

#[async_trait]
impl SnapshotHistory for LogHistory<'_> {
    async fn read(&mut self, object: &str) -> Result<Bytes> {
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        if let Some(records) = self.epochs.get(prefix) {
            if let Some(bytes) = records.records.get(object) {
                return Ok(bytes.clone());
            }
        }
        if let Some(bytes) = self.storage.snapshots.get(object).await? {
            return Ok(bytes);
        }
        if !self.epochs.contains_key(prefix) {
            let prefix_key = format!("{prefix}/");
            let (records, keys) = tokio::try_join!(
                self.storage.records(&prefix_key, false),
                self.storage.snapshots.list(&prefix_key)
            )?;
            self.epochs.insert(
                prefix.into(),
                EpochHistory {
                    records,
                    checkpoints: keys
                        .into_iter()
                        .filter(|key| key.ends_with(".checkpoint"))
                        .collect(),
                },
            );
        }
        self.epochs
            .get(prefix)
            .and_then(|epoch| epoch.records.get(object))
            .cloned()
            .context("SQLite dependency missing")
    }

    async fn checkpoint(&mut self, object: &str) -> Result<Option<Bytes>> {
        let key = format!("{object}.checkpoint");
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        if self
            .epochs
            .get(prefix)
            .is_some_and(|epoch| !epoch.checkpoints.contains(&key))
        {
            return Ok(None);
        }
        self.storage.snapshots.get(&key).await
    }
}
