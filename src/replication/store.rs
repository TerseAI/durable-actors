use std::path::PathBuf;

use anyhow::{Result, ensure};
use tokio_rusqlite::{
    Connection,
    rusqlite::{OptionalExtension, params},
};

pub struct ReplicaStore {
    connection: Connection,
    capacity: i64,
}

pub struct PendingSnapshot {
    pub object: String,
    pub archive_url: String,
    pub bytes: Vec<u8>,
    pub created_at_ms: i64,
}

impl ReplicaStore {
    pub async fn open(path: PathBuf, capacity: u64) -> Result<Self> {
        ensure!(capacity > 0, "replica spool capacity must be positive");
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let connection = Connection::open(path).await?;
        connection
            .call::<_, _, tokio_rusqlite::rusqlite::Error>(|db| {
                db.execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS snapshots (
                    object TEXT PRIMARY KEY, archive_url TEXT NOT NULL, bytes BLOB NOT NULL,
                    created_at_ms INTEGER NOT NULL, last_attempt_ms INTEGER NOT NULL DEFAULT 0
                );",
                )?;
                Ok(())
            })
            .await?;
        Ok(Self {
            connection,
            capacity: i64::try_from(capacity)?,
        })
    }

    pub async fn put(&self, object: &str, archive_url: &str, bytes: &[u8]) -> Result<()> {
        let object = object.to_owned();
        let archive_url = archive_url.to_owned();
        let bytes = bytes.to_vec();
        let capacity = self.capacity;
        let outcome = self.connection.call::<_, _, tokio_rusqlite::rusqlite::Error>(move |db| {
            let transaction = db.transaction()?;
            let existing: Option<Vec<u8>> = transaction.query_row(
                "SELECT bytes FROM snapshots WHERE object = ?1", [&object], |row| row.get(0)
            ).optional()?;
            if let Some(existing) = existing {
                return Ok(if existing == bytes { Ok(()) } else { Err("conflicting immutable snapshot") });
            }
            let used: i64 = transaction.query_row("SELECT COALESCE(SUM(length(bytes)), 0) FROM snapshots", [], |row| row.get(0))?;
            if used.saturating_add(bytes.len() as i64) > capacity {
                return Ok(Err("replica spool is full"));
            }
            transaction.execute(
                "INSERT INTO snapshots (object, archive_url, bytes, created_at_ms) VALUES (?1, ?2, ?3, CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))",
                params![object, archive_url, bytes],
            )?;
            transaction.commit()?;
            Ok(Ok(()))
        }).await?;
        outcome.map_err(anyhow::Error::msg)
    }

    pub async fn read(&self, object: &str) -> Result<Option<Vec<u8>>> {
        let object = object.to_owned();
        Ok(self
            .connection
            .call::<_, _, tokio_rusqlite::rusqlite::Error>(move |db| {
                db.query_row(
                    "SELECT bytes FROM snapshots WHERE object = ?1",
                    [object],
                    |row| row.get(0),
                )
                .optional()
            })
            .await?)
    }

    pub async fn pending(&self, limit: u32) -> Result<Vec<PendingSnapshot>> {
        Ok(self.connection.call::<_, _, tokio_rusqlite::rusqlite::Error>(move |db| {
            let mut statement = db.prepare("SELECT object, archive_url, bytes, created_at_ms FROM snapshots ORDER BY last_attempt_ms, created_at_ms LIMIT ?1")?;
            statement.query_map([limit], |row| Ok(PendingSnapshot {
                object: row.get(0)?, archive_url: row.get(1)?, bytes: row.get(2)?, created_at_ms: row.get(3)?,
            }))?.collect::<Result<Vec<_>, _>>()
        }).await?)
    }

    pub async fn archived(&self, object: &str) -> Result<()> {
        let object = object.to_owned();
        self.connection
            .call::<_, _, tokio_rusqlite::rusqlite::Error>(move |db| {
                db.execute("DELETE FROM snapshots WHERE object = ?1", [object])?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn attempted(&self, object: &str) -> Result<()> {
        let object = object.to_owned();
        self.connection.call::<_, _, tokio_rusqlite::rusqlite::Error>(move |db| {
            db.execute("UPDATE snapshots SET last_attempt_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) WHERE object = ?1", [object])?;
            Ok(())
        }).await?;
        Ok(())
    }
}
