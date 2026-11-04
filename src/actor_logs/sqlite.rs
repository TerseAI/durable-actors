use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{Connection, Transaction, params};

use super::{LogEvent, LogFilter, LogPage, LogPersistence, LogPosition};

const RETAINED_LINES: i64 = 100_000;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS logs (
    position INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    event TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_actor
    ON logs (project_id, actor_name, actor_id, timestamp_ms, session_id, sequence);
";

const INSERT: &str = "
INSERT INTO logs (project_id, actor_name, actor_id, session_id, sequence, timestamp_ms, event)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

const PRUNE: &str = "DELETE FROM logs WHERE position <= (SELECT MAX(position) FROM logs) - ?1";

const PAGE: &str = "
SELECT event FROM logs
WHERE project_id = ?1 AND actor_name = ?2 AND actor_id = ?3
  AND (?4 IS NULL OR session_id = ?4)
  AND (?5 IS NULL OR (timestamp_ms, session_id, sequence) < (?5, ?6, ?7))
ORDER BY timestamp_ms DESC, session_id DESC, sequence DESC
LIMIT ?8";

pub(crate) struct SqliteLogs {
    connection: Arc<Mutex<Connection>>,
    retained_lines: i64,
}

impl SqliteLogs {
    pub(crate) async fn open(path: PathBuf) -> Result<Self> {
        Self::open_retaining(path, RETAINED_LINES).await
    }

    async fn open_retaining(path: PathBuf, retained_lines: i64) -> Result<Self> {
        let connection = tokio::task::spawn_blocking(move || {
            let connection = Connection::open(path).context("open local actor logs")?;
            connection.execute_batch(SCHEMA)?;
            anyhow::Ok(connection)
        })
        .await??;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            retained_lines,
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || operation(&mut connection.lock().unwrap())).await?
    }
}

#[async_trait]
impl LogPersistence for SqliteLogs {
    async fn append(&self, events: Vec<LogEvent>) -> Result<()> {
        let retained_lines = self.retained_lines;
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            insert(&transaction, &events)?;
            transaction.execute(PRUNE, [retained_lines])?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    async fn page(&self, filter: &LogFilter) -> Result<LogPage> {
        let filter = filter.clone();
        self.run(move |connection| read_page(connection, &filter))
            .await
    }
}

fn insert(transaction: &Transaction<'_>, events: &[LogEvent]) -> Result<()> {
    let mut statement = transaction.prepare(INSERT)?;
    for event in events {
        statement.execute(params![
            event.project_id,
            event.actor_name,
            event.actor_id,
            event.session_id,
            integer(event.line.sequence),
            integer(event.line.timestamp_ms),
            serde_json::to_string(event)?,
        ])?;
    }
    Ok(())
}

fn read_page(connection: &Connection, filter: &LogFilter) -> Result<LogPage> {
    let before = filter.before.as_ref();
    let mut statement = connection.prepare(PAGE)?;
    let mut lines = statement
        .query_map(
            params![
                filter.actor.project_id,
                filter.actor.actor_name,
                filter.actor.actor_id,
                filter.session_id,
                before.map(|position| integer(position.timestamp_ms)),
                before.map(|position| position.session_id.as_str()),
                before.map(|position| integer(position.sequence)),
                integer(filter.limit as u64 + 1),
            ],
            |row| row.get::<_, String>(0),
        )?
        .map(|event| -> Result<LogEvent> { Ok(serde_json::from_str(&event?)?) })
        .collect::<Result<Vec<_>>>()?;
    let older = lines.len() > filter.limit;
    lines.truncate(filter.limit);
    lines.reverse();
    let before = older.then(|| LogPosition::of(&lines[0]).to_string());
    Ok(LogPage { lines, before })
}

fn integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[path = "../../tests/unit/actor_logs/sqlite.rs"]
mod tests;
