use std::{
    fs::File,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use rusqlite::{Connection, Transaction, params};
use serde::Deserialize;

use super::{TraceEvent, TracePage, replay::ReplayQuery};
mod replay;
mod sqlite_query;
use super::query::{SqlQuery, SqlResult};

const LOCAL_RETENTION: usize = 10_000;

#[async_trait]
pub(crate) trait TracePersistence: Send + Sync {
    // Events have stable IDs; repeated appends must not duplicate them.
    async fn append(&self, events: &[TraceEvent]) -> Result<()>;
    async fn query(&self, query: &SqlQuery) -> Result<SqlResult>;
    async fn replay(&self, query: &ReplayQuery) -> Result<TracePage>;
}

pub(crate) struct SqliteTracePersistence {
    path: PathBuf,
    retention: usize,
    connection: Arc<Mutex<Option<Connection>>>,
}

impl SqliteTracePersistence {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            retention: LOCAL_RETENTION,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn in_memory() -> Self {
        Self::new(":memory:".into())
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        let shared = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut shared = shared.lock().unwrap();
            if shared.is_none() {
                let mut connection =
                    Connection::open(&path).context("open request trace database")?;
                connection.busy_timeout(Duration::from_secs(5))?;
                initialize(&mut connection, &path)?;
                *shared = Some(connection);
            }
            operation(shared.as_mut().unwrap())
        })
        .await?
    }
}

#[async_trait]
impl TracePersistence for SqliteTracePersistence {
    async fn append(&self, events: &[TraceEvent]) -> Result<()> {
        let events = events.to_vec();
        let retention = self.retention;
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            let inserted = insert_events(&transaction, &events)?;
            transaction.execute("UPDATE trace_meta SET total = total + ?1", [inserted as i64])?;
            transaction.execute(
                "UPDATE trace_meta SET pruned = MAX(pruned, COALESCE((SELECT position FROM traces ORDER BY position DESC LIMIT 1 OFFSET ?1), 0))",
                [retention as i64],
            )?;
            transaction.execute("DELETE FROM traces WHERE position <= (SELECT pruned FROM trace_meta)", [])?;
            transaction.commit()?;
            Ok(())
        }).await
    }

    async fn query(&self, query: &SqlQuery) -> Result<SqlResult> {
        query.validate()?;
        let query = query.clone();
        self.run(move |connection| sqlite_query::query(connection, &query))
            .await
    }

    async fn replay(&self, query: &ReplayQuery) -> Result<TracePage> {
        query.validate()?;
        let query = query.clone();
        self.run(move |connection| replay::query(connection, &query))
            .await
    }
}

fn initialize(connection: &mut Connection, path: &Path) -> Result<()> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    ensure!(version <= 3, "unsupported request trace database version");
    if version == 3 {
        return Ok(());
    }
    let transaction = connection.transaction()?;
    if version == 0 {
        transaction.execute_batch("CREATE TABLE traces (position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL);")?;
        if path != Path::new(":memory:") {
            import_snapshot(&transaction, &path.with_extension("json"))?;
        }
    }
    if version < 2 {
        transaction.execute_batch("
        ALTER TABLE traces ADD COLUMN started_at_ms INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE traces ADD COLUMN actor_type TEXT NOT NULL DEFAULT '';
        ALTER TABLE traces ADD COLUMN actor_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE traces ADD COLUMN outcome TEXT NOT NULL DEFAULT '';
        UPDATE traces SET started_at_ms = json_extract(event, '$.startedAtMs'), actor_type = json_extract(event, '$.actorType'), actor_id = json_extract(event, '$.actorId'), outcome = json_extract(event, '$.outcome');
        CREATE INDEX traces_time ON traces(started_at_ms DESC, position DESC);
        CREATE INDEX traces_actor ON traces(actor_type, actor_id, started_at_ms DESC, position DESC);
        CREATE INDEX traces_outcome ON traces(outcome, started_at_ms DESC, position DESC);
        CREATE TABLE trace_meta (generation TEXT NOT NULL, pruned INTEGER NOT NULL, total INTEGER NOT NULL);
    ")?;
        transaction.execute(
            "INSERT INTO trace_meta VALUES (?1, 0, (SELECT COUNT(*) FROM traces))",
            [uuid::Uuid::new_v4().to_string()],
        )?;
    }
    transaction.execute_batch("
        CREATE VIEW request_events AS SELECT position AS sequence, event_id, started_at_ms, actor_type, actor_id, outcome, event,
            json_extract(event, '$.requestId') AS request_id,
            json_extract(event, '$.hostId') AS host_id,
            json_extract(event, '$.sessionId') AS session_id,
            json_extract(event, '$.kind') AS kind,
            json_extract(event, '$.operation') AS operation,
            json_extract(event, '$.connectionId') AS connection_id,
            json_extract(event, '$.durationMs') AS duration_ms,
            json_extract(event, '$.queueWaitMs') AS queue_wait_ms
        FROM traces;
        CREATE VIEW request_history AS SELECT generation, pruned, total,
            COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'traces'), 0) AS watermark
        FROM trace_meta;
    ")?;
    transaction.pragma_update(None, "user_version", 3)?;
    transaction.commit()?;
    Ok(())
}

fn insert_events(transaction: &Transaction<'_>, events: &[TraceEvent]) -> Result<usize> {
    let mut statement = transaction.prepare("INSERT INTO traces (event_id, event, started_at_ms, actor_type, actor_id, outcome) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT (event_id) DO NOTHING")?;
    let mut inserted = 0;
    for event in events {
        let outcome = serde_json::to_value(event.trace.outcome)?;
        inserted += statement.execute(params![
            event.event_id,
            serde_json::to_string(event)?,
            i64::try_from(event.trace.started_at_ms)?,
            event.trace.actor_type,
            event.trace.actor_id,
            outcome.as_str()
        ])?;
    }
    Ok(inserted)
}

fn import_snapshot(transaction: &Transaction<'_>, path: &Path) -> Result<()> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("open legacy request history"),
    };
    let saved: LegacySnapshot =
        serde_json::from_reader(file).context("parse legacy request history")?;
    ensure!(
        saved.version == 1,
        "unsupported legacy request history version"
    );
    for event in saved.history.records {
        transaction.execute(
            "INSERT INTO traces (event_id, event) VALUES (?1, ?2)",
            params![event.event_id, serde_json::to_string(&event)?],
        )?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct LegacySnapshot {
    version: u32,
    history: LegacyHistory,
}

#[derive(Deserialize)]
struct LegacyHistory {
    records: Vec<TraceEvent>,
}

#[cfg(test)]
#[path = "../../tests/unit/request_traces/persistence/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/request_traces/persistence/sql_tests.rs"]
mod sql_tests;
