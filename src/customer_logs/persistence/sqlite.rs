use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use rusqlite::{Connection, params};

use super::{LogPersistence, LogReport};
use crate::{
    clock::{Clock, SystemClock},
    customer_logs::{AttributedLog, LogPage, LogQuery},
};

pub(crate) struct SqliteLogPersistence {
    path: PathBuf,
    connection: Arc<Mutex<Option<Connection>>>,
    retention: Duration,
}

impl SqliteLogPersistence {
    pub(crate) fn new(path: PathBuf, retention: Duration) -> Self {
        Self {
            path,
            connection: Arc::default(),
            retention,
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let shared = self.connection.clone();
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut shared = shared.lock().unwrap();
            if shared.is_none() {
                let connection = Connection::open(path)?;
                connection.busy_timeout(Duration::from_secs(5))?;
                connection.execute_batch(include_str!("sqlite.sql"))?;
                *shared = Some(connection);
            }
            operation(shared.as_mut().unwrap())
        })
        .await?
    }
}

#[async_trait]
impl LogPersistence for SqliteLogPersistence {
    async fn initialize(&self) -> Result<()> {
        self.run(|_| Ok(())).await
    }

    async fn append(&self, report: &LogReport) -> Result<()> {
        let report = report.clone();
        let cutoff = SystemClock
            .now_ms()?
            .saturating_sub(self.retention.as_millis() as u64);
        self.run(move |connection| {
            let tx = connection.transaction()?;
            for record in &report.records {
                tx.execute("INSERT OR IGNORE INTO actor_logs(project_id,event_id,timestamp_ms,actor_name,actor_id,request_id,level,message,event_json,received_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)", params![record.project_id, record.log.event_id, record.log.timestamp_ms as i64, record.actor_name, record.actor_id, record.log.request_id, record.log.level, record.log.message, serde_json::to_string(record)?, report.received_at_ms as i64])?;
            }
            if report.dropped > 0 {
            tx.execute("INSERT OR IGNORE INTO actor_log_status(project_id,report_id,dropped,received_at_ms) VALUES(?1,?2,?3,?4)", params![report.project, report.id, report.dropped.min(i64::MAX as u64) as i64, report.received_at_ms as i64])?;
            }
            tx.execute("DELETE FROM actor_logs WHERE received_at_ms < ?1", [cutoff as i64])?;
            tx.execute("DELETE FROM actor_logs WHERE rowid IN (SELECT rowid FROM actor_logs ORDER BY received_at_ms DESC LIMIT -1 OFFSET 10000)", [])?;
            tx.execute("DELETE FROM actor_log_status WHERE received_at_ms < ?1", [cutoff as i64])?;
            tx.commit()?;
            Ok(())
        }).await
    }

    async fn history(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        let project = project.to_owned();
        let query = query.clone();
        let after = query.after(&project)?;
        let cutoff = SystemClock
            .now_ms()?
            .saturating_sub(self.retention.as_millis() as u64);
        self.run(move |connection| {
            let mut statement = connection.prepare("SELECT event_json FROM actor_logs WHERE project_id=?1 AND received_at_ms>=?2 AND (?3 IS NULL OR actor_name=?3) AND (?4 IS NULL OR actor_id=?4) AND (?5 IS NULL OR request_id=?5) AND (?6 IS NULL OR level=?6) AND (?7 IS NULL OR instr(message,?7)>0) AND timestamp_ms BETWEEN ?8 AND ?9 AND (?10 IS NULL OR (timestamp_ms,event_id)<(?10,?11)) ORDER BY timestamp_ms DESC,event_id DESC LIMIT ?12")?;
            let rows = statement.query_map(params![project, cutoff as i64, query.actor_name, query.actor_id, query.request_id, query.level, query.search, query.from_ms.unwrap_or(0) as i64, query.to_ms.unwrap_or(i64::MAX as u64) as i64, after.as_ref().map(|cursor| cursor.time as i64), after.as_ref().map(|cursor| cursor.id.as_str()), (query.limit + 1) as i64], |row| row.get::<_, String>(0))?;
            let records = rows.map(|row| Ok(serde_json::from_str::<AttributedLog>(&row?)?)).collect::<Result<Vec<_>>>()?;
            let mut page = query.finish(&project, records)?;
            page.dropped = connection.query_row("SELECT COALESCE(sum(dropped),0) FROM actor_log_status WHERE project_id=?1 AND received_at_ms>=?2", params![project, cutoff as i64], |row| row.get::<_, i64>(0))? as u64;
            Ok(page)
        }).await
    }
}
