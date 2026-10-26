use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use super::{LogPersistence, LogReport};
use crate::{
    clock::{Clock, SystemClock},
    customer_logs::{LogPage, LogQuery},
    postgres::PostgresDatabase,
};

pub(crate) struct PostgresLogPersistence {
    database: PostgresDatabase,
    retention: Duration,
}

impl PostgresLogPersistence {
    pub(crate) fn new(database: PostgresDatabase, retention: Duration) -> Self {
        Self {
            database,
            retention,
        }
    }
    fn cutoff(&self) -> Result<i64> {
        Ok(SystemClock
            .now_ms()?
            .saturating_sub(self.retention.as_millis() as u64) as i64)
    }
}

#[async_trait]
impl LogPersistence for PostgresLogPersistence {
    async fn initialize(&self) -> Result<()> {
        let _connection = self.database.connection().await?;
        Ok(())
    }

    async fn append(&self, report: &LogReport) -> Result<()> {
        let mut connection = self.database.connection().await?;
        let tx = connection.transaction().await?;
        let received = report.received_at_ms as i64;
        for record in &report.records {
            tx.execute("INSERT INTO durable_actors_customer_logs(project_id,event_id,timestamp_ms,actor_name,actor_id,request_id,level,message,event,received_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING", &[&record.project_id, &record.log.event_id, &(record.log.timestamp_ms as i64), &record.actor_name, &record.actor_id, &record.log.request_id, &record.log.level, &record.log.message, &serde_json::to_value(record)?, &received]).await?;
        }
        if report.dropped > 0 {
            tx.execute("INSERT INTO durable_actors_customer_log_status(project_id,report_id,dropped,received_at_ms) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", &[&report.project, &report.id, &(report.dropped.min(i64::MAX as u64) as i64), &received]).await?;
        }
        let cutoff = self.cutoff()?;
        tx.execute("DELETE FROM durable_actors_customer_logs WHERE ctid IN (SELECT ctid FROM durable_actors_customer_logs WHERE received_at_ms<$1 LIMIT 1000)", &[&cutoff]).await?;
        tx.execute("DELETE FROM durable_actors_customer_log_status WHERE ctid IN (SELECT ctid FROM durable_actors_customer_log_status WHERE received_at_ms<$1 LIMIT 1000)", &[&cutoff]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn history(&self, project: &str, query: &LogQuery) -> Result<LogPage> {
        let after = query.after(project)?;
        let connection = self.database.connection().await?;
        let rows = connection.query("SELECT event FROM durable_actors_customer_logs WHERE project_id=$1 AND received_at_ms>=$2 AND ($3::text IS NULL OR actor_name=$3) AND ($4::text IS NULL OR actor_id=$4) AND ($5::text IS NULL OR request_id=$5) AND ($6::text IS NULL OR level=$6) AND ($7::text IS NULL OR strpos(message,$7)>0) AND timestamp_ms BETWEEN $8 AND $9 AND ($10::bigint IS NULL OR (timestamp_ms,event_id)<($10,$11)) ORDER BY timestamp_ms DESC,event_id DESC LIMIT $12", &[&project, &self.cutoff()?, &query.actor_name, &query.actor_id, &query.request_id, &query.level, &query.search, &(query.from_ms.unwrap_or(0) as i64), &(query.to_ms.unwrap_or(i64::MAX as u64) as i64), &after.as_ref().map(|cursor| cursor.time as i64), &after.as_ref().map(|cursor| cursor.id.as_str()), &((query.limit + 1) as i64)]).await?;
        let records = rows
            .iter()
            .map(|row| serde_json::from_value(row.get(0)).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        let mut page = query.finish(project, records)?;
        let row = connection.query_one("SELECT LEAST(COALESCE(sum(dropped),0),9223372036854775807)::bigint FROM durable_actors_customer_log_status WHERE project_id=$1 AND received_at_ms>=$2", &[&project, &self.cutoff()?]).await?;
        page.dropped = row.get::<_, i64>(0) as u64;
        Ok(page)
    }
}
