use super::{
    TracePage, TraceRecord,
    history::HistoryQuery,
    metrics::*,
    reader::TraceReader,
    replay::{InvalidTraceCursor, ReplayQuery},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use moka::future::Cache;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(crate) mod transport;

#[derive(Clone, Serialize)]
pub(crate) struct QuerySpec {
    pub sql: String,
    pub parameters: BTreeMap<String, Value>,
    pub limit: u32,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ResultPage {
    pub job_id: String,
    pub page_token: String,
}
#[derive(Clone)]
pub(crate) struct QueryPage {
    pub rows: Vec<Value>,
    pub next: Option<ResultPage>,
}
#[async_trait]
pub(crate) trait QueryExecutor: Send + Sync {
    async fn query(&self, query: QuerySpec) -> Result<QueryPage>;
    async fn page(&self, page: ResultPage, limit: u32) -> Result<QueryPage>;
}
#[derive(Clone, Serialize, Deserialize)]
struct Cursor {
    filters: String,
    page: ResultPage,
    epoch: String,
    offset: u64,
    limit: u32,
    expires_at_ms: u64,
}

pub(crate) struct BigQueryReader {
    executor: Arc<dyn QueryExecutor>,
    clock: Arc<dyn crate::clock::Clock>,
    table: String,
    environment: String,
    retention_days: u32,
    cache_interval: Duration,
    cache: Cache<String, QueryPage>,
    cursor_key: hmac::Key,
    pending: Arc<tokio::sync::Semaphore>,
}
impl BigQueryReader {
    pub(crate) fn new(
        executor: Arc<dyn QueryExecutor>,
        clock: Arc<dyn crate::clock::Clock>,
        table: String,
        environment: String,
        retention_days: u32,
        cache_interval: Duration,
        secret: &[u8],
    ) -> Self {
        Self {
            executor,
            clock,
            table,
            environment,
            retention_days,
            cache_interval,
            cache: Cache::builder()
                .max_capacity(16 * 1024 * 1024)
                .weigher(|key: &String, page: &QueryPage| {
                    (key.len()
                        + page
                            .rows
                            .iter()
                            .map(|row| row.to_string().len())
                            .sum::<usize>()
                        + 1024)
                        .min(u32::MAX as usize) as u32
                })
                .time_to_live(cache_interval)
                .build(),
            cursor_key: hmac::Key::new(hmac::HMAC_SHA256, secret),
            pending: Arc::new(tokio::sync::Semaphore::new(16)),
        }
    }

    async fn read<T: DeserializeOwned>(
        &self,
        template: &str,
        query: &HistoryQuery,
        sessions: bool,
    ) -> Result<Vec<T>> {
        let spec = self.spec(template, query, sessions)?;
        let page = self.run(spec).await?;
        ensure!(
            page.next.is_none(),
            "analytics aggregate result exceeded its response limit"
        );
        page.rows
            .into_iter()
            .map(|row| Ok(serde_json::from_value(row)?))
            .collect()
    }

    async fn run(&self, spec: QuerySpec) -> Result<QueryPage> {
        let key = serde_json::to_string(&spec)?;
        self.cache
            .try_get_with(key, async {
                let permit = self
                    .pending
                    .clone()
                    .try_acquire_owned()
                    .context("analytics query concurrency limit reached")?;
                let executor = self.executor.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    executor.query(spec).await
                })
                .await?
            })
            .await
            .map_err(|error: Arc<anyhow::Error>| anyhow::anyhow!("{error:#}"))
    }

    fn spec(&self, template: &str, query: &HistoryQuery, sessions: bool) -> Result<QuerySpec> {
        query.validate()?;
        let project = query
            .project_id
            .as_ref()
            .context("analytics requires a project scope")?;
        let now = self.clock.now_ms()?;
        let bucket = self.cache_interval.as_millis() as u64;
        let to = query.to_ms.unwrap_or(now / bucket * bucket);
        let from = query
            .from_ms
            .unwrap_or(to.saturating_sub(u64::from(self.retention_days) * 86_400_000));
        ensure!(
            from <= to && to - from <= u64::from(self.retention_days) * 86_400_000,
            "analytics range exceeds configured retention"
        );
        ensure!(
            to <= now.saturating_add(300_000),
            "analytics range exceeds clock skew allowance"
        );
        let (scan_from, scan_to) = if sessions {
            (
                now.saturating_sub(u64::from(self.retention_days) * 86_400_000),
                now / bucket * bucket,
            )
        } else {
            (from, to)
        };
        let parameters = BTreeMap::from([
            ("project_id".into(), Value::from(project.clone())),
            ("environment".into(), Value::from(self.environment.clone())),
            ("from_ms".into(), Value::from(from)),
            ("to_ms".into(), Value::from(to)),
            ("scan_from_ms".into(), Value::from(scan_from)),
            ("scan_to_ms".into(), Value::from(scan_to)),
            (
                "actor_name".into(),
                serde_json::to_value(&query.actor_name)?,
            ),
            ("actor_id".into(), serde_json::to_value(&query.actor_id)?),
            ("outcome".into(), serde_json::to_value(query.outcome)?),
        ]);
        Ok(QuerySpec {
            sql: format!(
                "{}\n{template}",
                include_str!("bigquery/events.sql").replace("{{table}}", &self.table)
            ),
            parameters,
            limit: if template == include_str!("bigquery/history.sql") {
                query.limit as u32
            } else {
                500
            },
        })
    }

    async fn history_page(&self, query: &HistoryQuery) -> Result<TracePage> {
        query.validate()?;
        let filters = format!(
            "v1:{}:{}:{}:{}",
            self.table,
            self.environment,
            query.filter_key()?,
            query.limit
        );
        if let Some(id) = &query.cursor {
            let cursor = self.decode_cursor(id)?;
            ensure!(cursor.filters == filters, InvalidTraceCursor);
            if cursor.expires_at_ms >= self.clock.now_ms()? {
                let key = format!("page:{id}");
                let page = self
                    .cache
                    .try_get_with(key, async {
                        let permit = self
                            .pending
                            .clone()
                            .try_acquire_owned()
                            .context("analytics query concurrency limit reached")?;
                        let executor = self.executor.clone();
                        let next = cursor.page.clone();
                        let limit = cursor.limit;
                        tokio::spawn(async move {
                            let _permit = permit;
                            executor.page(next, limit).await
                        })
                        .await?
                    })
                    .await
                    .map_err(|error: Arc<anyhow::Error>| anyhow::anyhow!("{error:#}"))?;
                return self
                    .history_response(
                        page,
                        filters,
                        cursor.epoch,
                        cursor.offset,
                        query.limit,
                        false,
                    )
                    .await;
            }
        }
        let spec = self.spec(include_str!("bigquery/history.sql"), query, false)?;
        let page = self.run(spec).await?;
        self.history_response(
            page,
            filters,
            uuid::Uuid::new_v4().to_string(),
            0,
            query.limit,
            query.cursor.is_some(),
        )
        .await
    }

    fn encode_cursor(&self, cursor: &Cursor) -> Result<String> {
        let bytes = serde_json::to_vec(cursor)?;
        let signature = hmac::sign(&self.cursor_key, &bytes);
        let value = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        );
        ensure!(value.len() <= 4096, "BigQuery cursor exceeds maximum size");
        Ok(value)
    }

    fn decode_cursor(&self, value: &str) -> Result<Cursor> {
        let (payload, signature) = value.split_once('.').ok_or(InvalidTraceCursor)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| InvalidTraceCursor)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| InvalidTraceCursor)?;
        hmac::verify(&self.cursor_key, &bytes, &signature).map_err(|_| InvalidTraceCursor)?;
        Ok(serde_json::from_slice(&bytes).map_err(|_| InvalidTraceCursor)?)
    }

    async fn history_response(
        &self,
        page: QueryPage,
        filters: String,
        epoch: String,
        offset: u64,
        limit: usize,
        reset: bool,
    ) -> Result<TracePage> {
        let records: Vec<TraceRecord> = page
            .rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| {
                Ok(TraceRecord {
                    sequence: offset + i as u64 + 1,
                    event: serde_json::from_value(row)?,
                })
            })
            .collect::<Result<_>>()?;
        let cursor = offset + records.len() as u64;
        let next_cursor = if let Some(page) = page.next {
            Some(self.encode_cursor(&Cursor {
                filters,
                page,
                epoch: epoch.clone(),
                offset: cursor,
                limit: limit as u32,
                expires_at_ms: self.clock.now_ms()?.saturating_add(600_000),
            })?)
        } else {
            None
        };
        Ok(TracePage {
            epoch: epoch.clone(),
            cursor,
            capacity: limit,
            evicted: 0,
            dropped: 0,
            persistence_failed: false,
            records,
            next_cursor,
            resume_cursor: epoch,
            reset,
        })
    }
}

#[async_trait]
impl TraceReader for BigQueryReader {
    async fn history(&self, query: &HistoryQuery) -> Result<TracePage> {
        self.history_page(query).await
    }
    async fn metrics(&self, query: &TimeRange) -> Result<OverviewMetrics> {
        let rows: Vec<ClassMetrics> = self
            .read(
                include_str!("bigquery/overview.sql"),
                &query.history(),
                false,
            )
            .await?;
        let mut result = OverviewMetrics::default();
        for row in rows {
            if row.actor_name.is_empty() {
                result.total = row;
            } else {
                result.classes.push(row);
            }
        }
        Ok(result)
    }
    async fn queue_waits(&self, query: &QueueWaitQuery) -> Result<Vec<QueueWaitRow>> {
        self.read(
            include_str!("bigquery/queue_waits.sql"),
            &HistoryQuery {
                actor_name: query.actor_name.clone(),
                project_id: query.project_id.clone(),
                from_ms: query.from_ms,
                to_ms: query.to_ms,
                ..Default::default()
            },
            false,
        )
        .await
    }
    async fn websockets(&self, query: &TimeRange) -> Result<Vec<SocketSession>> {
        self.read(
            include_str!("bigquery/websockets.sql"),
            &query.history(),
            true,
        )
        .await
    }
    async fn replay(&self, query: &ReplayQuery) -> Result<TracePage> {
        query.validate()?;
        let mut page = self
            .history_page(&HistoryQuery {
                project_id: query.project_id.clone(),
                limit: query.limit,
                ..Default::default()
            })
            .await?;
        // The existing SSE contract supports replacing a snapshot; BQ has no ordered replay log.
        page.reset = true;
        page.next_cursor = None;
        Ok(page)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/request_traces/bigquery/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/request_traces/bigquery/live.rs"]
mod live;
