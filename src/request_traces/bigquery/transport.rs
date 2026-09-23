use super::{QueryExecutor, QueryPage, QuerySpec, ResultPage};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::credentials::{CacheableResource, Credentials};
use serde_json::{Value, json};
use std::time::Duration;

pub(crate) struct BigQueryTransport {
    client: reqwest::Client,
    credentials: Credentials,
    endpoint: String,
    project: String,
    location: String,
    maximum_bytes_billed: u64,
}
impl BigQueryTransport {
    pub(crate) fn new(
        credentials: Credentials,
        project: String,
        location: String,
        maximum_bytes_billed: u64,
    ) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            credentials,
            endpoint: "https://bigquery.googleapis.com/bigquery/v2".into(),
            project,
            location,
            maximum_bytes_billed,
        })
    }

    async fn execute(&self, job: &str, spec: QuerySpec) -> Result<QueryPage> {
        let body = job_body(
            &self.project,
            &self.location,
            job,
            &spec,
            self.maximum_bytes_billed,
        );
        self.send(
            self.client
                .post(format!("{}/projects/{}/jobs", self.endpoint, self.project))
                .json(&body),
        )
        .await?;
        self.results(
            ResultPage {
                job_id: job.into(),
                page_token: String::new(),
            },
            spec.limit,
        )
        .await
    }

    async fn results(&self, page: ResultPage, limit: u32) -> Result<QueryPage> {
        loop {
            let request = self
                .client
                .get(format!(
                    "{}/projects/{}/queries/{}",
                    self.endpoint, self.project, page.job_id
                ))
                .query(&[
                    ("location", self.location.clone()),
                    ("maxResults", limit.to_string()),
                    ("timeoutMs", "1000".into()),
                    ("pageToken", page.page_token.clone()),
                ]);
            let response = self.send(request).await?;
            ensure!(
                response["errors"]
                    .as_array()
                    .is_none_or(|errors| errors.is_empty()),
                "BigQuery query failed: {}",
                response["errors"]
            );
            if response["jobComplete"] == true {
                tracing::info!(
                    cache_hit = response["cacheHit"].as_bool(),
                    bytes_processed = response["totalBytesProcessed"].as_str(),
                    "analytics query completed"
                );
                return decode_page(&page.job_id, response, limit);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Value> {
        let CacheableResource::New { data: headers, .. } =
            self.credentials.headers(Default::default()).await?
        else {
            anyhow::bail!("credentials returned no headers")
        };
        let mut response = request.headers(headers).send().await?;
        ensure!(
            response.status().is_success(),
            "BigQuery API returned {}",
            response.status()
        );
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                body.len() + chunk.len() <= 8 * 1024 * 1024,
                "BigQuery response exceeds 8 MiB"
            );
            body.extend_from_slice(&chunk);
        }
        Ok(serde_json::from_slice(&body)?)
    }

    async fn cancel(&self, job: &str) {
        let request = self
            .client
            .post(format!(
                "{}/projects/{}/jobs/{job}/cancel",
                self.endpoint, self.project
            ))
            .query(&[("location", &self.location)])
            .json(&json!({}));
        if !matches!(
            tokio::time::timeout(Duration::from_secs(3), self.send(request)).await,
            Ok(Ok(_))
        ) {
            tracing::warn!(
                job_id = job,
                "BigQuery cancellation unconfirmed; server job deadline remains active"
            );
        }
    }
}
#[async_trait]
impl QueryExecutor for BigQueryTransport {
    async fn query(&self, spec: QuerySpec) -> Result<QueryPage> {
        let job = format!("actor_analytics_{}", uuid::Uuid::new_v4().simple());
        let result = tokio::time::timeout(Duration::from_secs(25), self.execute(&job, spec)).await;
        match result {
            Ok(Ok(page)) => Ok(page),
            failure => {
                self.cancel(&job).await;
                failure.context("analytics query deadline exceeded")?
            }
        }
    }
    async fn page(&self, page: ResultPage, limit: u32) -> Result<QueryPage> {
        tokio::time::timeout(Duration::from_secs(15), self.results(page, limit))
            .await
            .context("analytics result deadline exceeded")?
    }
}

fn job_body(
    project: &str,
    location: &str,
    job: &str,
    spec: &QuerySpec,
    maximum_bytes_billed: u64,
) -> Value {
    let parameters: Vec<_> = spec.parameters.iter().map(|(name, value)| {
        let kind = if name.ends_with("_ms") { "INT64" } else { "STRING" };
        let value = if value.is_null() { Value::Null } else { Value::String(value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())) };
        json!({"name": name, "parameterType": {"type": kind}, "parameterValue": {"value": value}})
    }).collect();
    json!({
        "jobReference": {"projectId": project, "location": location, "jobId": job},
        "configuration": {"jobTimeoutMs": "20000", "labels": {"component": "actor-analytics"}, "query": {
            "query": spec.sql, "useLegacySql": false, "useQueryCache": true,
            "maximumBytesBilled": maximum_bytes_billed.to_string(), "parameterMode": "NAMED", "queryParameters": parameters
        }}
    })
}

fn decode_page(job: &str, response: Value, limit: u32) -> Result<QueryPage> {
    let rows = response["rows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    ensure!(
        rows.len() <= limit as usize,
        "BigQuery exceeded requested page size"
    );
    let rows = rows
        .iter()
        .map(|row| {
            let value = row["f"][0]["v"]
                .as_str()
                .context("invalid analytics query result")?;
            Ok(serde_json::from_str(value)?)
        })
        .collect::<Result<_>>()?;
    let next = response["pageToken"]
        .as_str()
        .filter(|token| !token.is_empty())
        .map(|token| ResultPage {
            job_id: job.into(),
            page_token: token.into(),
        });
    Ok(QueryPage { rows, next })
}

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/bigquery/transport.rs"]
mod tests;
