use super::{
    bigquery::{BigQueryReader, transport::BigQueryTransport},
    pubsub::{GooglePublisher, PubSubTraceSink},
};
use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub struct AnalyticsConfig {
    pub topic: String,
    pub table: String,
    pub query_project: String,
    pub location: String,
    pub environment: String,
    pub metadata_fields: Vec<String>,
    pub retention_days: u32,
    pub maximum_bytes_billed: u64,
    pub cache_seconds: u64,
}

impl AnalyticsConfig {
    pub(crate) fn from_lookup(
        get: &mut impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<Self>> {
        let mut values = BTreeMap::new();
        for name in [
            "PUBSUB_TOPIC",
            "BQ_TABLE",
            "BQ_PROJECT",
            "BQ_LOCATION",
            "ENVIRONMENT",
            "METADATA_FIELDS",
            "RETENTION_DAYS",
            "MAXIMUM_BYTES_BILLED",
            "CACHE_SECONDS",
        ] {
            if let Some(value) = get(&format!("DURABLE_ACTORS_ANALYTICS_{name}")) {
                values.insert(name, value);
            }
        }
        if values.is_empty() {
            return Ok(None);
        }
        let required = |name| {
            values
                .get(name)
                .cloned()
                .with_context(|| format!("DURABLE_ACTORS_ANALYTICS_{name} is required"))
        };
        let config = Self {
            topic: required("PUBSUB_TOPIC")?,
            table: required("BQ_TABLE")?,
            query_project: required("BQ_PROJECT")?,
            location: required("BQ_LOCATION")?,
            environment: required("ENVIRONMENT")?,
            metadata_fields: values
                .get("METADATA_FIELDS")
                .map(|value| value.split(',').map(|s| s.trim().to_owned()).collect())
                .unwrap_or_default(),
            retention_days: values
                .get("RETENTION_DAYS")
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(30),
            maximum_bytes_billed: values
                .get("MAXIMUM_BYTES_BILLED")
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(1_073_741_824),
            cache_seconds: values
                .get("CACHE_SECONDS")
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(15),
        };
        config.validate()?;
        Ok(Some(config))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let topic: Vec<_> = self.topic.split('/').collect();
        ensure!(
            topic.len() == 4
                && topic[0] == "projects"
                && topic[2] == "topics"
                && identifier(topic[1], true)
                && identifier(topic[3], true),
            "analytics topic must be projects/PROJECT/topics/TOPIC"
        );
        let table: Vec<_> = self.table.split('.').collect();
        ensure!(
            table.len() == 3
                && identifier(table[0], true)
                && table[1..].iter().all(|part| identifier(part, false)),
            "analytics table must be PROJECT.DATASET.TABLE"
        );
        ensure!(
            identifier(&self.query_project, true) && identifier(&self.location, true),
            "invalid analytics query project or location"
        );
        ensure!(
            identifier(&self.environment, true),
            "invalid analytics environment"
        );
        ensure!(
            (1..=365).contains(&self.retention_days),
            "analytics retention must be 1–365 days"
        );
        ensure!(
            (1..=1_099_511_627_776).contains(&self.maximum_bytes_billed),
            "invalid analytics byte budget"
        );
        ensure!(
            (5..=300).contains(&self.cache_seconds),
            "analytics cache interval must be 5–300 seconds"
        );
        ensure!(
            self.metadata_fields.len() <= 32
                && self
                    .metadata_fields
                    .iter()
                    .all(|field| !field.is_empty() && field.len() <= 128),
            "invalid analytics metadata allowlist"
        );
        Ok(())
    }

    pub(crate) async fn build(&self, secret: &str) -> Result<AnalyticsRuntime> {
        self.validate()?;
        ensure!(
            !secret.is_empty(),
            "BigQuery analytics requires DURABLE_ACTORS_SECRET"
        );
        let clock = Arc::new(crate::clock::SystemClock);
        let publisher = Arc::new(GooglePublisher::new(&self.topic).await?);
        let credentials = google_cloud_auth::credentials::Builder::default().build()?;
        let executor = Arc::new(BigQueryTransport::new(
            credentials,
            self.query_project.clone(),
            self.location.clone(),
            self.maximum_bytes_billed,
        )?);
        Ok(AnalyticsRuntime {
            sink: Arc::new(PubSubTraceSink::new(
                publisher,
                clock.clone(),
                self.environment.clone(),
                self.metadata_fields.clone(),
                self.retention_days,
                1024,
            )),
            reader: Arc::new(BigQueryReader::new(
                executor,
                clock,
                self.table.clone(),
                self.environment.clone(),
                self.retention_days,
                Duration::from_secs(self.cache_seconds),
                secret.as_bytes(),
            )),
        })
    }
}

pub(crate) struct AnalyticsRuntime {
    pub sink: Arc<PubSubTraceSink>,
    pub reader: Arc<BigQueryReader>,
}

fn identifier(value: &str, hyphens: bool) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || (hyphens && c == b'-'))
}
