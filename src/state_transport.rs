use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;

use crate::storage::STATE_CONTENT_TYPE;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateWrite {
    Written,
    AlreadyExists,
    Replicated,
}

#[async_trait]
pub trait StateTransport: Send + Sync {
    async fn read(&self, signed_url: &str) -> Result<Bytes>;
    async fn write(&self, signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite>;
}

#[async_trait]
pub trait SnapshotWriter: Send + Sync {
    async fn write_snapshot(
        &self,
        plan: &crate::storage::WritePlan,
        bytes: Vec<u8>,
    ) -> Result<StateWrite>;
}

#[derive(Clone)]
pub struct HttpStateTransport {
    client: reqwest::Client,
}

impl HttpStateTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(25))
                .build()
                .expect("valid state transport client configuration"),
        }
    }
}

impl Default for HttpStateTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateTransport for HttpStateTransport {
    async fn read(&self, signed_url: &str) -> Result<Bytes> {
        validate_url(signed_url)?;
        let response = self
            .client
            .get(signed_url)
            .send()
            .await
            .context("read actor state through signed URL")?;
        ensure!(
            response.status().is_success(),
            "actor-state read failed with HTTP {}",
            response.status()
        );
        response
            .bytes()
            .await
            .context("read actor-state response body")
    }

    async fn write(&self, signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
        validate_url(signed_url)?;
        let response = self
            .client
            .put(signed_url)
            .header(reqwest::header::CONTENT_TYPE, STATE_CONTENT_TYPE)
            .body(bytes)
            .send()
            .await
            .context("write actor state through signed URL")?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(StateWrite::AlreadyExists);
        }
        ensure!(
            response.status().is_success(),
            "actor-state write failed with HTTP {}",
            response.status()
        );
        Ok(StateWrite::Written)
    }
}

fn validate_url(url: &str) -> Result<()> {
    let url = reqwest::Url::parse(url).context("parse signed actor-state URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "signed actor-state URL must be HTTP or HTTPS"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_http_state_urls() {
        assert!(validate_url("file:///tmp/state").is_err());
    }
}
