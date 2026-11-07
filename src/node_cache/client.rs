use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{TryStreamExt, stream::BoxStream};
use reqwest::header::AUTHORIZATION;
use tokio::io::AsyncWriteExt;

use crate::control_plane::HostAuthorization;

// With no cache pod on the node the Service drops packets instead of refusing them, so only a
// short connect timeout keeps installs from stalling.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(50);
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Built and connected while a spare waits: building the client alone costs ~15 ms in gVisor, so
/// assignment only attaches the host token.
#[derive(Clone)]
pub(crate) struct NodeCacheConnection {
    http: reqwest::Client,
    url: reqwest::Url,
}

impl NodeCacheConnection {
    pub(crate) async fn warm_from_env() -> Result<Option<Self>> {
        let Some(connection) = Self::from_env()? else {
            return Ok(None);
        };
        connection.preconnect().await;
        Ok(Some(connection))
    }

    pub(crate) fn from_env() -> Result<Option<Self>> {
        match std::env::var("DURABLE_ACTORS_NODE_CACHE_URL") {
            Ok(url) if !url.is_empty() => Ok(Some(Self::new(&url)?)),
            _ => Ok(None),
        }
    }

    pub(crate) fn new(url: &str) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .read_timeout(READ_TIMEOUT)
                .pool_idle_timeout(None)
                .build()?,
            url: reqwest::Url::parse(url).context("node cache URL is invalid")?,
        })
    }

    pub(crate) fn authorize(self, authorization: HostAuthorization) -> NodeCacheClient {
        NodeCacheClient {
            connection: self,
            authorization,
        }
    }

    async fn preconnect(&self) {
        if let Ok(url) = self.url.join("healthz") {
            let _ = self.http.get(url).send().await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct NodeCacheClient {
    connection: NodeCacheConnection,
    authorization: HostAuthorization,
}

impl NodeCacheClient {
    pub(crate) async fn code(
        &self,
        digest: &str,
    ) -> Result<Option<BoxStream<'static, Result<Bytes>>>> {
        self.read(&format!("code/{digest}")).await
    }

    pub(crate) async fn store_code(&self, digest: &str, path: &Path) -> Result<()> {
        self.write(&format!("code/{digest}"), path).await
    }

    /// The actor database saved at the snapshot with this digest, once the cache has answered
    /// but before its body has arrived.
    pub(crate) async fn database(&self, digest: &str) -> Result<Option<DatabaseDownload>> {
        Ok(self
            .read(&format!("sqlite/{digest}"))
            .await?
            .map(DatabaseDownload))
    }

    pub(crate) async fn store_database(&self, digest: &str, path: &Path) -> Result<()> {
        self.write(&format!("sqlite/{digest}"), path).await
    }

    async fn read(&self, entry: &str) -> Result<Option<BoxStream<'static, Result<Bytes>>>> {
        let response = self
            .connection
            .http
            .get(self.connection.url.join(entry)?)
            .header(AUTHORIZATION, self.authorization.header()?)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let chunks = response.error_for_status()?.bytes_stream();
        Ok(Some(Box::pin(chunks.map_err(anyhow::Error::from))))
    }

    async fn write(&self, entry: &str, path: &Path) -> Result<()> {
        let file = tokio::fs::File::open(path).await?;
        self.connection
            .http
            .put(self.connection.url.join(entry)?)
            .header(AUTHORIZATION, self.authorization.header()?)
            .body(reqwest::Body::wrap_stream(
                tokio_util::io::ReaderStream::new(file),
            ))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

pub(crate) struct DatabaseDownload(BoxStream<'static, Result<Bytes>>);

impl DatabaseDownload {
    pub(crate) async fn receive(mut self) -> Result<tempfile::NamedTempFile> {
        let output = tempfile::NamedTempFile::new()?;
        let mut file = tokio::fs::File::from_std(output.reopen()?);
        while let Some(chunk) = self.0.try_next().await? {
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(output)
    }
}
