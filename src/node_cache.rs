use std::{future::Future, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::net::TcpListener;

mod client;
mod keys;
mod server;
mod store;

pub(crate) use client::{DatabaseDownload, NodeCacheClient, NodeCacheConnection};

const MAX_ENTRY_BYTES: u64 = 512 * 1024 * 1024;
/// Larger databases take longer to export at shutdown than they save at the next activation.
pub(crate) const MAX_DATABASE_BYTES: u64 = 256 * 1024 * 1024;

/// Serves actor code and databases from this node's disk to the sandboxes scheduled on it.
pub async fn serve_node_cache(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let config = NodeCacheConfig::from_lookup(|name| std::env::var(name).ok())?;
    let store = Arc::new(store::DiskStore::open(config.directory, config.capacity_bytes).await?);
    let tokens = keys::HostTokens::load(config.verifier);
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, "node cache is ready");
    axum::serve(listener, server::router(store, tokens))
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve node cache")
}

struct NodeCacheConfig {
    bind: SocketAddr,
    directory: PathBuf,
    capacity_bytes: u64,
    verifier: keys::VerifierConfig,
}

impl NodeCacheConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let capacity_bytes = get("DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES")
            .context("DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES is required")?
            .parse()
            .context("DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES must be an integer")?;
        ensure!(
            capacity_bytes > 0,
            "DURABLE_ACTORS_NODE_CACHE_CAPACITY_BYTES must be positive"
        );
        Ok(Self {
            bind: get("DURABLE_ACTORS_NODE_CACHE_BIND")
                .unwrap_or_else(|| "0.0.0.0:7103".into())
                .parse()
                .context("DURABLE_ACTORS_NODE_CACHE_BIND must be a socket address")?,
            directory: get("DURABLE_ACTORS_NODE_CACHE_DIR")
                .unwrap_or_else(|| "/cache".into())
                .into(),
            capacity_bytes,
            verifier: keys::VerifierConfig {
                jwks_url: format!(
                    "{}{}",
                    get("DURABLE_ACTORS_CONTROL_PLANE_URL")
                        .context("DURABLE_ACTORS_CONTROL_PLANE_URL is required")?
                        .trim_end_matches('/'),
                    crate::control_plane::JWKS_PATH
                ),
                issuer: get("DURABLE_ACTORS_JWT_ISSUER")
                    .unwrap_or_else(|| crate::control_plane::DEFAULT_JWT_ISSUER.into()),
                audience: get("DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE")
                    .unwrap_or_else(|| crate::control_plane::DEFAULT_AUTHORITY_AUDIENCE.into()),
                max_lifetime: Duration::from_secs(
                    get("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS")
                        .map(|value| value.parse())
                        .transpose()
                        .context("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS must be an integer")?
                        .unwrap_or(crate::control_plane::DEFAULT_JWT_TTL_SECONDS),
                ),
            },
        })
    }
}

fn valid_digest(digest: &str) -> bool {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD
        .decode(digest)
        .is_ok_and(|bytes| bytes.len() == 32)
}

#[cfg(test)]
#[path = "../tests/unit/node_cache.rs"]
mod tests;
