use std::sync::Arc;

use anyhow::{Result, ensure};
use aws_lc_rs::digest::{Context as Digest, SHA256, digest};
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::TryStreamExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::{MAX_ENTRY_BYTES, keys::HostTokens, store::DiskStore, valid_digest};
use crate::{actor::ActorKey, control_plane::ActorPrincipal};

#[derive(Clone)]
struct NodeCache {
    store: Arc<DiskStore>,
    tokens: Arc<HostTokens>,
}

pub(super) fn router(store: Arc<DiskStore>, tokens: Arc<HostTokens>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/code/{digest}", get(read_code).put(write_code))
        .route("/sqlite/{digest}", get(read_database).put(write_database))
        .with_state(NodeCache { store, tokens })
}

async fn read_code(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Response {
    match cache.authorize(&headers, &digest) {
        Ok(_) => cache.serve(&code_key(&digest)).await,
        Err(status) => status.into_response(),
    }
}

async fn write_code(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> StatusCode {
    match cache.authorize(&headers, &digest) {
        Ok(_) => cache.receive(&code_key(&digest), body, Some(&digest)).await,
        Err(status) => status,
    }
}

async fn read_database(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Response {
    match cache.authorize(&headers, &digest) {
        Ok(host) => cache.serve(&database_key(&host.actor, &digest)).await,
        Err(status) => status.into_response(),
    }
}

async fn write_database(
    State(cache): State<NodeCache>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> StatusCode {
    match cache.authorize(&headers, &digest) {
        Ok(host) => {
            cache
                .receive(&database_key(&host.actor, &digest), body, None)
                .await
        }
        Err(status) => status,
    }
}

impl NodeCache {
    fn authorize(&self, headers: &HeaderMap, digest: &str) -> Result<ActorPrincipal, StatusCode> {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let host = self
            .tokens
            .verify(authorization)
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        if !valid_digest(digest) {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok(host)
    }

    async fn serve(&self, key: &str) -> Response {
        match self.store.get(key).await {
            Some((file, size)) => (
                [(header::CONTENT_LENGTH, size)],
                Body::from_stream(ReaderStream::new(file)),
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn receive(&self, key: &str, body: Body, digest: Option<&str>) -> StatusCode {
        match self.store_body(key, body, digest).await {
            Ok(()) => StatusCode::CREATED,
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "rejected node cache upload");
                StatusCode::BAD_REQUEST
            }
        }
    }

    /// Stores the body under `key`, first checking it hashes to `digest` when one is given.
    async fn store_body(&self, key: &str, body: Body, digest: Option<&str>) -> Result<()> {
        let staged = self.store.stage()?;
        let mut file = tokio::fs::File::from_std(staged.reopen()?);
        let mut hash = Digest::new(&SHA256);
        let mut size = 0u64;
        let mut chunks = body.into_data_stream();
        while let Some(chunk) = chunks.try_next().await? {
            size += chunk.len() as u64;
            ensure!(size <= MAX_ENTRY_BYTES, "cache entry is too large");
            hash.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        ensure!(
            digest.is_none_or(|digest| URL_SAFE_NO_PAD.encode(hash.finish().as_ref()) == digest),
            "content digest mismatch"
        );
        drop(file);
        self.store.insert(key, staged, size).await
    }
}

fn code_key(digest: &str) -> String {
    format!("code/{digest}")
}

/// Databases are keyed by the actor in the host token, so a host can neither read nor replace
/// another actor's database.
fn database_key(actor: &ActorKey, snapshot: &str) -> String {
    let scope = format!("{}\n{snapshot}", actor.storage_key().as_str());
    format!(
        "sqlite/{}",
        URL_SAFE_NO_PAD.encode(digest(&SHA256, scope.as_bytes()).as_ref())
    )
}
