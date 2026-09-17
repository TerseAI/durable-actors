use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::{hmac, rand::SystemRandom};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use tokio::{fs, io::AsyncWriteExt};

use crate::{actor::ActorKey, clock::Clock};

use super::{StateWriteTicket, StorageUrlSigner, snapshot_object_name};

pub struct LocalStorage {
    root: PathBuf,
    origin: String,
    key: hmac::Key,
    clock: Arc<dyn Clock>,
}

impl LocalStorage {
    pub fn new(root: PathBuf, origin: String, clock: Arc<dyn Clock>) -> Result<Self> {
        let key = hmac::Key::generate(hmac::HMAC_SHA256, &SystemRandom::new())
            .map_err(|_| anyhow::anyhow!("generate local storage signing key"))?;
        Ok(Self {
            root,
            origin,
            key,
            clock,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/_local/state/{*object}", get(read).put(write))
            .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
            .with_state(self)
    }

    fn signed_url(&self, method: &str, object: &str, expires: u64) -> Result<String> {
        validate_path(object)?;
        let signature = hmac::sign(&self.key, capability(method, object, expires).as_bytes());
        Ok(format!(
            "{}/_local/state/{object}?expires={expires}&signature={}",
            self.origin,
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
    }

    fn authorize(&self, method: &str, object: &str, access: &Access) -> Result<PathBuf> {
        validate_path(object)?;
        ensure!(
            access.expires > self.clock.now_ms()?,
            "local storage URL expired"
        );
        let signature = URL_SAFE_NO_PAD.decode(&access.signature)?;
        hmac::verify(
            &self.key,
            capability(method, object, access.expires).as_bytes(),
            &signature,
        )
        .map_err(|_| anyhow::anyhow!("invalid local storage capability"))?;
        Ok(self.root.join(object))
    }
}

#[async_trait]
impl StorageUrlSigner for LocalStorage {
    async fn read_url(&self, _region: &str, object: &str) -> Result<String> {
        self.signed_url("GET", object, self.clock.now_ms()?.saturating_add(60_000))
    }

    async fn write_ticket(
        &self,
        _region: &str,
        actor: &ActorKey,
        state_version: u64,
    ) -> Result<StateWriteTicket> {
        let object_name = snapshot_object_name(
            actor,
            state_version,
            &uuid::Uuid::new_v4().simple().to_string(),
        )?;
        let expires = self.clock.now_ms()?.saturating_add(60_000);
        Ok(StateWriteTicket {
            stream: None,
            replication: None,
            state_version,
            url: self.signed_url("PUT", &object_name, expires)?,
            object_name,
            expires_at_ms: i64::try_from(expires)?,
        })
    }

    fn regions(&self) -> Vec<String> {
        vec!["north-america-east".into()]
    }
}

#[derive(Deserialize)]
struct Access {
    expires: u64,
    signature: String,
}

async fn read(
    State(storage): State<Arc<LocalStorage>>,
    Path(object): Path<String>,
    Query(access): Query<Access>,
) -> Result<Bytes, StatusCode> {
    let path = storage
        .authorize("GET", &object, &access)
        .map_err(|_| StatusCode::FORBIDDEN)?;
    fs::read(path).await.map(Bytes::from).map_err(io_status)
}

async fn write(
    State(storage): State<Arc<LocalStorage>>,
    Path(object): Path<String>,
    Query(access): Query<Access>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let path = storage
        .authorize("PUT", &object, &access)
        .map_err(|_| StatusCode::FORBIDDEN)?;
    fs::create_dir_all(path.parent().ok_or(StatusCode::BAD_REQUEST)?)
        .await
        .map_err(io_status)?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
        .map_err(io_status)?;
    file.write_all(&body).await.map_err(io_status)?;
    file.sync_all().await.map_err(io_status)?;
    Ok(StatusCode::CREATED)
}

fn io_status(error: std::io::Error) -> StatusCode {
    match error.kind() {
        std::io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        std::io::ErrorKind::AlreadyExists => StatusCode::PRECONDITION_FAILED,
        _ => {
            tracing::error!(%error, "local state storage failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn capability(method: &str, object: &str, expires: u64) -> String {
    format!("{method}\n{object}\n{expires}")
}

fn validate_path(object: &str) -> Result<()> {
    ensure!(
        object.starts_with("snapshots/") && object.len() <= 1024,
        "invalid local snapshot path"
    );
    for part in object.split('/') {
        ensure!(
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
            "invalid local snapshot path component"
        );
    }
    PathBuf::from(object)
        .parent()
        .context("snapshot path has no parent")?;
    Ok(())
}
