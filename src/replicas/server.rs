use super::{
    access::Access,
    archive::Archive,
    client::{Command, ReplicaClient, Reply, Request, position},
    store::ReplicaDisk,
};
use crate::{
    bucket::{GcsBucket, PersistenceConfig, replace},
    clock::{Clock, SystemClock},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const BATCH_BYTES: usize = 16 * 1024 * 1024;
const BATCH_AGE_MS: u64 = 10_000;

pub async fn serve_replica(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let id = std::env::var("DURABLE_ACTORS_REPLICA_ID")?;
    let replicas = serde_json::from_str(&std::env::var("DURABLE_ACTORS_REPLICAS")?)?;
    let config = PersistenceConfig::Replicated {
        replicas,
        durability: serde_json::from_value(serde_json::Value::String(std::env::var(
            "DURABLE_ACTORS_DURABILITY",
        )?))?,
    };
    config.validate()?;
    let PersistenceConfig::Replicated { replicas, .. } = config else {
        unreachable!()
    };
    ensure!(
        replicas.iter().any(|r| r.id == id),
        "replica ID missing from configuration"
    );
    let access = Access::new(std::env::var("DURABLE_ACTORS_REPLICA_SECRET")?)?;
    let bucket = GcsBucket::new(&std::env::var("DURABLE_ACTORS_ARCHIVE_BUCKET")?).await?;
    bucket.require_standard().await?;
    let archive = Archive(Arc::new(bucket));
    let disk =
        ReplicaDisk::open(PathBuf::from(std::env::var("DURABLE_ACTORS_REPLICA_DATA")?)).await?;
    register_disk(&archive, &disk, &id).await?;
    let primary = replicas.first().is_some_and(|r| r.id == id);
    let peers = replicas
        .into_iter()
        .filter(|r| r.id != id)
        .map(|r| ReplicaClient::new(r, access.admin().into()))
        .collect::<Result<_>>()?;
    let server = Arc::new(ReplicaServer {
        id,
        disk,
        archive,
        access,
        peers,
        primary,
        clock: Arc::new(SystemClock),
    });
    let listener = tokio::net::TcpListener::bind(
        std::env::var("DURABLE_ACTORS_REPLICA_BIND").unwrap_or_else(|_| "0.0.0.0:7200".into()),
    )
    .await?;
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let uploader = server.clone();
    let upload_stop = stop.clone();
    let task = tokio::spawn(async move { uploader.upload_loop(upload_stop).await });
    tracing::info!(replica = %server.id, "storage replica ready");
    axum::serve(listener, routes(server))
        .with_graceful_shutdown(shutdown)
        .await?;
    stop.cancel();
    task.await?;
    Ok(())
}

struct ReplicaServer {
    id: String,
    disk: ReplicaDisk,
    archive: Archive,
    access: Access,
    peers: Vec<ReplicaClient>,
    primary: bool,
    clock: Arc<dyn Clock>,
}

fn routes(server: Arc<ReplicaServer>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/storage", post(storage))
        .layer(DefaultBodyLimit::disable())
        .with_state(server)
}
async fn health(State(server): State<Arc<ReplicaServer>>) -> StatusCode {
    match server.disk.identity().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
async fn storage(
    State(server): State<Arc<ReplicaServer>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Result<Json<std::result::Result<Reply, String>>, StatusCode> {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    server
        .access
        .authorize(token, request.command.scope())
        .map_err(|_| StatusCode::FORBIDDEN)?;
    if request.replica != server.id {
        return Err(StatusCode::CONFLICT);
    }
    Ok(Json(
        server
            .execute(request.command)
            .await
            .map_err(|e| format!("{e:#}")),
    ))
}

impl ReplicaServer {
    async fn execute(&self, command: Command) -> Result<Reply> {
        let mut reply = Reply::default();
        match command {
            Command::Prepare { prefix } => self.disk.prepare(&prefix).await?,
            Command::Seal { prefix } => self.disk.seal(&prefix).await?,
            Command::Append { prefix, record } => {
                self.disk
                    .append(&prefix, record, self.clock.now_ms()?)
                    .await?
            }
            Command::Latest { prefix } => {
                if let Some((object, bytes)) = self.disk.latest(&prefix).await? {
                    reply.object = Some(object);
                    reply.data = Some(STANDARD.encode(bytes));
                } else if let Some(object) = self.disk.latest_object(&prefix).await? {
                    let (_, version) = position(&object)?;
                    let bytes = self
                        .archive
                        .get(&prefix, version)
                        .await?
                        .context("archived replica head missing")?;
                    reply.object = Some(object);
                    reply.data = Some(STANDARD.encode(bytes));
                }
            }
            Command::Get { object } => {
                let (prefix, version) = position(&object)?;
                let bytes = match self.disk.get(&prefix, version).await? {
                    Some(bytes) => Some(bytes),
                    None => self.archive.get(&prefix, version).await?,
                };
                reply.data = bytes.map(|bytes| STANDARD.encode(bytes));
            }
            Command::List { prefix } => {
                let (local, archived) =
                    tokio::try_join!(self.disk.list(&prefix), self.archive.list(&prefix))?;
                reply.keys = local
                    .into_iter()
                    .chain(archived)
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect();
            }
            Command::ExportSealed => {
                reply.data = Some(STANDARD.encode(self.disk.export_sealed().await?))
            }
            Command::Archived { key } => {
                let batch = self.archive.read(&key).await?;
                self.disk.archived(&key, &batch).await?;
            }
        }
        Ok(reply)
    }

    async fn upload_loop(&self, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = stop.cancelled() => return, _ = interval.tick() => {} }
            tokio::select! {
                _ = stop.cancelled() => return,
                result = self.upload_due() => if let Err(error) = result { tracing::warn!(%error, "archive upload deferred; replica log retained"); }
            }
        }
    }

    async fn upload_due(&self) -> Result<()> {
        let age = if self.primary {
            BATCH_AGE_MS
        } else {
            BATCH_AGE_MS * 3
        };
        let size = if self.primary {
            BATCH_BYTES
        } else {
            usize::MAX / 2
        };
        let prefixes = self.disk.due(self.clock.now_ms()?, age, size).await?;
        let results = futures_util::future::join_all(
            prefixes.iter().map(|prefix| self.upload_prefix(prefix)),
        )
        .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn upload_prefix(&self, prefix: &str) -> Result<()> {
        let Some(batch) = self.disk.batch(prefix, BATCH_BYTES).await? else {
            return Ok(());
        };
        let key = self.archive.write(&batch).await?;
        self.disk.archived(&key, &batch).await?;
        for result in
            futures_util::future::join_all(self.peers.iter().map(|peer| peer.archived(&key))).await
        {
            if let Err(error) = result {
                tracing::warn!(%error, "replica archive notification deferred");
            }
        }
        Ok(())
    }
}

async fn register_disk(archive: &Archive, disk: &ReplicaDisk, id: &str) -> Result<()> {
    let key = format!("{}replica-disks/{id}.json", crate::storage_paths::ROOT);
    let identity = serde_json::to_vec(&disk.identity().await?)?;
    ensure!(
        replace(archive.0.as_ref(), &key, None, identity).await?,
        "replica disk identity changed; restore the replacement disk before starting it"
    );
    Ok(())
}

pub async fn restore_replica() -> Result<()> {
    let id = std::env::var("DURABLE_ACTORS_REPLICA_ID")?;
    let source_id = std::env::var("DURABLE_ACTORS_RESTORE_SOURCE_ID")?;
    ensure!(
        id != source_id,
        "replacement source must be another replica"
    );
    let replicas: Vec<crate::bucket::ReplicaPlacement> =
        serde_json::from_str(&std::env::var("DURABLE_ACTORS_REPLICAS")?)?;
    let source = replicas
        .into_iter()
        .find(|r| r.id == source_id)
        .context("restore source is not a configured replica")?;
    let access = Access::new(std::env::var("DURABLE_ACTORS_REPLICA_SECRET")?)?;
    let archive = Archive(Arc::new(
        GcsBucket::new(&std::env::var("DURABLE_ACTORS_ARCHIVE_BUCKET")?).await?,
    ));
    let marker = archive
        .0
        .get(&format!(
            "{}replica-disks/{id}.json",
            crate::storage_paths::ROOT
        ))
        .await?
        .context("replica identity registration is missing")?;
    let identity: String = serde_json::from_slice(&marker.bytes)?;
    let disk =
        ReplicaDisk::open(PathBuf::from(std::env::var("DURABLE_ACTORS_REPLICA_DATA")?)).await?;
    let client = ReplicaClient::new(source, access.admin().into())?;
    let backup = client
        .send(Command::ExportSealed)
        .await?
        .data
        .context("replica backup missing")?;
    disk.restore(STANDARD.decode(backup)?, identity).await?;
    tracing::info!(replica = %id, "replacement replica restored; old actor epochs remain sealed");
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/replicas/server.rs"]
mod tests;
