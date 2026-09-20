use super::{DEFAULT_REPLICA_BYTES, FileReplicaStore};
use anyhow::{Context, Result, ensure};
use std::{
    env,
    future::{Future, IntoFuture},
    path::PathBuf,
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub async fn serve_replica_host(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let token = env::var("DURABLE_OBJECT_SPARE_TOKEN").context("replica spare token missing")?;
    ensure!(token.len() >= 32, "replica spare token is too short");
    let path = env::var("DURABLE_OBJECT_REPLICA_DATA")
        .unwrap_or_else(|_| "/tmp/durable-object-replica".into());
    let store = Arc::new(FileReplicaStore::open(PathBuf::from(path), DEFAULT_REPLICA_BYTES).await?);
    let listener = TcpListener::bind(
        env::var("DURABLE_OBJECT_HOST_BIND").unwrap_or_else(|_| "0.0.0.0:7101".into()),
    )
    .await?;
    let control = TcpListener::bind(
        env::var("DURABLE_OBJECT_SPARE_BIND").unwrap_or_else(|_| "0.0.0.0:7102".into()),
    )
    .await?;
    let (storage_routes, assignment_routes) = super::spare::routers(store, token);
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let storage = axum::serve(listener, storage_routes)
        .with_graceful_shutdown(stop.clone().cancelled_owned())
        .into_future();
    let assignment = axum::serve(control, assignment_routes)
        .with_graceful_shutdown(stop.cancelled_owned())
        .into_future();
    let ready = env::var("DURABLE_OBJECT_SPARE_READY_FILE")
        .unwrap_or_else(|_| "/tmp/durable-object-spare-ready".into());
    tokio::fs::write(ready, b"ready\n").await?;
    tracing::info!(
        event = "replica_spare_ready",
        "generic replica listener ready"
    );
    tokio::select! {
        result = storage => result.context("serve replica storage"),
        result = assignment => result.context("serve replica assignment"),
        () = shutdown => Ok(()),
    }
}
