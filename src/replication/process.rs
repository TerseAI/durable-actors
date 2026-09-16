use std::{env, future::Future, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{fs, net::TcpListener};

use crate::clock::SystemClock;

use super::{DEFAULT_SPOOL_BYTES, ReplicaAccess, ReplicaStore, replica_router, start_archiver};

pub async fn serve_replica_host(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let host_id = env::var("DURABLE_OBJECT_HOST_ID").context("replica host ID is required")?;
    let secret = env::var("DURABLE_OBJECT_REPLICA_SECRET").context("replica secret is required")?;
    let path = env::var("DURABLE_OBJECT_REPLICA_DATA")
        .unwrap_or_else(|_| "/tmp/durable-object-replica/state.db".into());
    let store = Arc::new(ReplicaStore::open(PathBuf::from(path), DEFAULT_SPOOL_BYTES).await?);
    let listener = TcpListener::bind(
        env::var("DURABLE_OBJECT_HOST_BIND").unwrap_or_else(|_| "127.0.0.1:7101".into()),
    )
    .await?;
    let route = advertised_route(&listener).await?;
    let archive = start_archiver(store.clone());
    let router = replica_router(
        store,
        ReplicaAccess::new(&secret, Arc::new(SystemClock)),
        host_id.clone(),
    );
    publish_ready(&host_id, &route).await?;
    tracing::info!(event = "replica_ready", %host_id, %route);
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await;
    archive.cancel();
    result.context("serve replica storage")
}

async fn advertised_route(listener: &TcpListener) -> Result<String> {
    let Ok(path) = env::var("DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE") else {
        return Ok(format!("http://{}", listener.local_addr()?));
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(route) = fs::read_to_string(&path).await
                && !route.trim().is_empty()
            {
                return route.trim().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("wait for replica public route")
}

async fn publish_ready(host_id: &str, route: &str) -> Result<()> {
    if let Ok(path) = env::var("DURABLE_OBJECT_HOST_METADATA_FILE") {
        let temporary = format!("{path}.tmp");
        fs::write(&temporary, serde_json::to_vec(&serde_json::json!({
            "hostId": host_id, "route": route, "canonicalRegion": env::var("DURABLE_OBJECT_REGION")?,
        }))?).await?;
        fs::rename(temporary, path).await?;
    }
    if let Ok(path) = env::var("DURABLE_OBJECT_HOST_READY_FILE") {
        fs::write(path, b"ready").await?;
    }
    Ok(())
}
