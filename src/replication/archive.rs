use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::state_transport::{HttpStateTransport, StateTransport, StateWrite};

use super::{ReplicaStore, store::PendingSnapshot};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveTicket {
    pub write_url: String,
    pub read_url: String,
}

pub fn start_archiver(store: Arc<dyn ReplicaStore>) -> CancellationToken {
    let stop = CancellationToken::new();
    let cancelled = stop.clone();
    tokio::spawn(async move {
        let http = archive_client();
        let transport = HttpStateTransport::new();
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => break,
                _ = interval.tick() => {
                    let result = tokio::select! {
                        _ = cancelled.cancelled() => break,
                        result = archive_batch(store.as_ref(), &http, &transport) => result,
                    };
                    if let Err(error) = result {
                        tracing::warn!(event = "replica_archive_failed", error = %error);
                    }
                }
            }
        }
    });
    stop
}

pub async fn archive_pending(store: &dyn ReplicaStore) -> Result<()> {
    archive_batch(store, &archive_client(), &HttpStateTransport::new()).await
}

fn archive_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .expect("valid archive client configuration")
}

async fn archive_batch(
    store: &dyn ReplicaStore,
    http: &reqwest::Client,
    transport: &dyn StateTransport,
) -> Result<()> {
    for snapshot in store.pending(4).await? {
        match archive_snapshot(http, transport, &snapshot).await {
            Ok(()) => {
                store.archived(&snapshot.object).await?;
                tracing::info!(event = "replica_archived", object = %snapshot.object,
                    archive_lag_ms = now_ms().saturating_sub(snapshot.created_at_ms), bytes = snapshot.bytes.len());
            }
            Err(_) => {
                store.attempted(&snapshot.object).await?;
                tracing::warn!(event = "replica_archive_retry", object = %snapshot.object,
                    archive_lag_ms = now_ms().saturating_sub(snapshot.created_at_ms));
            }
        }
    }
    Ok(())
}

async fn archive_snapshot(
    http: &reqwest::Client,
    transport: &dyn StateTransport,
    snapshot: &PendingSnapshot,
) -> Result<()> {
    let ticket: ArchiveTicket = http
        .get(&snapshot.archive_url)
        .query(&[("object", &snapshot.object)])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if transport
        .write(&ticket.write_url, snapshot.bytes.clone())
        .await?
        == StateWrite::AlreadyExists
    {
        let existing = transport.read(&ticket.read_url).await?;
        ensure!(
            existing.as_ref() == snapshot.bytes,
            "archived snapshot conflicts with replica"
        );
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| i64::try_from(time.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
