use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    bucket::ReplicaMembership,
    replication::{ReplicaScope, ReplicaTarget},
    state_transport::GrpcStateTransport,
};

#[async_trait]
pub(crate) trait InitialReplicaSource: Send + Sync {
    async fn targets(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>>;
    async fn prepare(&self, scope: &ReplicaScope) -> Result<ReplicaMembership>;
}

#[async_trait]
impl InitialReplicaSource for crate::control_plane::ControlPlaneClient {
    async fn targets(&self, _: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        self.prepare_replica_connections().await
    }

    async fn prepare(&self, _: &ReplicaScope) -> Result<ReplicaMembership> {
        self.prepare_initial_replicas().await
    }
}

pub(crate) struct InitialReplication(watch::Receiver<Option<ReplicaMembership>>);

impl InitialReplication {
    pub fn start(
        source: Arc<dyn InitialReplicaSource>,
        scope: ReplicaScope,
        enabled: bool,
        stop: CancellationToken,
        transport: GrpcStateTransport,
    ) -> Self {
        let (sender, receiver) = watch::channel(None);
        if enabled {
            tokio::spawn(async move {
                tokio::select! {
                    _ = stop.cancelled() => {},
                    _ = async {
                        tokio::join!(
                            Self::prepare(source.clone(), scope.clone(), sender),
                            Self::preconnect(source, scope, transport),
                        );
                    } => {},
                }
            });
        }
        Self(receiver)
    }

    pub async fn ready(mut self) -> Result<ReplicaMembership> {
        loop {
            if let Some(membership) = self.0.borrow_and_update().clone() {
                return Ok(membership);
            }
            self.0
                .changed()
                .await
                .context("initial replica preparation stopped")?;
        }
    }

    async fn prepare(
        source: Arc<dyn InitialReplicaSource>,
        scope: ReplicaScope,
        sender: watch::Sender<Option<ReplicaMembership>>,
    ) {
        loop {
            match source.prepare(&scope).await {
                Ok(membership) => {
                    sender.send_replace(Some(membership));
                    return;
                }
                Err(error) => {
                    tracing::warn!(event = "initial_replication_degraded", %error, actor = %scope.actor.storage_key())
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    async fn preconnect(
        source: Arc<dyn InitialReplicaSource>,
        scope: ReplicaScope,
        transport: GrpcStateTransport,
    ) {
        let replicas = match source.targets(&scope).await {
            Ok(replicas) => replicas,
            Err(error) => {
                tracing::warn!(event = "replica_preconnect_failed", %error, actor = %scope.actor.storage_key());
                return;
            }
        };
        futures_util::future::join_all(replicas.iter().map(|replica| async {
            let started = Instant::now();
            let result = transport.preconnect(&replica.url).await;
            tracing::info!(
                event = "replica_preconnect",
                host_id = %replica.host_id,
                region = %replica.region,
                connected = result.is_ok(),
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                error = result.err().map(|error| error.to_string()),
            );
        }))
        .await;
    }
}

#[cfg(test)]
#[path = "../../../tests/support/initial_replication.rs"]
mod testing;

#[cfg(test)]
#[path = "../../../tests/unit/host/initial_replication.rs"]
mod tests;
