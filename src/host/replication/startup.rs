use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{bucket::ReplicaMembership, replication::ReplicaScope};

#[async_trait]
pub(crate) trait InitialReplicaSource: Send + Sync {
    async fn prepare(&self, scope: &ReplicaScope) -> Result<ReplicaMembership>;
}

#[async_trait]
impl InitialReplicaSource for crate::control_plane::ControlPlaneClient {
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
    ) -> Self {
        let (sender, receiver) = watch::channel(None);
        if enabled {
            tokio::spawn(async move {
                tokio::select! {
                    _ = stop.cancelled() => {},
                    _ = Self::prepare(source, scope, sender) => {},
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
}

#[cfg(test)]
#[path = "../../../tests/support/initial_replication.rs"]
mod testing;
