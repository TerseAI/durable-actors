use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_lc_rs::{digest, hmac};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use moka::future::Cache;
use tokio::task::JoinSet;

use crate::{
    actor::ActorKey,
    clock::SystemClock,
    replication::{ReplicaAccess, ReplicaProvisioner, ReplicaTarget},
    sandbox::{CommandSandboxProvider, EnsureReplicaRequest},
};

use super::{admin::AdminRegistry, process::SandboxProviderConfig};

pub(super) fn fleet(
    registry: Arc<dyn AdminRegistry>,
    config: &SandboxProviderConfig,
    signing_key: &str,
    replica_regions: Vec<String>,
) -> Result<(Arc<dyn ReplicaProvisioner>, ReplicaAccess)> {
    let origin = config.runtime.control_plane_url.clone();
    let secret = URL_SAFE_NO_PAD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_bytes()),
            b"little-actors:replica:v1",
        )
        .as_ref(),
    );
    let installation =
        URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, secret.as_bytes()).as_ref());
    let fleet = ModalReplicaFleet {
        provider: Arc::new(CommandSandboxProvider::new(
            config.provider_name.clone(),
            config.command.clone(),
            config.environment.clone(),
        )?),
        registry,
        secret: secret.clone(),
        origin: origin.clone(),
        installation,
        replica_regions,
        cache: Cache::builder()
            .max_capacity(64)
            .time_to_live(Duration::from_secs(30))
            .build(),
    };
    Ok((
        Arc::new(fleet),
        ReplicaAccess::new(&secret, Arc::new(SystemClock)),
    ))
}

struct ModalReplicaFleet {
    provider: Arc<CommandSandboxProvider>,
    registry: Arc<dyn AdminRegistry>,
    secret: String,
    origin: String,
    installation: String,
    replica_regions: Vec<String>,
    cache: Cache<(String, usize), Vec<ReplicaTarget>>,
}

#[async_trait]
impl ReplicaProvisioner for ModalReplicaFleet {
    fn replica_regions(&self) -> Vec<String> {
        self.replica_regions.clone()
    }

    async fn ensure(
        &self,
        actor: &ActorKey,
        region: &str,
        count: usize,
    ) -> Result<Vec<ReplicaTarget>> {
        self.cache
            .try_get_with((region.into(), count), self.provision(actor, count))
            .await
            .map_err(|error| anyhow::anyhow!("replica fleet unavailable: {error}"))
    }
}

impl ModalReplicaFleet {
    async fn provision(&self, actor: &ActorKey, count: usize) -> Result<Vec<ReplicaTarget>> {
        anyhow::ensure!(
            count == self.replica_regions.len(),
            "replica placement mismatch"
        );
        if count == 0 {
            return Ok(Vec::new());
        }
        let destinations = self.replica_regions.clone();
        let spec = self
            .registry
            .launch_spec(&actor.namespace_id)
            .await?
            .context("no actor image is registered")?;
        let mut pending = JoinSet::new();
        for (slot, destination) in destinations.into_iter().enumerate() {
            let request = EnsureReplicaRequest {
                installation_id: format!(
                    "{}:{}:epochs-v2",
                    self.installation,
                    env!("CARGO_PKG_VERSION")
                ),
                slot,
                canonical_region: destination.clone(),
                image_ref: spec.image_ref.clone(),
                host_id: format!("replica.{}", uuid::Uuid::new_v4()),
                secret: self.secret.clone(),
                control_plane_url: self.origin.clone(),
            };
            let provider = self.provider.clone();
            pending.spawn(async move {
                let handle = provider.ensure_replica(&request).await?;
                tracing::info!(event = "replica_provisioned", region = %destination, host_id = %handle.host_id, resource_id = handle.provisioning.as_ref().map(|p| p.resource_id.as_str()).unwrap_or(""));
                anyhow::Ok(ReplicaTarget {
                    host_id: handle.host_id.to_string(),
                    url: handle.route,
                    region: destination,
                })
            });
        }
        let mut peers = Vec::new();
        while let Some(result) = pending.join_next().await {
            peers.push(result??);
        }
        peers.sort_by(|a, b| a.host_id.cmp(&b.host_id));
        Ok(peers)
    }
}
