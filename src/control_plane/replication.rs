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
    postgres::PostgresDatabase,
    replication::{
        ReplicaAccess, ReplicaCatalog, ReplicaCoordinator, ReplicaManifest, ReplicaProvisioner,
        ReplicaTarget,
    },
    sandbox::{CommandSandboxProvider, EnsureReplicaRequest},
    state_transport::HttpStateTransport,
    storage_urls::StorageUrlSigner,
};

use super::{admin::AdminRegistry, process::SandboxProviderConfig};

pub(super) fn coordinator(
    bucket: Arc<dyn StorageUrlSigner>,
    database: PostgresDatabase,
    registry: Arc<dyn AdminRegistry>,
    config: &SandboxProviderConfig,
    signing_key: &str,
    count: usize,
    replica_regions: Vec<String>,
) -> Result<Arc<ReplicaCoordinator>> {
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
    Ok(Arc::new(ReplicaCoordinator::new(
        bucket,
        Arc::new(PostgresReplicaCatalog(database)),
        Arc::new(fleet),
        Arc::new(HttpStateTransport::new()),
        ReplicaAccess::new(&secret, Arc::new(SystemClock)),
        origin,
        count,
    )?))
}

struct PostgresReplicaCatalog(PostgresDatabase);

#[async_trait]
impl ReplicaCatalog for PostgresReplicaCatalog {
    async fn record(&self, object: &str, manifest: &ReplicaManifest) -> Result<()> {
        let document = serde_json::to_string(manifest)?;
        self.0.execute("INSERT INTO durable_object_snapshot_replicas (object_name, manifest) VALUES ($1, $2) ON CONFLICT (object_name) DO NOTHING", &[&object, &document]).await?;
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<ReplicaManifest>> {
        self.0
            .query_opt(
                "SELECT manifest FROM durable_object_snapshot_replicas WHERE object_name = $1",
                &[&object],
            )
            .await?
            .map(|row| serde_json::from_str(&row.get::<_, String>(0)).map_err(Into::into))
            .transpose()
    }
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
            .try_get_with((region.into(), count), self.provision(actor, region, count))
            .await
            .map_err(|error| anyhow::anyhow!("replica fleet unavailable: {error}"))
    }
}

impl ModalReplicaFleet {
    async fn provision(
        &self,
        actor: &ActorKey,
        region: &str,
        count: usize,
    ) -> Result<Vec<ReplicaTarget>> {
        let destinations =
            crate::replication::replica_destinations(region, count, &self.replica_regions)?;
        let spec = self
            .registry
            .launch_spec(&actor.namespace_id)
            .await?
            .context("no actor image is registered")?;
        let mut pending = JoinSet::new();
        for (slot, destination) in destinations.into_iter().enumerate() {
            let request = EnsureReplicaRequest {
                installation_id: format!("{}:{}", self.installation, env!("CARGO_PKG_VERSION")),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_replica_manifest_survives_reconnecting_to_postgres() -> Result<()> {
        let Ok(url) = std::env::var("DURABLE_OBJECT_TEST_POSTGRES_URL") else {
            return Ok(());
        };
        let database = PostgresDatabase::connect(&url).await?;
        let catalog = PostgresReplicaCatalog(database.clone());
        let object = format!("snapshots/{}", uuid::Uuid::new_v4());
        let manifest = ReplicaManifest {
            region: "us-east".into(),
            replicas: vec![ReplicaTarget {
                host_id: "old-host".into(),
                region: String::new(),
                url: "https://old-host.example".into(),
            }],
        };
        catalog.record(&object, &manifest).await?;
        let reopened = PostgresReplicaCatalog(PostgresDatabase::connect(&url).await?);
        let recovered = reopened
            .get(&object)
            .await?
            .context("replica manifest was lost")?;
        assert_eq!(recovered.region, manifest.region);
        assert_eq!(recovered.replicas, manifest.replicas);
        database
            .execute(
                "DELETE FROM durable_object_snapshot_replicas WHERE object_name = $1",
                &[&object],
            )
            .await?;
        Ok(())
    }
}
