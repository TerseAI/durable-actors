use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{
    actor::ActorKey,
    state_transport::StateTransport,
    storage_urls::{StateWriteTicket, StorageUrlSigner},
};

use super::{
    ArchiveTicket, MAX_REPLICAS, ReplicaAccess, ReplicaGrant, ReplicaTarget, ReplicationTicket,
    access::AccessQuery,
};

#[async_trait]
pub trait ReplicaCatalog: Send + Sync {
    async fn record(&self, object: &str, manifest: &ReplicaManifest) -> Result<()>;
    async fn get(&self, object: &str) -> Result<Option<ReplicaManifest>>;
}

#[async_trait]
pub trait ReplicaProvisioner: Send + Sync {
    fn replica_regions(&self) -> Vec<String> {
        Vec::new()
    }

    async fn ensure(
        &self,
        actor: &ActorKey,
        region: &str,
        count: usize,
    ) -> Result<Vec<ReplicaTarget>>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReplicaManifest {
    pub region: String,
    pub replicas: Vec<ReplicaTarget>,
}

pub struct ReplicaCoordinator {
    bucket: Arc<dyn StorageUrlSigner>,
    catalog: Arc<dyn ReplicaCatalog>,
    fleet: Arc<dyn ReplicaProvisioner>,
    transport: Arc<dyn StateTransport>,
    access: ReplicaAccess,
    origin: String,
    count: usize,
}

impl ReplicaCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: Arc<dyn StorageUrlSigner>,
        catalog: Arc<dyn ReplicaCatalog>,
        fleet: Arc<dyn ReplicaProvisioner>,
        transport: Arc<dyn StateTransport>,
        access: ReplicaAccess,
        origin: String,
        count: usize,
    ) -> Result<Self> {
        ensure!(
            count <= MAX_REPLICAS,
            "replica count exceeds {MAX_REPLICAS}"
        );
        Ok(Self {
            bucket,
            catalog,
            fleet,
            transport,
            access,
            origin,
            count,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/_replica/read", get(read))
            .route("/_replica/archive", get(archive))
            .with_state(self)
    }

    pub async fn recover(&self, region: &str, object: &str) -> Result<Bytes> {
        let mut reads = JoinSet::new();
        let bucket = self.bucket.clone();
        let transport = self.transport.clone();
        let region_name = region.to_owned();
        let object_name = object.to_owned();
        reads.spawn(async move {
            transport
                .read(&bucket.read_url(&region_name, &object_name).await?)
                .await
        });
        if let Some(manifest) = self.catalog.get(object).await? {
            ensure!(
                manifest.region == region,
                "replica manifest belongs to another region"
            );
            for replica in manifest.replicas {
                let url = self.access.url(
                    &replica.url,
                    "state",
                    &ReplicaGrant {
                        host_id: replica.host_id,
                        ..grant("GET", region, object, 60_000)?
                    },
                )?;
                let transport = self.transport.clone();
                reads.spawn(async move { transport.read(&url).await });
            }
        }
        while let Some(result) = reads.join_next().await {
            if let Ok(Ok(bytes)) = result {
                return Ok(bytes);
            }
        }
        anyhow::bail!(
            "committed snapshot is unavailable from object storage and its recorded replicas"
        )
    }
}

#[async_trait]
impl StorageUrlSigner for ReplicaCoordinator {
    fn durability(&self) -> super::DurabilityPolicy {
        let mut policy = super::DurabilityPolicy::new(self.count);
        policy.replica_regions = self.fleet.replica_regions();
        if !policy.replica_regions.is_empty() {
            policy.mode = "cross_region_preview".into();
        }
        policy
    }
    async fn read_url(&self, region: &str, object: &str) -> Result<String> {
        if self.catalog.get(object).await?.is_none() {
            return self.bucket.read_url(region, object).await;
        }
        self.access.url(
            &self.origin,
            "read",
            &grant("RECOVER", region, object, 60_000)?,
        )
    }

    async fn write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        version: u64,
    ) -> Result<StateWriteTicket> {
        if self.count == 0 {
            return self.bucket.write_ticket(region, actor, version).await;
        }
        let Some(replicas) = self.ready_replicas(actor, region).await else {
            return self.bucket.write_ticket(region, actor, version).await;
        };
        let mut ticket = self.bucket.write_ticket(region, actor, version).await?;
        let archive_url = self.access.url(
            &self.origin,
            "archive",
            &grant("ARCHIVE", region, &ticket.object_name, 3 * 86_400_000)?,
        )?;
        let targets = replicas
            .iter()
            .map(|replica| {
                Ok(ReplicaTarget {
                    host_id: replica.host_id.clone(),
                    region: replica.region.clone(),
                    url: self.access.url(
                        &replica.url,
                        "state",
                        &ReplicaGrant {
                            host_id: replica.host_id.clone(),
                            archive_url: archive_url.clone(),
                            ..grant("PUT", region, &ticket.object_name, 60_000)?
                        },
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let replication = ReplicationTicket {
            required_replicas: self.count,
            replicas: targets,
            archive_url,
        };
        replication.validate()?;
        self.catalog
            .record(
                &ticket.object_name,
                &ReplicaManifest {
                    region: region.into(),
                    replicas,
                },
            )
            .await?;
        ticket.replication = Some(replication);
        Ok(ticket)
    }

    fn regions(&self) -> Vec<String> {
        self.bucket.regions()
    }
}

impl ReplicaCoordinator {
    async fn ready_replicas(&self, actor: &ActorKey, region: &str) -> Option<Vec<ReplicaTarget>> {
        let fleet = self.fleet.clone();
        let actor = actor.clone();
        let region_name = region.to_owned();
        let count = self.count;
        let provisioning =
            tokio::spawn(async move { fleet.ensure(&actor, &region_name, count).await });
        match tokio::time::timeout(Duration::from_secs(1), provisioning).await {
            Ok(Ok(Ok(replicas))) => Some(replicas),
            _ => {
                tracing::warn!(
                    event = "replica_provisioning_fallback",
                    region,
                    "using object storage while replica provisioning is unavailable or pending"
                );
                None
            }
        }
    }
}

async fn read(
    State(coordinator): State<Arc<ReplicaCoordinator>>,
    Query(query): Query<AccessQuery>,
) -> Result<Bytes, StatusCode> {
    let grant = coordinator
        .access
        .verify(&query.token, "RECOVER")
        .map_err(|_| StatusCode::FORBIDDEN)?;
    tokio::time::timeout(
        Duration::from_secs(25),
        coordinator.recover(&grant.region, &grant.object),
    )
    .await
    .map_err(|_| StatusCode::GATEWAY_TIMEOUT)?
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn archive(
    State(coordinator): State<Arc<ReplicaCoordinator>>,
    Query(query): Query<AccessQuery>,
) -> Result<Json<ArchiveTicket>, StatusCode> {
    let grant = coordinator
        .access
        .verify(&query.token, "ARCHIVE")
        .map_err(|_| StatusCode::FORBIDDEN)?;
    let (write_url, read_url) = tokio::try_join!(
        coordinator
            .bucket
            .archive_write_url(&grant.region, &grant.object),
        coordinator.bucket.read_url(&grant.region, &grant.object),
    )
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(ArchiveTicket {
        write_url,
        read_url,
    }))
}

fn grant(operation: &str, region: &str, object: &str, lifetime_ms: u64) -> Result<ReplicaGrant> {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(ReplicaGrant {
        operation: operation.into(),
        region: region.into(),
        object: object.into(),
        host_id: String::new(),
        archive_url: String::new(),
        expires_at_ms: now.saturating_add(lifetime_ms),
    })
}
