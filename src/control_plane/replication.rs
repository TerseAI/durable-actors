use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use tokio::task::JoinSet;

use crate::{
    clock::SystemClock,
    postgres::PostgresDatabase,
    replication::{
        ReplicaAccess, ReplicaAssignment, ReplicaProvisioner, ReplicaScope, ReplicaTarget,
    },
    sandbox::{CommandSandboxProvider, ResourceLimits, SpareKind, pool::SparePool},
};

use super::{admin::AdminRegistry, process::SandboxProviderConfig};

mod lifecycle;
mod store;

pub(super) fn fleet(
    registry: Arc<dyn AdminRegistry>,
    config: &SandboxProviderConfig,
    signing_key: &str,
    replica_regions: Vec<String>,
    database: PostgresDatabase,
    stop: tokio_util::sync::CancellationToken,
) -> Result<(Arc<ActorReplicaFleet>, ReplicaAccess)> {
    let secret = URL_SAFE_NO_PAD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_bytes()),
            b"little-actors:replica:v1",
        )
        .as_ref(),
    );
    let mut pool_config = config.pool.clone();
    pool_config.kind = SpareKind::Replica;
    pool_config.regions = replica_regions.clone();
    pool_config.regions.sort();
    pool_config.regions.dedup();
    pool_config.resources = ResourceLimits::default();
    let pool = SparePool::new(
        database.clone(),
        Arc::new(CommandSandboxProvider::new(
            config.provider_name.clone(),
            config.command.clone(),
            config.environment.clone(),
        )?),
        pool_config,
    );
    pool.start(registry.clone(), stop);
    let fleet = Arc::new(ActorReplicaFleet {
        provider: Arc::new(PooledReplicaProvider {
            pool,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        }),
        registry,
        store: store::Store(database),
        secret: secret.clone(),
        replica_regions,
    });
    Ok((fleet, ReplicaAccess::new(&secret, Arc::new(SystemClock))))
}

pub(super) struct ActorReplicaFleet {
    provider: Arc<dyn ReplicaProvider>,
    registry: Arc<dyn AdminRegistry>,
    store: store::Store,
    secret: String,
    replica_regions: Vec<String>,
}

#[async_trait]
impl ReplicaProvisioner for ActorReplicaFleet {
    fn replica_regions(&self) -> Vec<String> {
        self.replica_regions.clone()
    }

    async fn ensure(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        self.repair(scope, &[]).await
    }

    async fn repair(&self, scope: &ReplicaScope, failed: &[String]) -> Result<Vec<ReplicaTarget>> {
        if self.replica_regions.is_empty() {
            return Ok(vec![]);
        }
        scope.actor.validate()?;
        let spec = self
            .registry
            .launch_spec()
            .await?
            .context("no actor image is registered")?;
        let group = self
            .store
            .prepare(scope, &self.replica_regions, &spec.image_ref, failed)
            .await?;
        self.provision(group).await
    }
}

impl ActorReplicaFleet {
    async fn provision(&self, group: store::Group) -> Result<Vec<ReplicaTarget>> {
        let mut pending = JoinSet::new();
        let mut targets = Vec::new();
        for (slot, placement) in group.slots.iter().enumerate() {
            let instance = placement
                .instances
                .last()
                .context("replica slot missing")?
                .clone();
            if let Some(target) = &instance.target {
                targets.push(target.clone());
                continue;
            }
            let assignment = ReplicaAssignment {
                scope: group.scope.clone(),
                host_id: instance.host.clone(),
                secret: self.secret.clone(),
            };
            let image = group.image.clone();
            let region = placement.region.clone();
            let provider = self.provider.clone();
            let store = self.store.clone();
            pending.spawn(async move {
                let target = match provider.ensure(&image, &region, &assignment).await {
                    Ok(target) => target,
                    Err(error) => {
                        store.retry_assignment(&assignment.scope.identity(), &instance.host).await?;
                        return Err(error);
                    }
                };
                if let Err(error) = store.record(&assignment.scope, slot, target.clone()).await {
                    if matches!(store.contains(&assignment.scope.identity()).await, Ok(false)) {
                        let _ = provider.retire(&instance.host).await;
                    }
                    return Err(error);
                }
                tracing::info!(event = "actor_replica_ready", actor = %assignment.scope.actor.storage_key(), activation = %assignment.scope.host, host_id = %target.host_id, region = %target.region);
                anyhow::Ok(target)
            });
        }
        let mut failure = None;
        while let Some(result) = pending.join_next().await {
            match result? {
                Ok(target) => targets.push(target),
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        targets.sort_by(|a, b| a.host_id.cmp(&b.host_id));
        Ok(targets)
    }
}

#[async_trait]
trait ReplicaProvider: Send + Sync {
    async fn ensure(
        &self,
        image: &str,
        region: &str,
        assignment: &ReplicaAssignment,
    ) -> Result<ReplicaTarget>;
    async fn retire(&self, host: &str) -> Result<()>;
}

struct PooledReplicaProvider {
    pool: Arc<SparePool>,
    client: reqwest::Client,
}

#[async_trait]
impl ReplicaProvider for PooledReplicaProvider {
    async fn ensure(
        &self,
        image: &str,
        region: &str,
        assignment: &ReplicaAssignment,
    ) -> Result<ReplicaTarget> {
        let spare = self
            .pool
            .acquire_replica(image, region, &assignment.host_id)
            .await?;
        let response = self
            .client
            .post(format!(
                "{}/assign",
                spare.control_route.trim_end_matches('/')
            ))
            .bearer_auth(&spare.control_token)
            .json(assignment)
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            response.status() == reqwest::StatusCode::NO_CONTENT,
            "replica spare did not confirm assignment"
        );
        self.pool.activate_replica(&assignment.host_id).await?;
        Ok(ReplicaTarget {
            host_id: assignment.host_id.clone(),
            url: spare.route,
            region: spare.canonical_region,
        })
    }

    async fn retire(&self, host: &str) -> Result<()> {
        self.pool.retire_replica(host).await
    }
}
