use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{CreateSpareRequest, ResourceLimits, SandboxProvider, SpareHandle, SpareKind};
use crate::{
    control_plane::admin::{AdminRegistry, HostLaunchSpec},
    postgres::PostgresDatabase,
};

mod replica;

#[derive(Clone)]
pub(crate) struct PoolConfig {
    pub kind: SpareKind,
    pub idle: u32,
    pub idle_ttl_seconds: u32,
    pub regions: Vec<String>,
    pub resources: ResourceLimits,
}

pub(crate) struct SparePool {
    store: PoolStore,
    provider: Arc<dyn SandboxProvider>,
    pub config: PoolConfig,
    wake: tokio::sync::Notify,
}

impl SparePool {
    pub fn new(
        database: PostgresDatabase,
        provider: Arc<dyn SandboxProvider>,
        config: PoolConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            store: PoolStore(database, config.kind),
            provider,
            config,
            wake: tokio::sync::Notify::new(),
        })
    }

    pub async fn claim(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        host: &str,
    ) -> Result<Option<SpareHandle>> {
        if spec.code_snapshot.is_none() || !spec.secret_refs.is_empty() {
            return Ok(None);
        }
        let result = self
            .store
            .claim(
                &self.key(&spec.image_ref, region),
                host,
                &spec.host_config_key(),
            )
            .await?;
        self.wake.notify_one();
        Ok(result)
    }

    pub async fn reserve_host(&self, session: &str, host: &str, config_key: &str) -> Result<()> {
        let name = format!("do-actor-{session}");
        self.store.0.execute("INSERT INTO durable_object_spares (name, pool_key, status, host_id, host_config_key, expires_at) VALUES ($1, '', 'claimed', $2, $3, clock_timestamp() + interval '120 seconds')", &[&name, &host, &config_key]).await?;
        Ok(())
    }

    pub async fn wait_ready(&self, host: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let row = self
                    .store
                    .0
                    .query_opt(
                        "SELECT status FROM durable_object_spares WHERE host_id = $1",
                        &[&host],
                    )
                    .await?;
                match row.as_ref().map(|row| row.get::<_, &str>(0)) {
                    Some("active") => return Ok(()),
                    Some("claimed") => tokio::time::sleep(Duration::from_millis(20)).await,
                    _ => anyhow::bail!("actor sandbox did not become ready"),
                }
            }
        })
        .await
        .context("actor sandbox readiness timed out")?
    }

    pub async fn remember(&self, host: &str, config_key: &str, spare: &SpareHandle) -> Result<()> {
        let updated = self.store.0.execute(
            "UPDATE durable_object_spares SET status = 'active', handle = $2, expires_at = created_at + interval '24 hours' \
             WHERE name = $1 AND host_id = $3 AND host_config_key = $4 AND status = 'claimed' AND expires_at > clock_timestamp()",
            &[&spare.name, &serde_json::to_string(spare)?, &host, &config_key],
        ).await?;
        ensure!(updated == 1, "actor sandbox claim expired or was retired");
        Ok(())
    }

    pub async fn host(&self, host: &str) -> Result<Option<SpareHandle>> {
        self.store
            .0
            .query_opt(
                "SELECT handle FROM durable_object_spares WHERE host_id = $1 AND status = 'active'",
                &[&host],
            )
            .await?
            .map(|row| serde_json::from_str(row.get::<_, &str>(0)).map_err(Into::into))
            .transpose()
    }

    pub async fn failed(&self, host: &str) -> Result<()> {
        self.store
            .0
            .execute(
                "UPDATE durable_object_spares SET status = 'retiring' WHERE host_id = $1",
                &[&host],
            )
            .await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn retire_config(&self, config_key: &str) -> Result<Vec<String>> {
        let client = self.store.0.connection().await?;
        let rows = client.query("UPDATE durable_object_spares SET status = 'retiring' WHERE host_config_key = $1 RETURNING handle", &[&config_key]).await?;
        let mut ids = Vec::new();
        for row in rows {
            if let Some(handle) = row.get::<_, Option<String>>(0) {
                ids.push(serde_json::from_str::<SpareHandle>(&handle)?.resource_id);
            }
        }
        self.cleanup().await?;
        Ok(ids)
    }

    pub fn start(self: &Arc<Self>, registry: Arc<dyn AdminRegistry>, stop: CancellationToken) {
        let pool = self.clone();
        tokio::spawn(async move {
            loop {
                let result = tokio::select! {
                    biased;
                    () = stop.cancelled() => break,
                    result = pool.reconcile(registry.as_ref()) => result,
                };
                if let Err(error) = result {
                    warn!(error = %format!("{error:#}"), "generic spare reconciliation failed");
                }
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = pool.wake.notified() => {},
                    () = tokio::time::sleep(Duration::from_secs(5)) => {},
                }
            }
        });
    }

    async fn reconcile(&self, registry: &dyn AdminRegistry) -> Result<()> {
        let deployments = registry.launch_specs().await?;
        let images: std::collections::BTreeSet<_> = deployments
            .iter()
            .filter(|spec| {
                self.config.kind == SpareKind::Replica
                    || (spec.code_snapshot.is_some() && spec.secret_refs.is_empty())
            })
            .map(|spec| spec.image_ref.as_str())
            .collect();
        let keys: Vec<String> = images
            .iter()
            .flat_map(|image| {
                self.config
                    .regions
                    .iter()
                    .map(|region| self.key(image, region))
            })
            .collect();
        self.store.retire_unwanted(&keys, self.config.idle).await?;
        self.cleanup().await?;
        for image in images {
            for region in &self.config.regions {
                let key = self.key(image, region);
                let mut builds = Vec::new();
                while let Some(name) = self.store.reserve(&key, self.config.idle).await? {
                    builds.push(self.build(&key, image, region, name));
                }
                for result in futures_util::future::join_all(builds).await {
                    result?;
                }
            }
        }
        Ok(())
    }

    async fn build(&self, key: &str, image: &str, region: &str, name: String) -> Result<()> {
        let result = self.create(image, region, &name).await;
        match result {
            Ok(handle) => {
                if !self
                    .store
                    .publish(key, &handle, self.config.idle_ttl_seconds)
                    .await?
                {
                    self.provider.retire_spare(&handle).await?;
                }
                Ok(())
            }
            Err(error) => {
                self.store
                    .0
                    .execute(
                        "UPDATE durable_object_spares SET status = 'retiring' WHERE name = $1",
                        &[&name],
                    )
                    .await?;
                Err(error)
            }
        }
    }

    async fn create(&self, image: &str, region: &str, name: &str) -> Result<SpareHandle> {
        let handle = self
            .provider
            .create_spare(&CreateSpareRequest {
                name: name.into(),
                image_ref: image.into(),
                canonical_region: region.into(),
                resources: self.config.resources.clone(),
                kind: self.config.kind,
            })
            .await?;
        ensure!(
            handle.name == name
                && handle.canonical_region == region
                && !handle.resource_id.is_empty()
                && !handle.route.is_empty()
                && !handle.control_route.is_empty()
                && !handle.control_token.is_empty(),
            "provider returned an invalid spare"
        );
        Ok(handle)
    }

    async fn cleanup(&self) -> Result<()> {
        for handle in self.store.retiring().await? {
            self.provider.retire_spare(&handle).await?;
            self.store
                .0
                .execute(
                    "DELETE FROM durable_object_spares WHERE name = $1 AND status = 'retiring'",
                    &[&handle.name],
                )
                .await?;
        }
        Ok(())
    }

    fn key(&self, image: &str, region: &str) -> String {
        serde_json::to_string(&(self.config.kind, image, region, &self.config.resources))
            .expect("serializable pool key")
    }
}

struct PoolStore(PostgresDatabase, SpareKind);

impl PoolStore {
    async fn reserve(&self, key: &str, target: u32) -> Result<Option<String>> {
        if target == 0 {
            return Ok(None);
        }
        let mut client = self.0.connection().await?;
        let transaction = client.transaction().await?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&key],
            )
            .await?;
        let count: i64 = transaction.query_one(
            "SELECT count(*) FROM durable_object_spares WHERE pool_key = $1 AND status IN ('ready', 'starting') AND expires_at > clock_timestamp()", &[&key],
        ).await?.get(0);
        if count >= i64::from(target) {
            return Ok(None);
        }
        let name = format!("do-spare-{}", uuid::Uuid::new_v4().simple());
        transaction.execute("INSERT INTO durable_object_spares (name, pool_key, status, expires_at, kind) VALUES ($1, $2, 'starting', clock_timestamp() + interval '120 seconds', $3)", &[&name, &key, &self.1.as_str()]).await?;
        transaction.commit().await?;
        Ok(Some(name))
    }

    async fn publish(&self, key: &str, handle: &SpareHandle, ttl: u32) -> Result<bool> {
        Ok(self.0.execute(
            "UPDATE durable_object_spares SET status = 'ready', handle = $3, expires_at = clock_timestamp() + make_interval(secs => $4) \
             WHERE name = $1 AND pool_key = $2 AND status = 'starting' AND expires_at > clock_timestamp()",
            &[&handle.name, &key, &serde_json::to_string(handle)?, &f64::from(ttl)],
        ).await? == 1)
    }

    async fn claim(&self, key: &str, host: &str, config_key: &str) -> Result<Option<SpareHandle>> {
        self.0.query_opt(
            "UPDATE durable_object_spares SET status = 'claimed', host_id = $2, host_config_key = $3, expires_at = clock_timestamp() + interval '120 seconds' \
             WHERE name = (SELECT name FROM durable_object_spares WHERE pool_key = $1 AND status = 'ready' AND expires_at > clock_timestamp() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING handle",
            &[&key, &host, &config_key],
        ).await?.map(|row| serde_json::from_str(row.get::<_, &str>(0)).context("decode spare handle")).transpose()
    }

    async fn retire_unwanted(&self, keys: &[String], target: u32) -> Result<()> {
        self.0.execute(
            "UPDATE durable_object_spares SET status = 'retiring' WHERE kind = $3 AND ((expires_at <= clock_timestamp() AND (kind = 'actor' OR status != 'active')) \
             OR (status IN ('ready', 'starting') AND (NOT (pool_key = ANY($1)) OR $2::bigint = 0)))",
            &[&keys, &(target as i64), &self.1.as_str()],
        ).await?;
        self.0.execute(
            "UPDATE durable_object_spares SET status = 'retiring' WHERE name IN \
             (SELECT name FROM (SELECT name, row_number() OVER (PARTITION BY pool_key ORDER BY created_at) AS n \
             FROM durable_object_spares WHERE kind = $2 AND status IN ('ready', 'starting')) ranked WHERE n > $1)",
            &[&(target as i64), &self.1.as_str()],
        ).await?;
        Ok(())
    }

    async fn retiring(&self) -> Result<Vec<SpareHandle>> {
        let client = self.0.connection().await?;
        client
            .query(
                "SELECT name, handle FROM durable_object_spares WHERE status = 'retiring' AND kind = $1",
                &[&self.1.as_str()],
            )
            .await?
            .into_iter()
            .map(|row| decode_handle(&row))
            .collect()
    }
}

fn decode_handle(row: &tokio_postgres::Row) -> Result<SpareHandle> {
    match row.get::<_, Option<&str>>("handle") {
        Some(handle) => Ok(serde_json::from_str(handle)?),
        None => Ok(SpareHandle {
            name: row.get("name"),
            resource_id: String::new(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        }),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/pool.rs"]
mod tests;
