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
                &spec.host_revision(),
            )
            .await?;
        self.wake.notify_one();
        Ok(result)
    }

    pub async fn reserve_host(&self, session: &str, host: &str, revision: &str) -> Result<()> {
        let name = format!("do-actor-{session}");
        self.store.0.execute("INSERT INTO durable_object_spares (name, pool_key, status, host_id, code_revision, expires_at) VALUES ($1, '', 'claimed', $2, $3, clock_timestamp() + interval '120 seconds')", &[&name, &host, &revision]).await?;
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

    pub async fn remember(&self, host: &str, revision: &str, spare: &SpareHandle) -> Result<()> {
        let updated = self.store.0.execute(
            "UPDATE durable_object_spares SET status = 'active', handle = $2, expires_at = created_at + interval '24 hours' \
             WHERE name = $1 AND host_id = $3 AND code_revision = $4 AND status = 'claimed' AND expires_at > clock_timestamp()",
            &[&spare.name, &serde_json::to_string(spare)?, &host, &revision],
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

    pub async fn retire_revision(&self, revision: &str) -> Result<Vec<String>> {
        let client = self.store.0.connection().await?;
        let rows = client.query("UPDATE durable_object_spares SET status = 'retiring' WHERE code_revision = $1 RETURNING handle", &[&revision]).await?;
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
        let deployment = registry.launch_spec().await?;
        let deployment = deployment.filter(|spec| {
            self.config.kind == SpareKind::Replica
                || (spec.code_snapshot.is_some() && spec.secret_refs.is_empty())
        });
        let keys: Vec<String> = deployment
            .as_ref()
            .map(|spec| {
                self.config
                    .regions
                    .iter()
                    .map(|region| self.key(&spec.image_ref, region))
                    .collect()
            })
            .unwrap_or_default();
        self.store.retire_unwanted(&keys, self.config.idle).await?;
        self.cleanup().await?;
        if let Some(spec) = deployment {
            for region in &self.config.regions {
                let key = self.key(&spec.image_ref, region);
                let mut builds = Vec::new();
                while let Some(name) = self.store.reserve(&key, self.config.idle).await? {
                    builds.push(self.build(&key, &spec.image_ref, region, name));
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

    async fn claim(&self, key: &str, host: &str, revision: &str) -> Result<Option<SpareHandle>> {
        self.0.query_opt(
            "UPDATE durable_object_spares SET status = 'claimed', host_id = $2, code_revision = $3, expires_at = clock_timestamp() + interval '120 seconds' \
             WHERE name = (SELECT name FROM durable_object_spares WHERE pool_key = $1 AND status = 'ready' AND expires_at > clock_timestamp() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING handle",
            &[&key, &host, &revision],
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
mod tests {
    use super::*;
    use crate::postgres::testing::with_postgres;

    fn pool(database: PostgresDatabase) -> Arc<SparePool> {
        SparePool::new(
            database,
            Arc::new(
                super::super::CommandSandboxProvider::new(
                    "test".into(),
                    "false".into(),
                    Default::default(),
                )
                .unwrap(),
            ),
            PoolConfig {
                kind: SpareKind::Actor,
                idle: 1,
                idle_ttl_seconds: 600,
                regions: vec!["region".into()],
                resources: ResourceLimits::default(),
            },
        )
    }

    #[tokio::test]
    async fn only_live_claims_can_become_routable() -> Result<()> {
        with_postgres(async |fixture| {
            let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
            pool.reserve_host("session", "host", "revision").await?;
            let spare = SpareHandle {
                control_route: String::new(),
                control_token: String::new(),
                name: "do-actor-session".into(),
                resource_id: "sb-test".into(),
                route: "https://spare.test".into(),
                canonical_region: "region".into(),
            };
            assert!(pool.host("host").await?.is_none());
            assert!(
                tokio::time::timeout(Duration::from_millis(50), pool.wait_ready("host"))
                    .await
                    .is_err()
            );
            pool.remember("host", "revision", &spare).await?;
            pool.wait_ready("host").await?;
            assert_eq!(pool.host("host").await?, Some(spare.clone()));
            pool.failed("host").await?;
            assert!(pool.remember("host", "revision", &spare).await.is_err());
            assert!(pool.wait_ready("host").await.is_err());
            assert!(pool.host("host").await?.is_none());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn expired_claims_and_outdated_runtime_spares_cannot_be_reused() -> Result<()> {
        with_postgres(async |fixture| {
            let pool = pool(PostgresDatabase::connect(&fixture.url).await?);
            pool.reserve_host("expired", "host", "revision").await?;
            pool.store.0.execute("UPDATE durable_object_spares SET expires_at = clock_timestamp() - interval '1 second'", &[]).await?;
            let spare = SpareHandle {
 control_route: String::new(),
 control_token: String::new(), name: "do-actor-expired".into(), resource_id: "sb-expired".into(), route: "https://spare.test".into(), canonical_region: "region".into() };
            assert!(pool.remember("host", "revision", &spare).await.is_err());
            let old = pool.store.reserve("old-runtime", 1).await?.unwrap();
            let current = pool.store.reserve("current-runtime", 1).await?.unwrap();
            pool.store.retire_unwanted(&["current-runtime".into()], 1).await?;
            let retiring = pool.store.retiring().await?;
            assert!(retiring.iter().any(|s| s.name == old));
            assert!(retiring.iter().any(|s| s.name == spare.name));
            assert!(!retiring.iter().any(|s| s.name == current));
            Ok(())
        }).await
    }

    #[tokio::test]
    async fn concurrent_pool_claims_are_exclusive_and_never_return_assigned_sandboxes() -> Result<()>
    {
        with_postgres(async |fixture| {
            let store = PoolStore(
                PostgresDatabase::connect(&fixture.url).await?,
                SpareKind::Actor,
            );
            let key = "runtime-a/region-a/1000/1024";
            let names =
                futures_util::future::try_join_all((0..8).map(|_| store.reserve(key, 2))).await?;
            let names = names.into_iter().flatten().collect::<Vec<_>>();
            assert_eq!(names.len(), 2);
            assert!(store.claim(key, "host-early", "revision").await?.is_none());
            for name in &names {
                assert!(
                    store
                        .publish(
                            key,
                            &SpareHandle {
                                control_route: String::new(),
                                control_token: String::new(),
                                name: name.clone(),
                                resource_id: format!("sb-{name}"),
                                route: "https://spare.test".into(),
                                canonical_region: "region-a".into()
                            },
                            600
                        )
                        .await?
                );
            }
            assert!(
                store
                    .claim("other-runtime", "host-wrong", "revision")
                    .await?
                    .is_none()
            );
            let claims = futures_util::future::try_join_all((0..8).map(|i| {
                let store = &store;
                async move { store.claim(key, &format!("host-{i}"), "revision").await }
            }))
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            assert_eq!(claims.len(), 2);
            assert_ne!(claims[0].resource_id, claims[1].resource_id);
            assert!(store.claim(key, "host-retry", "revision").await?.is_none());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn zero_capacity_and_runtime_changes_retire_only_unassigned_spares() -> Result<()> {
        with_postgres(async |fixture| {
            let store = PoolStore(
                PostgresDatabase::connect(&fixture.url).await?,
                SpareKind::Actor,
            );
            assert!(store.reserve("runtime", 0).await?.is_none());
            let name = store.reserve("runtime", 1).await?.unwrap();
            let spare = SpareHandle {
                control_route: String::new(),
                control_token: String::new(),
                name,
                resource_id: "sb-test".into(),
                route: "https://spare.test".into(),
                canonical_region: "region".into(),
            };
            store.publish("runtime", &spare, 600).await?;
            store.retire_unwanted(&[], 0).await?;
            assert!(store.claim("runtime", "host", "rev").await?.is_none());
            assert_eq!(store.retiring().await?.len(), 1);
            assert!(!store.publish("runtime", &spare, 600).await?);
            Ok(())
        })
        .await
    }
}
