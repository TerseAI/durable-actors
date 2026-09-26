use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::Serialize;

use super::ProxyConfig;
use crate::{
    postgres::PostgresDatabase,
    sandbox::{
        CommandSandboxProvider, ResourceLimits, SandboxProvider, SocketCredentials,
        SocketCredentialsRequest, SpareHandle,
    },
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyLaunch {
    name: String,
    image_ref: String,
    canonical_region: String,
    resources: ResourceLimits,
    config: ProxyConfig,
}

#[async_trait]
pub(crate) trait ProxyProvisioner: Send + Sync {
    async fn create(&self, launch: &ProxyLaunch) -> Result<SpareHandle>;
    async fn retire(&self, handle: &SpareHandle) -> Result<()>;
    async fn socket_credentials(
        &self,
        handle: &SpareHandle,
        session: &str,
    ) -> Result<SocketCredentials>;
}

#[async_trait]
impl ProxyProvisioner for CommandSandboxProvider {
    async fn create(&self, launch: &ProxyLaunch) -> Result<SpareHandle> {
        self.execute("ensure_proxy", launch).await
    }

    async fn retire(&self, handle: &SpareHandle) -> Result<()> {
        self.retire_spare(handle).await
    }

    async fn socket_credentials(
        &self,
        handle: &SpareHandle,
        session: &str,
    ) -> Result<SocketCredentials> {
        SandboxProvider::socket_credentials(
            self,
            &SocketCredentialsRequest {
                resource_id: Some(handle.resource_id.clone()),
                canonical_region: handle.canonical_region.clone(),
                host_id: crate::host::HostId::new(format!("proxy.v1.{session}")),
                session_id: session.into(),
            },
        )
        .await
    }
}

pub(crate) struct ProxyPool {
    database: PostgresDatabase,
    provider: Arc<dyn ProxyProvisioner>,
    image: String,
    health: reqwest::Client,
}

pub(crate) struct ReadyProxy {
    pub config: ProxyConfig,
    pub handle: SpareHandle,
}

impl ProxyPool {
    pub(crate) fn new(
        database: PostgresDatabase,
        provider: Arc<dyn ProxyProvisioner>,
        image: String,
    ) -> Result<Self> {
        Ok(Self {
            database,
            provider,
            image,
            health: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    pub(crate) async fn ensure(
        &self,
        object_id: &str,
        template: &ProxyConfig,
    ) -> Result<ReadyProxy> {
        tokio::time::timeout(
            Duration::from_secs(100),
            self.ensure_until_ready(object_id, template),
        )
        .await
        .context("regional proxy preparation timed out")?
    }

    pub(crate) async fn socket_credentials(&self, ready: &ReadyProxy) -> Result<SocketCredentials> {
        self.provider
            .socket_credentials(&ready.handle, &ready.config.session)
            .await
    }

    async fn ensure_until_ready(
        &self,
        object_id: &str,
        template: &ProxyConfig,
    ) -> Result<ReadyProxy> {
        loop {
            if let Some(ready) = self.current(object_id, template).await? {
                if self.healthy(&ready.handle.route).await {
                    return Ok(ready);
                }
                self.expire(object_id, &ready.config).await?;
                if let Err(error) = self.provider.retire(&ready.handle).await {
                    tracing::warn!(error = %error, "unreachable proxy retirement deferred to sandbox timeout");
                }
            }
            let mut config = template.clone();
            config.session = uuid::Uuid::new_v4().to_string();
            if self.claim(object_id, &config).await? {
                return self.create(object_id, config).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn healthy(&self, route: &str) -> bool {
        for attempt in 0..3 {
            if self
                .health
                .get(format!("{}/healthz", route.trim_end_matches('/')))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return true;
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        false
    }

    async fn current(&self, object_id: &str, template: &ProxyConfig) -> Result<Option<ReadyProxy>> {
        let row = self.database.query_opt(
            "SELECT session_id, handle FROM durable_actors_proxies WHERE object_id = $1 AND region = $2 \
             AND protocol_version = 1 AND handle IS NOT NULL AND expires_at > clock_timestamp()",
            &[&object_id, &template.region.as_str()],
        ).await?;
        row.map(|row| {
            let mut config = template.clone();
            config.session = row.get(0);
            let handle: SpareHandle = serde_json::from_str(row.get(1))?;
            ensure!(
                handle.canonical_region == config.region.as_str(),
                "stored proxy placement mismatch"
            );
            super::validate_origin(&handle.route)?;
            Ok(ReadyProxy { config, handle })
        })
        .transpose()
    }

    async fn claim(&self, object_id: &str, config: &ProxyConfig) -> Result<bool> {
        let changed = self.database.execute(
            "INSERT INTO durable_actors_proxies (object_id, region, protocol_version, session_id, expires_at) \
             VALUES ($1, $2, 1, $3, clock_timestamp() + interval '90 seconds') \
             ON CONFLICT (object_id, region, protocol_version) DO UPDATE SET session_id = $3, handle = NULL, \
             expires_at = EXCLUDED.expires_at WHERE durable_actors_proxies.expires_at <= clock_timestamp()",
            &[&object_id, &config.region.as_str(), &config.session],
        ).await?;
        Ok(changed == 1)
    }

    async fn create(&self, object_id: &str, config: ProxyConfig) -> Result<ReadyProxy> {
        let launch = ProxyLaunch {
            name: format!("do-proxy-{}", config.session),
            image_ref: self.image.clone(),
            canonical_region: config.region.as_str().into(),
            resources: ResourceLimits::default(),
            config: config.clone(),
        };
        let handle = match self.provider.create(&launch).await {
            Ok(handle) => handle,
            Err(error) => {
                self.expire(object_id, &config).await?;
                return Err(error);
            }
        };
        let result = self.publish(object_id, &config, &handle).await;
        if let Err(error) = result {
            let _ = self.provider.retire(&handle).await;
            return Err(error);
        }
        Ok(ReadyProxy { config, handle })
    }

    async fn publish(
        &self,
        object_id: &str,
        config: &ProxyConfig,
        handle: &SpareHandle,
    ) -> Result<()> {
        ensure!(
            handle.canonical_region == config.region.as_str() && !handle.resource_id.is_empty(),
            "provider returned invalid regional proxy"
        );
        super::validate_origin(&handle.route)?;
        let written = self.database.execute(
            "UPDATE durable_actors_proxies SET handle = $4, expires_at = clock_timestamp() + interval '23 hours' \
             WHERE object_id = $1 AND region = $2 AND protocol_version = 1 AND session_id = $3 \
             AND handle IS NULL AND expires_at > clock_timestamp()",
            &[&object_id, &config.region.as_str(), &config.session, &serde_json::to_string(handle)?],
        ).await?;
        ensure!(written == 1, "proxy provisioning claim expired");
        Ok(())
    }

    async fn expire(&self, object_id: &str, config: &ProxyConfig) -> Result<()> {
        self.database.execute(
            "UPDATE durable_actors_proxies SET expires_at = clock_timestamp() WHERE object_id = $1 AND region = $2 AND protocol_version = 1 AND session_id = $3",
            &[&object_id, &config.region.as_str(), &config.session],
        ).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/regional/proxy_pool.rs"]
mod tests;
