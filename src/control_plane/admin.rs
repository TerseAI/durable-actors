use std::sync::Mutex;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::postgres::PostgresDatabase;

use super::ActorJwtIssuer;
use super::contracts::{PublicActorContract, PublishedContract, check_contract_hash};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostLaunchSpec {
    pub code_revision: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub secret_refs: Vec<String>,
}

impl HostLaunchSpec {
    pub(crate) fn host_revision(&self) -> String {
        if self.secret_refs.is_empty() {
            return self.code_revision.clone();
        }
        let identity = serde_json::to_vec(&(&self.code_revision, &self.secret_refs))
            .expect("host identity is serializable");
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &identity);
        format!("cfg.{}", URL_SAFE_NO_PAD.encode(digest.as_ref()))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_component("code revision", &self.code_revision, 128)?;
        ensure!(
            !self.image_ref.is_empty() && self.image_ref.len() <= 255,
            "sandbox image reference must contain between 1 and 255 bytes"
        );
        ensure!(
            self.working_directory.starts_with('/') && self.working_directory.len() <= 1024,
            "host working directory must be an absolute path of at most 1024 bytes"
        );
        if let Some(entrypoint) = &self.actor_entrypoint {
            ensure!(
                !entrypoint.is_empty() && entrypoint.len() <= 1024,
                "actor entrypoint must contain between 1 and 1024 bytes"
            );
        }
        ensure!(
            self.secret_refs.len() <= 16,
            "at most 16 secret references are allowed"
        );
        for reference in &self.secret_refs {
            validate_component("secret reference", reference, 255)?;
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait AdminRegistry: Send + Sync {
    #[cfg(test)]
    async fn register_test_deployment(&self, spec: &HostLaunchSpec) -> Result<bool> {
        self.register_deployment(spec, None).await
    }
    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool>;
    async fn deployment_contract(
        &self,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>>;
    async fn launch_spec(&self) -> Result<Option<HostLaunchSpec>>;
    async fn remove_deployment(&self) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct AdminService {
    api_key: String,
    registry: std::sync::Arc<dyn AdminRegistry>,
    issuer: ActorJwtIssuer,
}

impl AdminService {
    pub(crate) fn new(
        api_key: String,
        registry: std::sync::Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
    ) -> Result<Self> {
        ensure!(
            !api_key.is_empty() && api_key.trim() == api_key,
            "API key is invalid"
        );
        Ok(Self {
            api_key,
            registry,
            issuer,
        })
    }

    pub(super) fn issue_direct_socket(
        &self,
        grant: super::socket_ticket::SocketGrant,
        credentials: crate::sandbox::SocketCredentials,
    ) -> Result<serde_json::Value> {
        let mut url = reqwest::Url::parse(&credentials.url)?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none(),
            "invalid socket endpoint"
        );
        url.set_path("/v1/socket");
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("invalid socket endpoint"))?;
        let home_region = grant.region.clone();
        let (key, connect_by_ms, authorized_until_ms) = self.issuer.issue_socket(grant)?;
        if !credentials.token.is_empty() {
            url.query_pairs_mut()
                .append_pair("_modal_connect_token", &credentials.token);
        }
        url.query_pairs_mut().append_pair("key", &key);
        Ok(
            serde_json::json!({ "transport": "websocket", "homeRegion": home_region, "websocketUrl": url.as_str(), "connectByMs": connect_by_ms, "authorizedUntilMs": authorized_until_ms }),
        )
    }

    pub(crate) fn authenticate(&self, authorization: &str) -> Result<()> {
        let token = authorization
            .strip_prefix("Bearer ")
            .context("admin credential must use Bearer authentication")?;
        ensure!(
            !token.is_empty()
                && token.trim() == token
                && bool::from(token.as_bytes().ct_eq(self.api_key.as_bytes())),
            "admin credential is invalid"
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn register_test_deployment(&self, spec: &HostLaunchSpec) -> Result<bool> {
        self.registry.register_test_deployment(spec).await
    }

    pub(crate) async fn current_deployment(&self) -> Result<Option<HostLaunchSpec>> {
        self.registry.launch_spec().await
    }

    pub(crate) async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        self.registry.register_deployment(spec, contract).await
    }

    pub(crate) async fn deployment_contract(
        &self,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>> {
        if let Some(revision) = revision {
            validate_component("code revision", revision, 128)?;
        }
        self.registry.deployment_contract(revision).await
    }

    pub(crate) async fn validate_contract_registration(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<()> {
        if let Some(contract) = contract {
            let existing = self.deployment_contract(Some(&spec.code_revision)).await?;
            check_contract_hash(
                existing
                    .as_ref()
                    .map(|record| record.contract_hash.as_str()),
                contract,
            )?;
        }
        Ok(())
    }

    pub(crate) async fn remove_deployment(&self) -> Result<()> {
        self.registry.remove_deployment().await
    }

    pub(crate) fn jwks_json(&self) -> Result<Vec<u8>> {
        self.issuer.jwks_json()
    }
}

#[derive(Default)]
pub(crate) struct LocalAdminRegistry {
    state: Mutex<LocalAdminState>,
}

#[derive(Default)]
struct LocalAdminState {
    deployment: Option<HostLaunchSpec>,
    contract: Option<PublishedContract>,
}

#[async_trait]
impl AdminRegistry for LocalAdminRegistry {
    async fn remove_deployment(&self) -> Result<()> {
        *self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))? =
            LocalAdminState::default();
        Ok(())
    }

    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        spec.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        let mut changed = state.deployment.as_ref() != Some(spec);
        if let Some(contract) = contract {
            let existing = state
                .contract
                .as_ref()
                .filter(|record| record.code_revision == spec.code_revision);
            check_contract_hash(
                existing.map(|record| record.contract_hash.as_str()),
                contract,
            )?;
            changed |= existing.is_none();
            state.contract = Some(PublishedContract::new(&spec.code_revision, contract));
        }
        if state
            .contract
            .as_ref()
            .is_some_and(|record| record.code_revision != spec.code_revision)
        {
            state.contract = None;
        }
        state.deployment = Some(spec.clone());
        Ok(changed)
    }

    async fn deployment_contract(
        &self,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        Ok(state
            .contract
            .as_ref()
            .filter(|record| revision.is_none_or(|revision| record.code_revision == revision))
            .cloned())
    }

    async fn launch_spec(&self) -> Result<Option<HostLaunchSpec>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .deployment
            .clone())
    }
}

pub(crate) struct PostgresAdminRegistry {
    database: PostgresDatabase,
}

impl PostgresAdminRegistry {
    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl AdminRegistry for PostgresAdminRegistry {
    async fn remove_deployment(&self) -> Result<()> {
        self.database
            .execute("DELETE FROM durable_object_deployment", &[])
            .await?;
        Ok(())
    }

    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool> {
        spec.validate()?;
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let changed = transaction
            .execute(
                "INSERT INTO durable_object_deployment \
                   (singleton, code_revision, image_ref, working_directory, actor_entrypoint, secret_refs) \
                 VALUES (TRUE, $1, $2, $3, $4, $5) \
                 ON CONFLICT (singleton) DO UPDATE SET \
                   code_revision = EXCLUDED.code_revision, image_ref = EXCLUDED.image_ref, \
                   working_directory = EXCLUDED.working_directory, actor_entrypoint = EXCLUDED.actor_entrypoint, \
                   secret_refs = EXCLUDED.secret_refs, \
                   updated_at = clock_timestamp() \
                 WHERE (durable_object_deployment.code_revision, \
                        durable_object_deployment.image_ref, \
                        durable_object_deployment.working_directory, \
                        durable_object_deployment.actor_entrypoint, \
                        durable_object_deployment.secret_refs) \
                       IS DISTINCT FROM \
                       (EXCLUDED.code_revision, EXCLUDED.image_ref, \
                        EXCLUDED.working_directory, EXCLUDED.actor_entrypoint, EXCLUDED.secret_refs)",
                &[
                    &spec.code_revision,
                    &spec.image_ref,
                    &spec.working_directory,
                    &spec.actor_entrypoint,
                    &spec.secret_refs,
                ],
            )
            .await
            .context("register PostgreSQL deployment")? == 1;
        transaction
            .execute(
                "DELETE FROM durable_object_contracts WHERE code_revision <> $1",
                &[&spec.code_revision],
            )
            .await?;
        let mut published = false;
        if let Some(contract) = contract {
            let existing = transaction
                .query_opt(
                    "SELECT contract_hash FROM durable_object_contracts WHERE code_revision = $1",
                    &[&spec.code_revision],
                )
                .await?;
            check_contract_hash(existing.as_ref().map(|row| row.get::<_, &str>(0)), contract)?;
            if existing.is_none() {
                transaction.execute(
                    "INSERT INTO durable_object_contracts (singleton, code_revision, contract_hash, contract_json) VALUES (TRUE, $1, $2, $3)",
                    &[&spec.code_revision, &contract.hash(), &serde_json::to_string(contract.document())?],
                ).await?;
                published = true;
            }
        }
        transaction.commit().await?;
        Ok(changed || published)
    }

    async fn deployment_contract(
        &self,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>> {
        let row = self
            .database
            .query_opt(
                "SELECT code_revision, contract_hash, contract_json FROM durable_object_contracts \
             WHERE code_revision = COALESCE($1, \
               (SELECT code_revision FROM durable_object_deployment))",
                &[&revision],
            )
            .await?;
        row.map(|row| {
            Ok(PublishedContract {
                code_revision: row.get(0),
                contract_hash: row.get(1),
                contract: serde_json::from_str(row.get::<_, &str>(2))?,
            })
        })
        .transpose()
    }

    async fn launch_spec(&self) -> Result<Option<HostLaunchSpec>> {
        Ok(self
            .database
            .query_opt(
                "SELECT code_revision, image_ref, working_directory, actor_entrypoint, secret_refs \
                 FROM durable_object_deployment",
                &[],
            )
            .await
            .context("load PostgreSQL host launch spec")?
            .map(|row| HostLaunchSpec {
                code_revision: row.get(0),
                image_ref: row.get(1),
                working_directory: row.get(2),
                actor_entrypoint: row.get(3),
                secret_refs: row.get(4),
            }))
    }
}

pub(crate) fn validate_component(name: &str, value: &str, maximum: usize) -> Result<()> {
    ensure!(!value.is_empty(), "{name} must not be empty");
    ensure!(
        value.len() <= maximum,
        "{name} must be at most {maximum} bytes"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
        "{name} contains unsupported characters"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/admin.rs"]
mod tests;
