#[cfg(test)]
use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::{actor::ActorScope, postgres::PostgresDatabase};

use super::contracts::{PublicActorContract, PublishedContract, check_contract_hash};
use super::{ActorJwtIssuer, issuer::IssuedActorToken};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostLaunchSpec {
    pub namespace_id: String,
    pub code_revision: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub secret_refs: Vec<String>,
    pub socket_gateway_url: Option<String>,
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
        ActorScope {
            namespace_id: self.namespace_id.clone(),
        }
        .validate()?;
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
        if let Some(endpoint) = &self.socket_gateway_url {
            let url = reqwest::Url::parse(endpoint).context("invalid socket gateway URL")?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/",
                "socket gateway URL must be an HTTP(S) origin without credentials"
            );
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait AdminRegistry: Send + Sync {
    #[cfg(test)]
    async fn ensure_namespace_and_register_deployment(
        &self,
        spec: &HostLaunchSpec,
    ) -> Result<bool> {
        self.register_deployment(spec, None).await
    }
    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<bool>;
    async fn deployment_contract(
        &self,
        namespace_id: &str,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>>;
    async fn launch_spec(&self, namespace_id: &str) -> Result<Option<HostLaunchSpec>>;
    async fn remove_deployment(&self, namespace_id: &str) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct AdminService {
    api_key: String,
    pub(super) default_namespace: String,
    registry: std::sync::Arc<dyn AdminRegistry>,
    issuer: ActorJwtIssuer,
    socket_origin: Option<String>,
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
            default_namespace: "default".into(),
            registry,
            issuer,
            socket_origin: None,
        })
    }

    pub(super) fn with_default_namespace(mut self, namespace_id: &str) -> Result<Self> {
        validate_namespace(namespace_id)?;
        self.default_namespace = namespace_id.to_owned();
        Ok(self)
    }

    pub(super) fn with_socket_origin(mut self, origin: &str) -> Result<Self> {
        let url = reqwest::Url::parse(origin)?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none(),
            "invalid socket origin"
        );
        self.socket_origin = Some(url.to_string());
        Ok(self)
    }

    pub(super) async fn issue_socket(
        &self,
        grant: super::socket_ticket::SocketGrant,
    ) -> Result<serde_json::Value> {
        let spec = self
            .current_deployment(&grant.actor.namespace_id)
            .await?
            .context("actor deployment is not registered")?;
        let origin = spec
            .socket_gateway_url
            .or_else(|| self.socket_origin.clone())
            .context("socket origin is not configured")?;
        let mut url = reqwest::Url::parse(&origin)?.join("/v1/socket")?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("invalid socket URL"))?;
        let key = self.issuer.issue_socket(grant)?;
        url.query_pairs_mut().append_pair("key", &key);
        Ok(serde_json::json!({ "websocketUrl": url.as_str(), "key": key }))
    }

    pub(super) fn verify_socket(&self, token: &str) -> Result<super::socket_ticket::SocketTicket> {
        self.issuer.verify_socket(token)
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
    pub(crate) async fn ensure_namespace_and_register_deployment(
        &self,
        spec: &HostLaunchSpec,
    ) -> Result<bool> {
        self.registry
            .ensure_namespace_and_register_deployment(spec)
            .await
    }

    pub(crate) async fn current_deployment(
        &self,
        namespace_id: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        self.registry.launch_spec(namespace_id).await
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
        namespace: &str,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>> {
        validate_namespace(namespace)?;
        if let Some(revision) = revision {
            validate_component("code revision", revision, 128)?;
        }
        self.registry.deployment_contract(namespace, revision).await
    }

    pub(crate) async fn validate_contract_registration(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&PublicActorContract>,
    ) -> Result<()> {
        if let Some(contract) = contract {
            let existing = self
                .deployment_contract(&spec.namespace_id, Some(&spec.code_revision))
                .await?;
            check_contract_hash(
                existing
                    .as_ref()
                    .map(|record| record.contract_hash.as_str()),
                contract,
            )?;
        }
        Ok(())
    }

    pub(crate) async fn deployment_exists(&self, namespace_id: &str) -> Result<bool> {
        Ok(self.registry.launch_spec(namespace_id).await?.is_some())
    }

    pub(crate) async fn remove_deployment(&self, namespace_id: &str) -> Result<()> {
        self.registry.remove_deployment(namespace_id).await
    }

    pub(crate) fn issue_workflow_token(
        &self,
        namespace_id: &str,
        execution_id: &str,
        storage_region: &str,
        deadline_unix_ms: i64,
    ) -> Result<IssuedActorToken> {
        self.issuer
            .issue_workflow(namespace_id, execution_id, storage_region, deadline_unix_ms)
    }

    pub(crate) fn jwks_json(&self) -> Result<Vec<u8>> {
        self.issuer.jwks_json()
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct LocalAdminRegistry {
    state: Mutex<LocalAdminState>,
}

#[cfg(test)]
#[derive(Default)]
struct LocalAdminState {
    namespaces: HashSet<String>,
    launch_specs: HashMap<String, HostLaunchSpec>,
    contracts: HashMap<String, PublishedContract>,
}

#[cfg(test)]
#[async_trait]
impl AdminRegistry for LocalAdminRegistry {
    async fn remove_deployment(&self, namespace_id: &str) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        state.launch_specs.remove(namespace_id);
        state.contracts.remove(namespace_id);
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
        let mut changed = state.launch_specs.get(&spec.namespace_id) != Some(spec);
        if let Some(contract) = contract {
            let existing = state
                .contracts
                .get(&spec.namespace_id)
                .filter(|record| record.code_revision == spec.code_revision);
            check_contract_hash(
                existing.map(|record| record.contract_hash.as_str()),
                contract,
            )?;
            changed |= existing.is_none();
            state.contracts.insert(
                spec.namespace_id.clone(),
                PublishedContract::new(&spec.namespace_id, &spec.code_revision, contract),
            );
        }
        if state
            .contracts
            .get(&spec.namespace_id)
            .is_some_and(|record| record.code_revision != spec.code_revision)
        {
            state.contracts.remove(&spec.namespace_id);
        }
        state.namespaces.insert(spec.namespace_id.clone());
        state
            .launch_specs
            .insert(spec.namespace_id.clone(), spec.clone());
        Ok(changed)
    }

    async fn deployment_contract(
        &self,
        namespace_id: &str,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        Ok(state
            .contracts
            .get(namespace_id)
            .filter(|record| revision.is_none_or(|revision| record.code_revision == revision))
            .cloned())
    }

    async fn launch_spec(&self, namespace_id: &str) -> Result<Option<HostLaunchSpec>> {
        validate_namespace(namespace_id)?;
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .launch_specs
            .get(namespace_id)
            .cloned())
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
    async fn remove_deployment(&self, namespace_id: &str) -> Result<()> {
        validate_namespace(namespace_id)?;
        self.database
            .execute(
                "DELETE FROM durable_object_project_specs WHERE namespace_id = $1",
                &[&namespace_id],
            )
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
                "WITH ensured_namespace AS ( \
                   INSERT INTO durable_object_namespaces (namespace_id) VALUES ($1) \
                   ON CONFLICT (namespace_id) DO UPDATE SET namespace_id = EXCLUDED.namespace_id \
                   RETURNING namespace_id \
                 ) \
                 INSERT INTO durable_object_project_specs \
                   (namespace_id, code_revision, image_ref, working_directory, actor_entrypoint, secret_refs, socket_gateway_url) \
                 SELECT namespace_id, $2, $3, $4, $5, $6, $7 FROM ensured_namespace \
                 ON CONFLICT (namespace_id) DO UPDATE SET \
                   code_revision = EXCLUDED.code_revision, image_ref = EXCLUDED.image_ref, \
                   working_directory = EXCLUDED.working_directory, actor_entrypoint = EXCLUDED.actor_entrypoint, \
                   secret_refs = EXCLUDED.secret_refs, socket_gateway_url = EXCLUDED.socket_gateway_url, \
                   updated_at = clock_timestamp() \
                 WHERE (durable_object_project_specs.code_revision, \
                        durable_object_project_specs.image_ref, \
                        durable_object_project_specs.working_directory, \
                        durable_object_project_specs.actor_entrypoint, \
                        durable_object_project_specs.secret_refs, durable_object_project_specs.socket_gateway_url) \
                       IS DISTINCT FROM \
                       (EXCLUDED.code_revision, EXCLUDED.image_ref, \
                        EXCLUDED.working_directory, EXCLUDED.actor_entrypoint, EXCLUDED.secret_refs, EXCLUDED.socket_gateway_url)",
                &[
                    &spec.namespace_id,
                    &spec.code_revision,
                    &spec.image_ref,
                    &spec.working_directory,
                    &spec.actor_entrypoint,
                    &spec.secret_refs,
                    &spec.socket_gateway_url,
                ],
            )
            .await
            .context("ensure PostgreSQL namespace and register project deployment")? == 1;
        transaction.execute(
            "DELETE FROM durable_object_contracts WHERE namespace_id = $1 AND code_revision <> $2",
            &[&spec.namespace_id, &spec.code_revision],
        ).await?;
        let mut published = false;
        if let Some(contract) = contract {
            let existing = transaction.query_opt(
                "SELECT contract_hash FROM durable_object_contracts WHERE namespace_id = $1 AND code_revision = $2",
                &[&spec.namespace_id, &spec.code_revision],
            ).await?;
            check_contract_hash(existing.as_ref().map(|row| row.get::<_, &str>(0)), contract)?;
            if existing.is_none() {
                transaction.execute(
                    "INSERT INTO durable_object_contracts (namespace_id, code_revision, contract_hash, contract_json) VALUES ($1, $2, $3, $4)",
                    &[&spec.namespace_id, &spec.code_revision, &contract.hash(), &serde_json::to_string(contract.document())?],
                ).await?;
                published = true;
            }
        }
        transaction.commit().await?;
        Ok(changed || published)
    }

    async fn deployment_contract(
        &self,
        namespace_id: &str,
        revision: Option<&str>,
    ) -> Result<Option<PublishedContract>> {
        let row = self
            .database
            .query_opt(
                "SELECT code_revision, contract_hash, contract_json FROM durable_object_contracts \
             WHERE namespace_id = $1 AND code_revision = COALESCE($2, \
               (SELECT code_revision FROM durable_object_project_specs WHERE namespace_id = $1))",
                &[&namespace_id, &revision],
            )
            .await?;
        row.map(|row| {
            Ok(PublishedContract {
                namespace_id: namespace_id.into(),
                code_revision: row.get(0),
                contract_hash: row.get(1),
                contract: serde_json::from_str(row.get::<_, &str>(2))?,
            })
        })
        .transpose()
    }

    async fn launch_spec(&self, namespace_id: &str) -> Result<Option<HostLaunchSpec>> {
        validate_namespace(namespace_id)?;
        Ok(self
            .database
            .query_opt(
                "SELECT code_revision, image_ref, working_directory, actor_entrypoint, secret_refs, socket_gateway_url \
                 FROM durable_object_project_specs WHERE namespace_id = $1",
                &[&namespace_id],
            )
            .await
            .context("load PostgreSQL host launch spec")?
            .map(|row| HostLaunchSpec {
                namespace_id: namespace_id.to_owned(),
                code_revision: row.get(0),
                image_ref: row.get(1),
                working_directory: row.get(2),
                actor_entrypoint: row.get(3),
                secret_refs: row.get(4),
                socket_gateway_url: row.get(5),
            }))
    }
}

fn validate_namespace(namespace_id: &str) -> Result<()> {
    ActorScope {
        namespace_id: namespace_id.to_owned(),
    }
    .validate()
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
mod tests {
    use super::*;

    #[test]
    fn secret_changes_get_a_new_host_revision_without_changing_the_image_revision() {
        let mut deployment = spec("image-1");
        let initial = deployment.host_revision();
        deployment.secret_refs = vec!["secrets-first".into()];
        let first = deployment.host_revision();
        assert_ne!(initial, first);
        deployment.secret_refs = vec!["secrets-second".into()];
        assert_ne!(first, deployment.host_revision());
        assert_eq!(deployment.code_revision, "revision-1");
        deployment.secret_refs.clear();
        assert_eq!(initial, deployment.host_revision());
    }

    #[tokio::test]
    async fn deployment_retains_secret_references_and_gateway_address() -> Result<()> {
        let registry = LocalAdminRegistry::default();
        let mut deployment = spec("im-1");
        deployment.secret_refs = vec!["project-secrets".into()];
        deployment.socket_gateway_url = Some("https://sockets.example".into());
        assert!(
            registry
                .ensure_namespace_and_register_deployment(&deployment)
                .await?
        );
        assert_eq!(
            registry.launch_spec(&deployment.namespace_id).await?,
            Some(deployment.clone())
        );
        deployment.secret_refs = vec!["replacement-secrets".into()];
        assert!(
            registry
                .ensure_namespace_and_register_deployment(&deployment)
                .await?
        );
        deployment.socket_gateway_url = Some("https://user:password@sockets.example".into());
        assert!(deployment.validate().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn registration_replaces_the_projects_active_revision() -> Result<()> {
        let registry = LocalAdminRegistry::default();
        assert!(
            registry
                .ensure_namespace_and_register_deployment(&spec("im-1"))
                .await?
        );
        assert!(
            !registry
                .ensure_namespace_and_register_deployment(&spec("im-1"))
                .await?
        );
        let mut replacement = spec("im-2");
        replacement.code_revision = "revision-2".into();
        assert!(
            registry
                .ensure_namespace_and_register_deployment(&replacement)
                .await?
        );
        assert_eq!(registry.launch_spec("project-1").await?, Some(replacement));
        assert!(
            registry
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
                .namespaces
                .contains("project-1")
        );
        Ok(())
    }

    #[tokio::test]
    async fn postgres_registration_ensures_the_namespace_and_deployment_atomically() -> Result<()> {
        crate::postgres::testing::with_postgres(async |fixture| {
            let database = PostgresDatabase::connect(&fixture.url).await?;
            let registry = PostgresAdminRegistry::from_database(database.clone());
            let mut deployment = spec("image-1");
            deployment.namespace_id = format!("project-{}", uuid::Uuid::new_v4());
            deployment.secret_refs = vec!["project-secrets".into()];
            deployment.socket_gateway_url = Some("https://sockets.example".into());

            assert!(
                registry
                    .ensure_namespace_and_register_deployment(&deployment)
                    .await?
            );
            assert_eq!(
                registry.launch_spec(&deployment.namespace_id).await?,
                Some(deployment.clone())
            );
            let namespace_exists = database
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM durable_object_namespaces WHERE namespace_id = $1)",
                &[&deployment.namespace_id],
            )
            .await?
            .get::<_, bool>(0);
            assert!(namespace_exists);
            registry.remove_deployment(&deployment.namespace_id).await?;
            assert_eq!(registry.launch_spec(&deployment.namespace_id).await?, None);
            registry.remove_deployment(&deployment.namespace_id).await?;
            Ok(())
        })
        .await
    }

    fn spec(image: &str) -> HostLaunchSpec {
        HostLaunchSpec {
            namespace_id: "project-1".into(),
            code_revision: "revision-1".into(),
            image_ref: image.into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: Some("src/durable-objects.ts".into()),
            secret_refs: vec![],
            socket_gateway_url: None,
        }
    }
}
