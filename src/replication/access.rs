use std::sync::Arc;

use anyhow::{Result, ensure};
use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::clock::Clock;

#[derive(Clone)]
pub struct ReplicaAccess {
    key: hmac::Key,
    namespace: Option<String>,
    clock: Arc<dyn Clock>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaGrant {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<super::ReplicaStream>,
    pub operation: String,
    pub object: String,
    pub region: String,
    pub host_id: String,
    pub archive_url: String,
    pub expires_at_ms: u64,
}

impl ReplicaAccess {
    pub fn new(secret: &str, clock: Arc<dyn Clock>) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
            namespace: None,
            clock,
        }
    }

    pub fn delegate_secret(&self, namespace: &str) -> Result<String> {
        crate::actor::ActorScope {
            namespace_id: namespace.into(),
        }
        .validate()?;
        ensure!(
            self.namespace.is_none(),
            "cannot delegate a scoped replica key"
        );
        Ok(URL_SAFE_NO_PAD
            .encode(hmac::sign(&self.key, format!("namespace:{namespace}").as_bytes()).as_ref()))
    }

    pub fn delegated(secret: &str, namespace: &str, clock: Arc<dyn Clock>) -> Self {
        let mut access = Self::new(secret, clock);
        access.namespace = Some(namespace.into());
        access
    }

    pub fn for_namespace(&self, namespace: &str) -> Result<Self> {
        Ok(Self::delegated(
            &self.delegate_secret(namespace)?,
            namespace,
            self.clock.clone(),
        ))
    }

    pub fn url(&self, origin: &str, resource: &str, grant: &ReplicaGrant) -> Result<String> {
        if let Some(namespace) = &self.namespace {
            ensure!(
                grant
                    .object
                    .starts_with(&crate::storage_paths::namespace(namespace)),
                "replica grant crossed namespace scope"
            );
        }
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(grant)?);
        let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&self.key, payload.as_bytes()).as_ref());
        let scope = self
            .namespace
            .as_ref()
            .map(|n| format!("{n}~"))
            .unwrap_or_default();
        Ok(format!(
            "{}/_replica/{resource}?token={scope}{payload}.{signature}",
            origin.trim_end_matches('/')
        ))
    }

    pub(crate) fn verify(&self, token: &str, operation: &str) -> Result<ReplicaGrant> {
        ensure!(token.len() <= 16 * 1024, "replica capability is too large");
        let (namespace, token) = token
            .split_once('~')
            .map_or((None, token), |(ns, token)| (Some(ns), token));
        let verifier = match (&self.namespace, namespace) {
            (None, Some(namespace)) => self.for_namespace(namespace)?,
            (None, None) => self.clone(),
            (Some(own), Some(namespace)) if own == namespace => self.clone(),
            _ => anyhow::bail!("replica grant crossed namespace scope"),
        };
        let (payload, signature) = token
            .split_once('.')
            .ok_or_else(|| anyhow::anyhow!("invalid replica capability"))?;
        hmac::verify(
            &verifier.key,
            payload.as_bytes(),
            &URL_SAFE_NO_PAD.decode(signature)?,
        )
        .map_err(|_| anyhow::anyhow!("invalid replica signature"))?;
        let grant: ReplicaGrant = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
        if let Some(namespace) = namespace {
            ensure!(
                grant
                    .object
                    .starts_with(&crate::storage_paths::namespace(namespace)),
                "replica grant crossed namespace scope"
            );
        }
        ensure!(
            grant.operation == operation,
            "replica operation is not authorized"
        );
        ensure!(
            grant.expires_at_ms > self.clock.now_ms()?,
            "replica capability expired"
        );
        ensure!(
            grant.object.starts_with(crate::storage_paths::ROOT) && grant.object.len() <= 1024,
            "invalid replica object"
        );
        ensure!(
            grant
                .stream
                .as_ref()
                .is_none_or(|stream| stream.prefix == grant.object
                    && stream.session.split('/').nth(3) == grant.object.split('/').nth(3)),
            "replica stream does not match its capability"
        );
        crate::placement::validate_region(&grant.region)?;
        Ok(grant)
    }
}

#[derive(Deserialize)]
pub(crate) struct AccessQuery {
    pub token: String,
    pub object: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_delegate_cannot_authorize_another_namespace() -> Result<()> {
        let root = ReplicaAccess::new("secret", Arc::new(crate::clock::SystemClock));
        let delegate = root.for_namespace("project")?;
        let grant = ReplicaGrant {
            stream: None,
            operation: "GET".into(),
            object: "little-actors/v1/namespaces/cHJvamVjdA/snapshots/aa/test/1.json".into(),
            region: "us-east".into(),
            host_id: "replica".into(),
            archive_url: String::new(),
            expires_at_ms: u64::MAX,
        };
        let url = delegate.url("http://replica", "state", &grant)?;
        root.verify(url.split("token=").nth(1).unwrap(), "GET")?;
        let forbidden = ReplicaGrant {
            object: "little-actors/v1/namespaces/b3RoZXI/snapshots/aa/test/1.json".into(),
            ..grant
        };
        assert!(delegate.url("http://replica", "state", &forbidden).is_err());
        let unscoped = ReplicaAccess::new(
            &root.delegate_secret("project")?,
            Arc::new(crate::clock::SystemClock),
        );
        let forged = unscoped.url("http://replica", "state", &forbidden)?;
        let forged = format!("project~{}", forged.split("token=").nth(1).unwrap());
        assert!(root.verify(&forged, "GET").is_err());
        let mismatched = ReplicaGrant {
            object: "little-actors/v1/namespaces/cHJvamVjdA/snapshots/aa/test/".into(),
            stream: Some(super::super::ReplicaStream {
                prefix: "little-actors/v1/namespaces/b3RoZXI/snapshots/aa/test/".into(),
                session: "little-actors/v1/namespaces/b3RoZXI/snapshots/sessions/one/".into(),
                owner_epoch: 1,
                base_version: 0,
            }),
            ..forbidden
        };
        let forged = delegate.url("http://replica", "state", &mismatched)?;
        assert!(
            root.verify(forged.split("token=").nth(1).unwrap(), "GET")
                .is_err()
        );
        Ok(())
    }
}
