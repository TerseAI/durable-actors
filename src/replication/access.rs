use std::sync::Arc;

use anyhow::{Result, ensure};
use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::clock::Clock;

#[derive(Clone)]
pub struct ReplicaAccess {
    key: hmac::Key,
    secret: Arc<str>,
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
    pub expires_at_ms: u64,
}

impl ReplicaAccess {
    pub fn new(secret: &str, clock: Arc<dyn Clock>) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
            secret: secret.into(),
            clock,
        }
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }

    pub fn url(&self, origin: &str, grant: &ReplicaGrant) -> Result<String> {
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(grant)?);
        let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&self.key, payload.as_bytes()).as_ref());
        let url = reqwest::Url::parse(origin)?;
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/"
                && url.host_str().is_some(),
            "invalid replica origin"
        );
        let scheme = match url.scheme() {
            "http" => "grpc",
            "https" => "grpcs",
            _ => anyhow::bail!("replica origin must be HTTP or HTTPS"),
        };
        let origin = url.origin().ascii_serialization();
        let address = origin.replacen(url.scheme(), scheme, 1);
        Ok(format!("{address}?token={payload}.{signature}"))
    }

    pub(crate) fn verify(&self, token: &str, operation: &str) -> Result<ReplicaGrant> {
        self.verify_any(token, &[operation])
    }

    pub(crate) fn verify_any(&self, token: &str, operations: &[&str]) -> Result<ReplicaGrant> {
        ensure!(token.len() <= 16 * 1024, "replica capability is too large");
        let (payload, signature) = token
            .split_once('.')
            .ok_or_else(|| anyhow::anyhow!("invalid replica capability"))?;
        hmac::verify(
            &self.key,
            payload.as_bytes(),
            &URL_SAFE_NO_PAD.decode(signature)?,
        )
        .map_err(|_| anyhow::anyhow!("invalid replica signature"))?;
        let grant: ReplicaGrant = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
        ensure!(
            operations.contains(&grant.operation.as_str()),
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
                    && stream
                        .session
                        .starts_with(&format!("{}hosts/", crate::storage_paths::ROOT))
                    && stream.owner_epoch > 0),
            "replica stream does not match its capability"
        );
        crate::placement::validate_region(&grant.region)?;
        Ok(grant)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/replication/access.rs"]
mod tests;
