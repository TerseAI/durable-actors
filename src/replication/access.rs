use std::sync::Arc;

use anyhow::{Result, ensure};
use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::clock::Clock;

#[derive(Clone)]
pub struct ReplicaAccess {
    key: hmac::Key,
    clock: Arc<dyn Clock>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaGrant {
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
            clock,
        }
    }

    pub fn url(&self, origin: &str, resource: &str, grant: &ReplicaGrant) -> Result<String> {
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(grant)?);
        let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&self.key, payload.as_bytes()).as_ref());
        Ok(format!(
            "{}/_replica/{resource}?token={payload}.{signature}",
            origin.trim_end_matches('/')
        ))
    }

    pub(crate) fn verify(&self, token: &str, operation: &str) -> Result<ReplicaGrant> {
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
            grant.operation == operation,
            "replica operation is not authorized"
        );
        ensure!(
            grant.expires_at_ms > self.clock.now_ms()?,
            "replica capability expired"
        );
        ensure!(
            grant.object.starts_with("snapshots/") && grant.object.len() <= 1024,
            "invalid replica object"
        );
        crate::placement::validate_region(&grant.region)?;
        Ok(grant)
    }
}

#[derive(Deserialize)]
pub(crate) struct AccessQuery {
    pub token: String,
}
