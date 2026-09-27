use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};

use super::auth::{bearer_token, decode_public_keys, unix_seconds};

#[derive(Clone)]
pub(crate) struct ProjectGrantVerifier {
    keys: Arc<HashMap<String, DecodingKey>>,
    validation: Validation,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ProjectGrant {
    #[serde(rename = "sub")]
    pub subject: String,
    pub jti: String,
    #[serde(rename = "projectId")]
    project_id: String,
    scope: String,
    iat: i64,
    #[serde(rename = "exp")]
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InvocationGrant {
    pub subject: String,
    pub grant_id: String,
    pub expires_at: i64,
    pub methods: Vec<String>,
}

impl ProjectGrantVerifier {
    pub(crate) fn new(keys: &str, issuer: &str, audience: &str) -> Result<Self> {
        ensure!(
            !issuer.is_empty() && !audience.is_empty(),
            "project grant issuer and audience are required"
        );
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["iss", "aud", "sub", "exp", "nbf"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        Ok(Self {
            keys: Arc::new(decode_public_keys(keys)?),
            validation,
        })
    }

    pub(crate) fn authenticate(
        &self,
        authorization: &str,
        project_id: &str,
    ) -> Result<ProjectGrant> {
        let token = bearer_token(authorization)?;
        ensure!(token.len() <= 8192, "project grant is too large");
        let header = decode_header(token)?;
        ensure!(
            header.typ.as_deref() == Some("JWT"),
            "invalid project grant type"
        );
        let key = self
            .keys
            .get(
                header
                    .kid
                    .as_deref()
                    .context("project grant key ID is required")?,
            )
            .context("unknown project grant key")?;
        let grant = decode::<ProjectGrant>(token, key, &self.validation)?.claims;
        let now = unix_seconds()?;
        ensure!(
            grant.scope == "actor:resolve" && grant.project_id == project_id,
            "project grant scope mismatch"
        );
        ensure!(
            !grant.subject.is_empty()
                && grant.subject.len() <= 128
                && !grant.jti.is_empty()
                && grant.jti.len() <= 128,
            "invalid project grant identity"
        );
        ensure!(
            grant.iat <= now + 5
                && grant.expires_at > now
                && grant.expires_at > grant.iat
                && grant.expires_at.saturating_sub(grant.iat) <= 60,
            "invalid project grant lifetime"
        );
        Ok(grant)
    }
}

impl ProjectGrant {
    pub(crate) fn invocation(self, methods: Vec<String>) -> InvocationGrant {
        InvocationGrant {
            subject: self.subject,
            grant_id: self.jti,
            expires_at: self.expires_at,
            methods,
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/project_grant.rs"]
mod tests;
