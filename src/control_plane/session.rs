use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};

use super::{
    admin::validate_component,
    auth::{bearer_token, decode_public_keys, unix_seconds},
};
use crate::actor::ActorKey;

#[derive(Clone)]
pub(crate) struct SessionVerifier {
    keys: Arc<HashMap<String, DecodingKey>>,
    validation: Validation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ActorSession {
    pub iss: String,
    pub aud: String,
    #[serde(rename = "sub")]
    pub subject: String,
    pub jti: String,
    #[serde(rename = "projectId")]
    pub project_id: String,
    pub scope: String,
    pub permissions: Vec<SessionPermission>,
    pub iat: i64,
    pub nbf: i64,
    #[serde(rename = "exp")]
    pub expires_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SessionPermission {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InvocationGrant {
    pub subject: String,
    pub grant_id: String,
    pub expires_at: i64,
    pub methods: Vec<String>,
}

impl SessionVerifier {
    pub(super) fn new(keys: &str, issuer: &str, audience: &str) -> Result<Self> {
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
    ) -> Result<ActorSession> {
        let token = bearer_token(authorization)?;
        ensure!(token.len() <= 8192, "actor session is too large");
        let header = decode_header(token)?;
        ensure!(
            header.typ.as_deref() == Some("JWT"),
            "invalid actor session type"
        );
        let key = self
            .keys
            .get(
                header
                    .kid
                    .as_deref()
                    .context("actor session key ID is required")?,
            )
            .context("unknown actor session key")?;
        let session = decode::<ActorSession>(token, key, &self.validation)?.claims;
        session.validate(unix_seconds()?)?;
        ensure!(
            session.project_id == project_id,
            "actor session project mismatch"
        );
        Ok(session)
    }
}

impl ActorSession {
    pub(super) fn validate(&self, now: i64) -> Result<()> {
        ensure!(
            self.scope == "actor:session",
            "actor session purpose mismatch"
        );
        validate_component("project ID", &self.project_id, 64)?;
        ensure!(
            !self.subject.is_empty()
                && self.subject.len() <= 128
                && !self.jti.is_empty()
                && self.jti.len() <= 128,
            "invalid actor session identity"
        );
        ensure!(
            self.iat <= now + 5
                && self.expires_at > now
                && self.expires_at > self.iat
                && self.expires_at.saturating_sub(self.iat) <= 60,
            "invalid actor session lifetime"
        );
        ensure!(
            !self.permissions.is_empty() && self.permissions.len() <= 32,
            "actor session requires 1-32 permission rules"
        );
        for permission in &self.permissions {
            permission.validate()?;
        }
        Ok(())
    }

    pub(crate) fn contains(&self, actor: &ActorKey) -> bool {
        self.project_id == actor.project_id
            && self
                .permissions
                .iter()
                .any(|permission| permission.matches(actor))
    }

    pub(crate) fn invocation(
        self,
        actor: &ActorKey,
        published_methods: Vec<String>,
    ) -> Result<InvocationGrant> {
        ensure!(self.contains(actor), "actor is outside the session scope");
        let methods: Vec<_> = published_methods
            .into_iter()
            .filter(|method| {
                self.permissions.iter().any(|permission| {
                    permission.matches(actor)
                        && permission
                            .methods
                            .as_ref()
                            .is_none_or(|methods| methods.contains(method))
                })
            })
            .collect();
        ensure!(
            !methods.is_empty(),
            "session has no published RPC methods for this actor"
        );
        Ok(InvocationGrant {
            subject: self.subject,
            grant_id: self.jti,
            expires_at: self.expires_at,
            methods,
        })
    }
}

impl SessionPermission {
    fn validate(&self) -> Result<()> {
        if let Some(name) = &self.actor_name {
            validate_component("actor name", name, 48)?;
        }
        if let Some(id) = &self.actor_id {
            validate_component("actor ID", id, 128)?;
        }
        if let Some(methods) = &self.methods {
            ensure!(
                !methods.is_empty() && methods.len() <= 256,
                "session method list requires 1-256 methods"
            );
            for method in methods {
                validate_component("method", method, 128)?;
            }
        }
        Ok(())
    }

    fn matches(&self, actor: &ActorKey) -> bool {
        self.actor_name
            .as_ref()
            .is_none_or(|name| name == &actor.actor_name)
            && self
                .actor_id
                .as_ref()
                .is_none_or(|id| id == &actor.actor_id)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/session.rs"]
mod tests;
