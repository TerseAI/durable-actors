use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::{ActorKey, validate_socket_metadata},
    placement::validate_region,
};

pub(super) struct SocketGrant {
    pub actor: ActorKey,
    pub region: String,
    pub metadata: Value,
    pub authorization_lifetime_ms: i64,
}

impl SocketGrant {
    pub(super) fn validate(&self) -> Result<()> {
        self.actor.validate()?;
        validate_region(&self.region)?;
        validate_socket_metadata(&self.metadata)?;
        ensure!(
            (1_000..=86_400_000).contains(&self.authorization_lifetime_ms),
            "socket authorization lifetime must be between one second and one day"
        );
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SocketTicket {
    pub iss: String,
    pub aud: String,
    pub scope: String,
    pub iat: i64,
    pub nbf: i64,
    pub exp: i64,
    pub actor: ActorKey,
    pub region: String,
    pub metadata: Value,
    pub connect_by_ms: i64,
    pub authorized_until_ms: i64,
}

impl SocketTicket {
    pub(super) fn validate(&self, now_ms: i64) -> Result<()> {
        self.actor.validate()?;
        validate_region(&self.region)?;
        validate_socket_metadata(&self.metadata)?;
        ensure!(self.scope == "actor:socket", "invalid socket ticket scope");
        ensure!(
            self.iat * 1000 <= now_ms && self.nbf * 1000 <= now_ms,
            "socket ticket is not active"
        );
        ensure!(
            now_ms < self.connect_by_ms && now_ms < self.authorized_until_ms,
            "socket ticket has expired"
        );
        ensure!(
            self.connect_by_ms <= self.authorized_until_ms
                && self.connect_by_ms - self.iat * 1000 <= 61_000,
            "invalid socket ticket admission lifetime"
        );
        ensure!(
            self.authorized_until_ms - self.iat * 1000 <= 86_401_000,
            "invalid socket authorization lifetime"
        );
        Ok(())
    }
}
