use anyhow::{Context, Result, ensure};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::{Deserialize, Serialize};

use super::Region;
use crate::actor::ActorKey;

mod activity;
pub(crate) mod pool;
mod process;
pub use process::serve_proxy;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProxyConfig {
    pub actor: ActorKey,
    pub session: String,
    pub region: Region,
    pub keys: String,
    pub issuer: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProxyTransport {
    Invocation,
    Socket,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProxyDestination {
    pub route: String,
    pub token: String,
    pub owner_epoch: u64,
    pub expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_until_ms: Option<i64>,
    pub kind: ProxyTransport,
}

impl ProxyDestination {
    fn validate_authorization(&self, issued_at_ms: i64) -> Result<()> {
        match (self.kind, self.authorized_until_ms) {
            (ProxyTransport::Invocation, None) => Ok(()),
            (ProxyTransport::Socket, Some(until))
                if until >= self.expires_at_ms
                    && until <= issued_at_ms.saturating_add(86_401_000) =>
            {
                Ok(())
            }
            _ => anyhow::bail!("invalid proxy socket authorization lifetime"),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ProxyRequest {
    pub object_id: String,
    pub home_region: Region,
    pub destination: ProxyDestination,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProxyTicket {
    iss: String,
    aud: String,
    scope: String,
    iat: i64,
    nbf: i64,
    exp: i64,
    actor: ActorKey,
    session: String,
    pub destination: ProxyDestination,
}

impl ProxyTicket {
    pub(crate) fn expires_at_ms(&self) -> i64 {
        self.exp * 1000
    }

    pub(crate) fn new(config: &ProxyConfig, destination: ProxyDestination) -> Result<Self> {
        config.actor.validate()?;
        validate_origin(&destination.route)?;
        ensure!(
            destination.owner_epoch > 0 && !destination.token.is_empty(),
            "proxy requires an upstream capability"
        );
        let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64 / 1000;
        destination.validate_authorization(now * 1000)?;
        let expires = (destination.expires_at_ms / 1000).min(now + 60);
        ensure!(expires > now, "upstream capability has expired");
        Ok(Self {
            iss: config.issuer.clone(),
            aud: "durable-actors-proxy".into(),
            scope: "actor:proxy".into(),
            iat: now,
            nbf: now,
            exp: expires,
            actor: config.actor.clone(),
            session: config.session.clone(),
            destination,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ProxyVerifier {
    pub config: ProxyConfig,
    keys: JwkSet,
    validation: Validation,
}

impl ProxyVerifier {
    pub(crate) fn new(config: ProxyConfig) -> Result<Self> {
        config.actor.validate()?;
        uuid::Uuid::parse_str(&config.session)?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&config.issuer]);
        validation.set_audience(&["durable-actors-proxy"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        Ok(Self {
            keys: serde_json::from_str(&config.keys)?,
            config,
            validation,
        })
    }

    pub(crate) fn verify(&self, token: &str, kind: ProxyTransport) -> Result<ProxyTicket> {
        ensure!(token.len() <= 128 * 1024, "proxy ticket is too large");
        let header = decode_header(token)?;
        let key = self
            .keys
            .find(header.kid.as_deref().context("proxy key ID missing")?)
            .context("unknown proxy key")?;
        let ticket =
            decode::<ProxyTicket>(token, &DecodingKey::from_jwk(key)?, &self.validation)?.claims;
        ensure!(
            ticket.scope == "actor:proxy"
                && ticket.actor == self.config.actor
                && ticket.session == self.config.session
                && ticket.destination.kind == kind,
            "proxy capability scope mismatch"
        );
        ensure!(
            ticket.exp - ticket.iat <= 60
                && ticket.exp > ticket.iat
                && ticket.exp * 1000 <= ticket.destination.expires_at_ms,
            "invalid proxy capability lifetime"
        );
        ticket
            .destination
            .validate_authorization(ticket.iat * 1000)?;
        validate_origin(&ticket.destination.route)?;
        Ok(ticket)
    }
}

pub(crate) fn validate_origin(origin: &str) -> Result<()> {
    let url = reqwest::Url::parse(origin)?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "invalid regional service origin"
    );
    Ok(())
}
