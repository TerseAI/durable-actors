use std::time::Duration;

use super::socket_ticket::{SocketGrant, SocketTicket};
use anyhow::{Context, Result, ensure};
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
    jwk::{Jwk, JwkSet, PublicKeyUse},
};
use serde::Serialize;

use crate::{
    actor::ActorKey,
    control_plane::auth::ActorInvocationCapability,
    host::{ActorProcessRole, HostId},
    placement::validate_region,
};

const WORKFLOW_DEADLINE_GRACE: Duration = Duration::from_secs(30);
const MAX_WORKFLOW_LIFETIME: Duration = Duration::from_secs(86_400);
const HOST_TOKEN_TTL: Duration = Duration::from_secs(1_800);
const INVOCATION_TARGET_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct ActorJwtIssuer {
    encoding_key: EncodingKey,
    public_key: Jwk,
    key_id: String,
    issuer: String,
    authority_audience: String,
    invocation_audience: String,
    max_lifetime: Duration,
}

pub(crate) struct IssuedActorToken {
    pub token: String,
    pub expires_at_ms: i64,
}

impl ActorJwtIssuer {
    pub(crate) fn from_base64_pkcs8(
        encoded_key: &str,
        key_id: impl Into<String>,
        issuer: impl Into<String>,
        authority_audience: impl Into<String>,
        invocation_audience: impl Into<String>,
        max_lifetime: Duration,
    ) -> Result<Self> {
        let key_id = key_id.into();
        ensure!(
            !key_id.is_empty(),
            "DURABLE_OBJECT_JWT_KEY_ID must not be empty"
        );
        ensure!(
            !max_lifetime.is_zero(),
            "actor JWT lifetime must be positive"
        );
        let issuer = issuer.into();
        let authority_audience = authority_audience.into();
        let invocation_audience = invocation_audience.into();
        ensure!(!issuer.is_empty(), "actor JWT issuer must not be empty");
        ensure!(
            !authority_audience.is_empty(),
            "actor authority JWT audience must not be empty"
        );
        ensure!(
            !invocation_audience.is_empty(),
            "actor invocation JWT audience must not be empty"
        );
        let pkcs8 = STANDARD
            .decode(encoded_key)
            .context("DURABLE_OBJECT_JWT_SIGNING_KEY must be base64-encoded PKCS#8")?;
        let key_pair = Ed25519KeyPair::from_pkcs8(&pkcs8)
            .context("DURABLE_OBJECT_JWT_SIGNING_KEY is not an Ed25519 PKCS#8 key")?;
        let encoding_key = EncodingKey::from_ed_der(&pkcs8);
        let mut public_key: Jwk = serde_json::from_value(serde_json::json!({
            "alg": "EdDSA",
            "crv": "Ed25519",
            "kty": "OKP",
            "x": URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref())
        }))
        .context("derive the actor JWT public key")?;
        public_key.common.key_id = Some(key_id.clone());
        public_key.common.public_key_use = Some(PublicKeyUse::Signature);
        Ok(Self {
            encoding_key,
            public_key,
            key_id,
            issuer,
            authority_audience,
            invocation_audience,
            max_lifetime,
        })
    }

    pub(crate) fn verifier_keys_json(&self) -> Result<String> {
        Ok(String::from_utf8(self.jwks_json()?)?)
    }

    pub(super) fn issue_socket(&self, grant: SocketGrant) -> Result<String> {
        self.issue_socket_at(grant, unix_millis()?)
    }

    fn issue_socket_at(&self, grant: SocketGrant, now_ms: i64) -> Result<String> {
        grant.validate()?;
        let lifetime = grant
            .authorization_lifetime_ms
            .min(duration_millis(self.max_lifetime)?);
        ensure!(
            lifetime >= 1000,
            "configured token lifetime is too short for a socket"
        );
        let authorized_until_ms = now_ms
            .checked_add(lifetime)
            .context("socket authorization time overflow")?;
        let connect_by_ms = authorized_until_ms.min(now_ms + 60_000);
        let claims = SocketTicket {
            iss: self.issuer.clone(),
            aud: format!("{}:websocket", self.authority_audience),
            scope: "actor:socket".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: (connect_by_ms + 999) / 1000,
            actor: grant.actor,
            region: grant.region,
            metadata: grant.metadata,
            authorized_until_ms,
            connect_by_ms,
        };
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.key_id.clone());
        Ok(encode(&header, &claims, &self.encoding_key)?)
    }

    pub(super) fn verify_socket(&self, token: &str) -> Result<SocketTicket> {
        self.verify_socket_at(token, unix_millis()?)
    }

    fn verify_socket_at(&self, token: &str, now_ms: i64) -> Result<SocketTicket> {
        ensure!(token.len() <= 128 * 1024, "socket ticket is too large");
        let header = decode_header(token)?;
        ensure!(
            header.kid.as_deref() == Some(&self.key_id) && header.alg == Algorithm::EdDSA,
            "socket signing key is invalid"
        );
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[format!("{}:websocket", self.authority_audience)]);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        let claims = decode::<SocketTicket>(
            token,
            &DecodingKey::from_jwk(&self.public_key)?,
            &validation,
        )?
        .claims;
        claims.validate(now_ms)?;
        Ok(claims)
    }

    pub(crate) fn jwks_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&JwkSet {
            keys: vec![self.public_key.clone()],
        })?)
    }

    pub(crate) fn issue_workflow(
        &self,
        namespace_id: &str,
        execution_id: &str,
        storage_region: &str,
        deadline_unix_ms: i64,
    ) -> Result<IssuedActorToken> {
        validate_region(storage_region)?;
        let now_ms = unix_millis()?;
        ensure!(
            deadline_unix_ms > now_ms,
            "workflow deadline must be in the future"
        );
        let maximum_expiration = now_ms.saturating_add(duration_millis(
            self.max_lifetime.min(MAX_WORKFLOW_LIFETIME),
        )?);
        let requested_expiration =
            deadline_unix_ms.saturating_add(duration_millis(WORKFLOW_DEADLINE_GRACE)?);
        let expires_at_ms = requested_expiration.min(maximum_expiration);
        let process_id = format!("workflow.v1.{namespace_id}.{}", uuid::Uuid::new_v4());
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![
                self.authority_audience.clone(),
                self.invocation_audience.clone(),
            ],
            sub: execution_id.to_owned(),
            namespace_id: namespace_id.to_owned(),
            process_id,
            session_id: uuid::Uuid::new_v4().to_string(),
            process_role: ActorProcessRole::Workflow,
            region: storage_region.to_owned(),
            code_revision: None,
            scope: "actor:invoke".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: expires_at_ms / 1000,
            invocation: None,
        })
    }

    pub(crate) fn issue_host(
        &self,
        namespace_id: &str,
        host_id: &HostId,
        session_id: &str,
        code_revision: &str,
        region: &str,
    ) -> Result<IssuedActorToken> {
        let now_ms = unix_millis()?;
        let expires_at_ms =
            now_ms.saturating_add(duration_millis(self.max_lifetime.min(HOST_TOKEN_TTL))?);
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![
                self.authority_audience.clone(),
                self.invocation_audience.clone(),
            ],
            sub: host_id.as_str().to_owned(),
            namespace_id: namespace_id.to_owned(),
            process_id: host_id.as_str().to_owned(),
            session_id: session_id.to_owned(),
            process_role: ActorProcessRole::Host,
            region: region.to_owned(),
            code_revision: Some(code_revision.to_owned()),
            scope: "actor:authority actor:invoke".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: expires_at_ms / 1000,
            invocation: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn issue_invocation_target(
        &self,
        actor: &ActorKey,
        host_id: &HostId,
        session_id: &str,
        code_revision: &str,
        region: &str,
        owner_epoch: u64,

        workflow_expires_at: i64,
    ) -> Result<IssuedActorToken> {
        actor.validate()?;
        validate_region(region)?;
        ensure!(owner_epoch > 0, "actor owner epoch must be positive");
        let now_ms = unix_millis()?;
        let now = now_ms / 1_000;
        let target_expires_at = now.saturating_add(i64::try_from(INVOCATION_TARGET_TTL.as_secs())?);
        let issuer_expires_at = now.saturating_add(i64::try_from(self.max_lifetime.as_secs())?);
        let expires_at = workflow_expires_at
            .min(target_expires_at)
            .min(issuer_expires_at);
        ensure!(expires_at > now, "workflow credential expires too soon");
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![self.invocation_audience.clone()],
            sub: host_id.as_str().to_owned(),
            namespace_id: actor.namespace_id.clone(),
            process_id: host_id.as_str().to_owned(),
            session_id: session_id.to_owned(),
            process_role: ActorProcessRole::Host,
            region: region.to_owned(),
            code_revision: Some(code_revision.to_owned()),
            scope: "actor:invoke".into(),
            iat: now,
            nbf: now,
            exp: expires_at,
            invocation: Some(ActorInvocationCapability {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch,
            }),
        })
    }

    fn issue(&self, claims: ActorJwtClaims) -> Result<IssuedActorToken> {
        let expires_at_ms = claims
            .exp
            .checked_mul(1_000)
            .context("issued actor token expiration overflow")?;
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.key_id.clone());
        Ok(IssuedActorToken {
            token: encode(&header, &claims, &self.encoding_key).context("sign actor JWT")?,
            expires_at_ms,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorJwtClaims {
    iss: String,
    aud: Vec<String>,
    sub: String,
    namespace_id: String,
    #[serde(rename = "processId")]
    process_id: String,
    session_id: String,
    #[serde(rename = "processRole")]
    process_role: ActorProcessRole,
    #[serde(rename = "storageRegion")]
    region: String,
    code_revision: Option<String>,
    scope: String,
    iat: i64,
    nbf: i64,
    exp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    invocation: Option<ActorInvocationCapability>,
}

fn unix_millis() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported JWT range")
}

fn duration_millis(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).context("duration exceeds supported JWT range")
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
    use base64::{Engine, engine::general_purpose::STANDARD};

    use super::*;
    use crate::control_plane::{ActorJwtVerifier, ActorTokenPurpose};

    #[test]
    fn socket_tickets_bind_actor_metadata_with_short_admission() -> Result<()> {
        let issuer = socket_issuer()?;
        let now = 1_700_000_000_000;
        let grant = || SocketGrant {
            actor: ActorKey {
                namespace_id: "project".into(),
                actor_type: "Room".into(),
                actor_id: "lobby".into(),
            },
            region: "us-east".into(),
            metadata: serde_json::json!({"userId":"alice"}),
            authorization_lifetime_ms: 900_000,
        };
        let token = issuer.issue_socket_at(grant(), now)?;
        let claims = issuer.verify_socket_at(&token, now + 1)?;
        assert_eq!(claims.actor, grant().actor);
        assert_eq!(claims.metadata, grant().metadata);
        assert_eq!(claims.authorized_until_ms, now + 900_000);
        assert!(issuer.verify_socket_at(&token, now + 60_000).is_err());
        assert!(issuer.verify_socket_at(&token, now - 1_000).is_err());
        assert!(socket_issuer()?.verify_socket_at(&token, now).is_err());
        let workflow =
            issuer.issue_workflow("project", "execution", "us-east", unix_millis()? + 30_000)?;
        assert!(issuer.verify_socket(&workflow.token).is_err());
        Ok(())
    }

    fn socket_issuer() -> Result<ActorJwtIssuer> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(86_400),
        )
    }

    #[test]
    fn workflow_tokens_never_outlive_twenty_four_hours() -> Result<()> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "test-key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(172_800),
        )?;
        let before = unix_millis()?;
        let issued =
            issuer.issue_workflow("project", "run", "us-central1", before + 172_800_000)?;
        assert!(issued.expires_at_ms >= before + 86_399_000);
        assert!(issued.expires_at_ms <= unix_millis()? + 86_400_000);
        let short = issuer.issue_workflow("project", "run", "us-central1", before + 60_000)?;
        assert!(short.expires_at_ms <= before + 90_000);
        Ok(())
    }

    #[test]
    fn issued_workflow_tokens_round_trip_through_the_public_key_set() -> Result<()> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "test-key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let issued = issuer.issue_workflow(
            "project-1",
            "execution-1",
            "us-central1-a",
            unix_millis()? + 10_000,
        )?;

        let principal = verifier.authenticate_authorization(&format!("Bearer {}", issued.token))?;

        assert_eq!(principal.scope.namespace_id, "project-1");
        assert_eq!(principal.process_role, ActorProcessRole::Workflow);
        assert_eq!(principal.region, "us-central1-a");
        let jwks: serde_json::Value = serde_json::from_slice(&issuer.jwks_json()?)?;
        assert_eq!(jwks["keys"][0]["kid"], "test-key");
        assert_eq!(jwks["keys"][0]["crv"], "Ed25519");
        Ok(())
    }

    #[test]
    fn direct_invocation_tokens_are_bound_to_one_actor_target_without_host_authority() -> Result<()>
    {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "test-key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let actor = crate::actor::ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        let host_id = HostId::new("host.v2.project-1:revision-1.host-1");
        let issued = issuer.issue_invocation_target(
            &actor,
            &host_id,
            "00000000-0000-4000-8000-000000000001",
            "revision-1",
            "north-america-east",
            3,
            unix_millis()? / 1_000 + 30,
        )?;
        let invocation_verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let principal =
            invocation_verifier.authenticate_authorization(&format!("Bearer {}", issued.token))?;

        assert_eq!(principal.host_id, host_id);
        assert_eq!(
            principal.invocation.expect("invocation capability").actor,
            actor
        );
        assert!(issued.expires_at_ms <= (unix_millis()? + 30_000));

        let authority_verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        assert!(
            authority_verifier
                .authenticate_authorization(&format!("Bearer {}", issued.token))
                .is_err()
        );
        Ok(())
    }
}
