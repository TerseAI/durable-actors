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
            project_id: "default".into(),
            actor_name: "Room".into(),
            actor_id: "lobby".into(),
        },
        region: "us-east".into(),
        target: None,
        metadata: serde_json::json!({"userId":"alice"}),
        authorization_lifetime_ms: 900_000,
    };
    let (token, connect_by, authorized_until) = issuer.issue_socket_at(grant(), now)?;
    assert_eq!(connect_by, now + 60_000);
    assert_eq!(authorized_until, now + 900_000);
    let claims = issuer.verify_socket_at(&token, now + 1)?;
    assert_eq!(claims.actor, grant().actor);
    assert_eq!(claims.metadata, grant().metadata);
    assert_eq!(claims.authorized_until_ms, now + 900_000);
    assert!(issuer.verify_socket_at(&token, now + 60_000).is_err());
    assert!(issuer.verify_socket_at(&token, now - 1_000).is_err());
    assert!(socket_issuer()?.verify_socket_at(&token, now).is_err());
    let host = issuer.issue_host(
        &HostId::new("host.v3.r1.one"),
        &uuid::Uuid::new_v4().to_string(),
        "r1",
        "us-east",
        &ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
    )?;
    assert!(issuer.verify_socket(&host.token).is_err());
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
fn host_tokens_round_trip_with_a_bounded_lifetime() -> Result<()> {
    let issuer = socket_issuer()?;
    let before = unix_millis()?;
    let host = HostId::new("host.v3.r1.one");
    let issued = issuer.issue_host(
        &host,
        &uuid::Uuid::new_v4().to_string(),
        "r1",
        "us-east",
        &ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
    )?;
    let verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(1800),
    )?;
    let principal = verifier.authenticate_authorization(&format!("Bearer {}", issued.token))?;
    assert_eq!(principal.host_id, host);
    assert_eq!(principal.region, "us-east");
    assert!(issued.expires_at_ms <= before + 1_800_000);
    Ok(())
}

#[test]
fn direct_invocation_tokens_are_bound_to_one_actor_target_without_host_authority() -> Result<()> {
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
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "counter-1".into(),
    };
    let host_id = HostId::new("host.v3.revision-1.host-1");
    let issued = issuer.issue_invocation_target(
        &actor,
        &host_id,
        "00000000-0000-4000-8000-000000000001",
        "revision-1",
        "north-america-east",
        3,
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
    assert!(issued.expires_at_ms <= (unix_millis()? + 60_000));

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

#[test]
fn proxy_grants_are_bound_to_one_actor_session_and_transport() -> Result<()> {
    use crate::regional::proxy::{ProxyConfig, ProxyDestination, ProxyTicket, ProxyVerifier};
    let issuer = socket_issuer()?;
    let config = ProxyConfig {
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        session: uuid::Uuid::new_v4().to_string(),
        region: crate::regional::Region::East,
        keys: issuer.verifier_keys_json()?,
        issuer: "issuer".into(),
    };
    let grant = ProxyTicket::new(
        &config,
        ProxyDestination {
            route: "https://primary.test".into(),
            token: "primary-invocation".into(),
            owner_epoch: 3,
            expires_at_ms: unix_millis()? + 30_000,
            authorized_until_ms: None,
            kind: crate::regional::proxy::ProxyTransport::Invocation,
        },
    )?;
    let token = issuer.issue_proxy(&grant)?;
    let verifier = ProxyVerifier::new(config.clone())?;
    assert!(
        verifier
            .verify(&token, crate::regional::proxy::ProxyTransport::Invocation)
            .is_ok()
    );
    assert!(
        verifier
            .verify(&token, crate::regional::proxy::ProxyTransport::Socket)
            .is_err()
    );
    let mut other = config;
    other.session = uuid::Uuid::new_v4().to_string();
    assert!(
        ProxyVerifier::new(other)?
            .verify(&token, crate::regional::proxy::ProxyTransport::Invocation)
            .is_err()
    );
    let authority = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    assert!(
        authority
            .authenticate_authorization(&format!("Bearer {token}"))
            .is_err()
    );
    Ok(())
}

#[test]
fn proxy_expiry_matches_the_signed_capability_and_is_capped_by_upstream() -> Result<()> {
    use crate::regional::proxy::{ProxyConfig, ProxyDestination, ProxyTicket, ProxyTransport};
    let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
    let config = ProxyConfig {
        actor: crate::actor::ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        session: uuid::Uuid::new_v4().to_string(),
        region: crate::regional::Region::West,
        keys: "{}".into(),
        issuer: "issuer".into(),
    };
    for lifetime in [20_000, 3_600_000] {
        let grant = ProxyTicket::new(
            &config,
            ProxyDestination {
                route: "https://host.test".into(),
                token: "grant".into(),
                owner_epoch: 1,
                expires_at_ms: now + lifetime,
                authorized_until_ms: None,
                kind: ProxyTransport::Invocation,
            },
        )?;
        let json = serde_json::to_value(&grant)?;
        assert_eq!(grant.expires_at_ms(), json["exp"].as_i64().unwrap() * 1000);
        assert!(grant.expires_at_ms() <= now + lifetime);
        assert!(grant.expires_at_ms() <= json["iat"].as_i64().unwrap() * 1000 + 60_000);
    }
    Ok(())
}
