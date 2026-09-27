use super::*;
use crate::control_plane::{auth::unix_seconds, project_grant::ProjectGrantVerifier};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};

#[tokio::test]
async fn delegated_discovery_returns_a_scoped_ticket_but_never_admin_access() -> Result<()> {
    let (service, admin, token, expiry, verifier) = fixture().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let router = super::super::super::public_api::router(service, admin);
    let server = tokio::spawn(async { axum::serve(listener, router).await });
    let client = reqwest::Client::new();
    let reply: Value = client
        .post(format!(
            "{origin}/v1/projects/default/actors/ChatRoom/room/find-actor"
        ))
        .bearer_auth(&token)
        .json(&json!({}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(reply["expiresAtMs"].as_i64().unwrap() <= expiry * 1000);
    let principal = verifier
        .authenticate_authorization(&format!("Bearer {}", reply["token"].as_str().unwrap()))?;
    let capability = principal.invocation.unwrap();
    assert_eq!(capability.actor.project_id, "default");
    assert_eq!(capability.actor.actor_id, "room");
    let grant = capability.grant.unwrap();
    assert_eq!(grant.subject, "terse-credential");
    assert_eq!(grant.expires_at, expiry);
    assert!(grant.methods.iter().any(|method| method == "sendMessage"));
    assert!(!grant.methods.iter().any(|method| method == "onConnect"));

    for (method, path) in [
        (
            reqwest::Method::POST,
            "/v1/projects/other/actors/ChatRoom/room/find-actor",
        ),
        (reqwest::Method::GET, "/v1/projects/default/deployment"),
        (
            reqwest::Method::GET,
            "/v1/projects/default/deployment/contract",
        ),
        (reqwest::Method::PUT, "/v1/projects/default/deployment"),
        (reqwest::Method::DELETE, "/v1/projects/default/deployment"),
        (
            reqwest::Method::POST,
            "/v1/projects/default/actors/ChatRoom/room/find-websocket",
        ),
    ] {
        assert_eq!(
            client
                .request(method, format!("{origin}{path}"))
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path}"
        );
    }
    let missing = client
        .post(format!(
            "{origin}/v1/projects/default/actors/PrivateActor/room/find-actor"
        ))
        .bearer_auth(&token)
        .json(&json!({}))
        .send()
        .await?;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    server.abort();
    Ok(())
}

async fn fixture() -> Result<(
    ControlPlaneService,
    AdminService,
    String,
    i64,
    ActorJwtVerifier,
)> {
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
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(
        Some("admin-secret".into()),
        registry.clone(),
        issuer.clone(),
    )?
    .with_project_grants(Some(ProjectGrantVerifier::new(
        &issuer.verifier_keys_json()?,
        "terse",
        "actor-discovery",
    )?))?;
    let spec = HostLaunchSpec {
        project_id: "default".into(),
        source: None,
        image_ref: "im-runtime".into(),
        code_snapshot: None,
        working_directory: "/customer".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
    };
    let contract =
        crate::control_plane::contracts::PublicActorContract::new(serde_json::from_str(
            include_str!("../../../sdk/tests/fixtures/public-contract.json"),
        )?)?;
    admin.register_deployment(&spec, Some(&contract)).await?;
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let now = unix_seconds()?;
    let expiry = now + 25;
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("test-key".into());
    let token = encode(
        &header,
        &json!({"iss":"terse","aud":"actor-discovery","sub":"terse-credential","jti":"grant-1","projectId":"default","scope":"actor:resolve","iat":now,"nbf":now,"exp":expiry}),
        &EncodingKey::from_ed_der(pkcs8.as_ref()),
    )?;
    Ok((service, admin, token, expiry, verifier))
}
