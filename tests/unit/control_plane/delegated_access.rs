use super::*;
use crate::control_plane::auth::unix_seconds;
use serde_json::{Value, json};

#[tokio::test]
async fn delegated_discovery_returns_a_scoped_ticket_but_never_admin_access() -> Result<()> {
    let (service, admin, expiry, verifier) = fixture(Some("admin-secret")).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let router = super::super::super::public_api::router(service, admin);
    let server = tokio::spawn(async { axum::serve(listener, router).await });
    let client = reqwest::Client::new();
    let session_request = json!({"subject":"terse-credential", "expiresAtMs":expiry * 1000,
        "permissions":[{"actorName":"ChatRoom", "actorId":"room", "methods":["sendMessage"]}]});
    let response = client
        .post(format!("{origin}/v1/projects/default/sessions"))
        .bearer_auth("admin-secret")
        .json(&session_request)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let session: Value = response.json().await?;
    assert!(session["expiresAtMs"].as_i64().unwrap() <= expiry * 1000);
    let token = session["token"].as_str().unwrap();
    for credential in ["invalid", token] {
        let response = client
            .post(format!("{origin}/v1/projects/default/sessions"))
            .bearer_auth(credential)
            .json(&session_request)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
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
    assert_eq!(grant.methods, vec!["sendMessage"]);

    for (actor_name, actor_id) in [("ChatRoom", "another-room"), ("PrivateActor", "room")] {
        let response = client
            .post(format!(
                "{origin}/v1/projects/default/actors/{actor_name}/{actor_id}/find-actor"
            ))
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    }

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
    for invalid in [
        json!({"subject":"terse-credential","expiresAtMs":expiry * 1000,"permissions":[]}),
        json!({"subject":"","expiresAtMs":expiry * 1000,"permissions":[{}]}),
        json!({"subject":"terse-credential","expiresAtMs":0,"permissions":[{}]}),
    ] {
        let response = client
            .post(format!("{origin}/v1/projects/default/sessions"))
            .bearer_auth("admin-secret")
            .json(&invalid)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    }
    server.abort();
    Ok(())
}

#[tokio::test]
async fn session_issuance_requires_configured_administrative_authentication() -> Result<()> {
    let (_, admin, _, _) = fixture(None).await?;
    assert!(admin.authorize_session_issuance("").is_err());
    assert!(
        admin
            .authorize_session_issuance("Bearer arbitrary")
            .is_err()
    );
    Ok(())
}

async fn fixture(
    api_key: Option<&str>,
) -> Result<(ControlPlaneService, AdminService, i64, ActorJwtVerifier)> {
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
    let admin = AdminService::new(api_key.map(str::to_owned), registry.clone(), issuer.clone())?;
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
    Ok((service, admin, expiry, verifier))
}
