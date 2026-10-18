use super::*;
use crate::host::HostId;

#[tokio::test]
async fn assignment_is_authenticated_single_use_and_waits_for_readiness() -> anyhow::Result<()> {
    let (authorization, token) = authorization()?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router(authorization, send, std::env::temp_dir()))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await?;
    assert_eq!(response.status(), 401);
    let request = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({"actor": "one"}));
    let call = tokio::spawn(async move { request.send().await });
    let assigned = tokio::time::timeout(std::time::Duration::from_secs(1), receive).await??;
    assert_eq!(assigned.environment["actor"], "one");
    assert!(
        !call.is_finished(),
        "assignment must wait for hydration and ownership"
    );
    let duplicate = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({}))
        .send()
        .await?;
    assert_eq!(duplicate.status(), 409);
    assert!(
        assigned
            .ready
            .send(super::super::process::HostReadiness {
                host_id: HostId::new("host.v3.test.one"),
                session_id: "session".into(),
                route: "https://host.test".into(),
                canonical_region: "north-america-east".into(),
                owner_epoch: 42,
                lease: crate::host_leases::HostLease {
                    id: HostId::new("host.v3.test.one"),
                    session_id: "session".into(),
                    route: "https://host.test".into(),
                    expires_at_ms: 60_000,
                },
            })
            .is_ok()
    );
    let reply = call.await??;
    assert_eq!(reply.status(), 200);
    assert_eq!(reply.json::<serde_json::Value>().await?["ownerEpoch"], 42);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn failed_initialization_never_reports_ready() -> anyhow::Result<()> {
    let (authorization, token) = authorization()?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router(authorization, send, std::env::temp_dir()))
            .await
            .unwrap();
    });
    let request = reqwest::Client::new()
        .post(url)
        .bearer_auth(&token)
        .json(&serde_json::json!({}));
    let call = tokio::spawn(async move { request.send().await });
    drop(receive.await?);
    assert_eq!(call.await??.status(), 503);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn prepared_code_is_published_after_verification() -> anyhow::Result<()> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let root = tempfile::tempdir()?;
    let directory = root.path().to_owned();
    let (authorization, token) = authorization()?;
    let (send, _receive) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router(authorization, send, directory)).await
    }));
    let body = "export const Counter = 1;";
    let artifact = crate::bundle::BundleFile {
        path: "actors.mjs".into(),
        object: "code/actors.mjs".into(),
        generation: 1,
        sha256: URL_SAFE_NO_PAD.encode(
            aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, body.as_bytes()).as_ref(),
        ),
    };
    let status = reqwest::Client::new()
        .post(format!("{url}/prepare-code"))
        .bearer_auth(&token)
        .header(
            "terse-code-artifact",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&artifact)?),
        )
        .body(body)
        .send()
        .await?
        .status();
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        tokio::fs::read_to_string(root.path().join("actors.mjs")).await?,
        body
    );
    drop(server);
    Ok(())
}

fn authorization() -> anyhow::Result<(crate::control_plane::assignment::AssignmentVerifier, String)>
{
    use base64::Engine;
    let key = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(
        &aws_lc_rs::rand::SystemRandom::new(),
    )?;
    let issuer = crate::control_plane::ActorJwtIssuer::from_base64_pkcs8(
        &base64::engine::general_purpose::STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invocation",
        std::time::Duration::from_secs(1800),
    )?;
    let verifier = crate::control_plane::assignment::AssignmentVerifier::new(
        &issuer.verifier_keys_json()?,
        "issuer",
        || Ok("sandbox".into()),
    )?;
    Ok((verifier, issuer.issue_assignment("sandbox")?))
}
