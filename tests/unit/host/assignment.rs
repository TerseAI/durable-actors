use super::*;
use crate::host::HostId;

#[tokio::test]
async fn assignment_is_authenticated_single_use_and_waits_for_readiness() -> anyhow::Result<()> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(String::from("secret").into(), send, std::env::temp_dir()),
        )
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
        .bearer_auth("secret")
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
        .bearer_auth("secret")
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
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(String::from("secret").into(), send, std::env::temp_dir()),
        )
        .await
        .unwrap();
    });
    let request = reqwest::Client::new()
        .post(url)
        .bearer_auth("secret")
        .json(&serde_json::json!({}));
    let call = tokio::spawn(async move { request.send().await });
    drop(receive.await?);
    assert_eq!(call.await??.status(), 503);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn code_preparation_requires_authentication() -> anyhow::Result<()> {
    let (send, _receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/prepare-code", listener.local_addr()?);
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            listener,
            router(String::from("secret").into(), send, std::env::temp_dir()),
        )
        .await
    }));
    let response = reqwest::Client::new()
        .post(url)
        .body("customer code")
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    drop(server);
    Ok(())
}

#[tokio::test]
async fn prepared_code_is_verified_and_cannot_change_after_assignment() -> anyhow::Result<()> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let root = tempfile::tempdir()?;
    let directory = root.path().to_owned();
    let (send, receive) = oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            listener,
            router(String::from("secret").into(), send, directory),
        )
        .await
    }));
    let body = "export const Counter = 1;";
    let mut artifact = crate::artifacts::ArtifactFile {
        path: "actors.mjs".into(),
        object: "code/actors.mjs".into(),
        generation: 1,
        sha256: URL_SAFE_NO_PAD.encode(
            aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, body.as_bytes()).as_ref(),
        ),
    };
    let client = reqwest::Client::new();
    let upload = |artifact: &crate::artifacts::ArtifactFile, body: &'static str| {
        client
            .post(format!("{url}/prepare-code"))
            .bearer_auth("secret")
            .header(
                "terse-code-artifact",
                URL_SAFE_NO_PAD.encode(serde_json::to_vec(artifact).unwrap()),
            )
            .body(body)
    };
    assert_eq!(
        upload(&artifact, body).send().await?.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        tokio::fs::read_to_string(root.path().join("actors.mjs")).await?,
        body
    );
    assert_eq!(
        upload(&artifact, "corrupt").send().await?.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        tokio::fs::read_to_string(root.path().join("actors.mjs")).await?,
        body
    );
    artifact.path = "../escape.mjs".into();
    assert_eq!(
        upload(&artifact, body).send().await?.status(),
        StatusCode::BAD_REQUEST
    );
    artifact.path = "actors.mjs".into();
    let assign = client
        .post(format!("{url}/assign"))
        .bearer_auth("secret")
        .json(&serde_json::json!({}));
    let call = tokio::spawn(async move { assign.send().await });
    let assigned = receive.await?;
    assert_eq!(
        upload(&artifact, body).send().await?.status(),
        StatusCode::CONFLICT
    );
    drop(assigned);
    assert_eq!(call.await??.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(server);
    Ok(())
}
