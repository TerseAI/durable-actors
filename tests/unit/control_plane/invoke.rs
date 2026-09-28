use super::*;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn combined_invocation_resolves_and_dispatches_with_a_scoped_host_token() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":null}),
    )])
    .await?;
    let response = fixture.call("api-key", json!({"requestId":"call-1", "method":"sendMessage", "args":["hello"], "homeRegion":"north-america-east"})).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let reply: Value = response.json().await?;
    assert_eq!(reply["outcome"]["type"], "completed");
    assert!(reply["outcome"]["result"].is_null());
    assert_eq!(reply["target"]["ownerEpoch"], 1);
    assert!(reply["target"]["expiresAtMs"].as_i64().unwrap() > 0);
    assert!(
        reply["target"]["route"]
            .as_str()
            .unwrap()
            .starts_with("http://127.0.0.1:")
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    let requests = fixture.host.requests.lock().unwrap();
    assert_eq!(
        requests[0].1,
        json!({"requestId":"call-1", "ownerEpoch":1, "method":"sendMessage", "args":["hello"]})
    );
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    assert_eq!(principal.actor.actor_id, "one");
    assert_eq!(
        requests[0].0,
        format!("Bearer {}", reply["target"]["token"].as_str().unwrap())
    );
    Ok(())
}

#[tokio::test]
async fn combined_invocation_authenticates_and_validates_requests() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":7}),
    )])
    .await?;
    let request = json!({"requestId":"call-1", "method":"sendMessage", "args":[]});
    assert_eq!(
        fixture.call("invalid", request.clone()).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    for body in [
        json!({"requestId":"", "method":"sendMessage", "args":[]}),
        json!({"requestId":"r", "method":"sendMessage", "args":{}}),
        json!({"requestId":"r", "method":"sendMessage", "args":[], "homeRegion":false}),
    ] {
        assert_eq!(
            fixture.call("api-key", body).await?.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    let expiry = (super::super::super::auth::unix_seconds()? + 30) * 1000;
    let session = fixture
        .admin
        .issue_session("default".into(), "subject".into(), expiry)?;
    let response = fixture.call(&session.token, request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let requests = fixture.host.requests.lock().unwrap();
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    let grant = principal.invocation.unwrap().grant.unwrap();
    assert_eq!(grant.subject, "subject");
    Ok(())
}

#[tokio::test]
async fn combined_invocation_returns_pre_dispatch_rejections_for_the_client_retry_budget()
-> Result<()> {
    for reason in ["stale_owner", "host_unavailable", "upstream_not_reached"] {
        let outcome = json!({"type":"not_executed", "reason":reason});
        let fixture = Fixture::start(vec![(StatusCode::OK, outcome.clone())]).await?;
        let reply: Value = fixture
            .call(
                "api-key",
                json!({"requestId":"same-id", "method":"clear", "args":[]}),
            )
            .await?
            .json()
            .await?;
        assert_eq!(reply["outcome"], outcome);
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    }
    let fixture = Fixture::start(vec![(StatusCode::UNAUTHORIZED, json!({}))]).await?;
    let reply: Value = fixture
        .call(
            "api-key",
            json!({"requestId":"same-id", "method":"clear", "args":[]}),
        )
        .await?
        .json()
        .await?;
    assert_eq!(reply["outcome"], json!({"type":"unauthenticated"}));
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);

    let mut fixture = Fixture::start(vec![(StatusCode::OK, json!({}))]).await?;
    let host = fixture.servers.pop().unwrap();
    host.abort();
    assert!(host.await.unwrap_err().is_cancelled());
    let reply: Value = fixture
        .call(
            "api-key",
            json!({"requestId":"refused", "method":"clear", "args":[]}),
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        reply["outcome"],
        json!({"type":"not_executed", "reason":"upstream_not_reached"})
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn combined_invocation_does_not_replay_failed_or_ambiguous_dispatches() -> Result<()> {
    for (status, body, code) in [
        (
            StatusCode::OK,
            json!({"type":"failed", "code":"actor_error", "message":"boom"}),
            "actor_error",
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"unknown"}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"completed"}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"failed", "code":"", "message":"boom"}),
            "outcome_unknown",
        ),
    ] {
        let fixture = Fixture::start(vec![(status, body)]).await?;
        let reply: Value = fixture
            .call(
                "api-key",
                json!({"requestId":"r", "method":"clear", "args":[]}),
            )
            .await?
            .json()
            .await?;
        assert_eq!(
            reply["outcome"]
                .get("code")
                .unwrap_or(&reply["error"]["code"]),
            code
        );
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn cold_invocation_returns_assignment_outcome_without_dispatching_again() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let response = fixture
        .call(
            "api-key",
            json!({"requestId":"first-write", "method":"clear", "args":[]}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let reply: Value = response.json().await?;
    assert_eq!(reply["outcome"], json!({"type":"completed", "result":7}));
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn lost_assignment_response_does_not_redispatch_to_the_now_active_owner() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let response = fixture
        .call(
            "api-key",
            json!({"requestId":"lost-response", "method":"clear", "args":[]}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        response.json::<Value>().await?["error"]["code"],
        "outcome_unknown"
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

struct Fixture {
    origin: String,
    admin: AdminService,
    verifier: ActorJwtVerifier,
    host: Arc<HostFixture>,
    servers: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Fixture {
    async fn start(replies: Vec<(StatusCode, Value)>) -> Result<Self> {
        let cold = replies.is_empty();
        let host = Arc::new(HostFixture {
            replies: Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
        });
        let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let host_origin = format!("http://{}", host_listener.local_addr()?);
        let routes = Router::new()
            .route(
                "/v1/projects/default/actors/ChatRoom/one/invoke",
                post(host_invoke),
            )
            .with_state(host.clone());
        let host_server = tokio::spawn(async move { axum::serve(host_listener, routes).await });
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
        let contract =
            super::super::super::contracts::PublicActorContract::new(serde_json::from_str(
                include_str!("../../../sdk/tests/fixtures/public-contract.json"),
            )?)?;
        admin
            .register_deployment(
                &HostLaunchSpec {
                    project_id: "default".into(),
                    source: None,
                    image_ref: "image".into(),
                    code_snapshot: None,
                    working_directory: "/app".into(),
                    actor_entrypoint: None,
                    secret_refs: vec![],
                },
                Some(&contract),
            )
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        if !cold {
            let spec = registry.launch_spec("default").await?.unwrap();
            let lease = HostLease {
                route: host_origin.clone(),
                ..test_lease(&HostId::new(format!(
                    "host.v3.{}.fixture",
                    spec.host_config_key()
                )))
            };
            placements.set_owner(
                &ActorKey {
                    project_id: "default".into(),
                    actor_name: "ChatRoom".into(),
                    actor_id: "one".into(),
                }
                .storage_key(),
                lease,
                "north-america-east",
            )?;
        }
        let service = ControlPlaneService::new(
            placements.clone(),
            auth,
            registry,
            issuer,
            Arc::new(InvocationProvisioner {
                route: host_origin,
                placements: placements.clone(),
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let routes = super::super::super::public_api::router(service, admin.clone());
        let server = tokio::spawn(async move { axum::serve(listener, routes).await });
        Ok(Self {
            origin,
            admin,
            verifier,
            host,
            servers: vec![server, host_server],
        })
    }

    async fn call(&self, token: &str, body: Value) -> Result<reqwest::Response> {
        Ok(reqwest::Client::new()
            .post(format!(
                "{}/v1/projects/default/actors/ChatRoom/one/invoke",
                self.origin
            ))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

struct HostFixture {
    replies: Mutex<VecDeque<(StatusCode, Value)>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<(String, Value)>>,
}

async fn host_invoke(
    State(state): State<Arc<HostFixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    state
        .requests
        .lock()
        .unwrap()
        .push((headers["authorization"].to_str().unwrap().into(), body));
    let (status, reply) = state
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected replay");
    (status, Json(reply)).into_response()
}

struct InvocationProvisioner {
    route: String,
    placements: Arc<LocalObjectPlacementStore>,
}
#[async_trait]
impl HostProvisioner for InvocationProvisioner {
    fn host_idle_timeout_ms(&self) -> u64 {
        10_000
    }

    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _: Option<&HostLaunchSpec>,
        _: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::super::super::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }
    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        _: &str,
        _: &ActorKey,
        _: bool,
        initial: Option<&crate::sandbox::InitialInvocation>,
    ) -> Result<(HostLease, u64, Option<serde_json::Value>)> {
        if let Some(initial) =
            initial.filter(|initial| initial.invocation.request_id == "lost-response")
        {
            self.placements.set_owner(
                &initial.invocation.actor.storage_key(),
                HostLease {
                    route: self.route.clone(),
                    ..test_lease(&HostId::new(format!(
                        "host.v3.{}.fixture",
                        spec.host_config_key()
                    )))
                },
                "north-america-east",
            )?;
            return Err(
                anyhow::anyhow!("assignment response lost").context(InitialInvocationUnknown)
            );
        }
        Ok((
            HostLease {
                route: self.route.clone(),
                ..test_lease(&HostId::new(format!(
                    "host.v3.{}.fixture",
                    spec.host_config_key()
                )))
            },
            1,
            initial.map(|initial| {
                assert_eq!(initial.invocation.request_id, "first-write");
                assert_eq!(initial.invocation.method, "clear");
                assert!(initial.invocation.args.is_empty());
                json!({"type":"completed", "result":7})
            }),
        ))
    }
    async fn terminate_hosts(&self, _: &HostLaunchSpec, _: &[String]) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}
