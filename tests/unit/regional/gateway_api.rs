use super::*;
use crate::regional::DirectoryStore;
use std::sync::atomic::{AtomicUsize, Ordering};
use tower::ServiceExt;

struct Directory {
    object: ObjectAssignment,
    reads: AtomicUsize,
}

#[async_trait]
impl DirectoryStore for Directory {
    async fn by_name(&self, actor: &ActorKey) -> Result<Option<ObjectAssignment>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok((actor == &self.object.actor).then(|| self.object.clone()))
    }

    async fn by_id(&self, id: &str) -> Result<Option<ObjectAssignment>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok((id == self.object.object_id).then(|| self.object.clone()))
    }

    async fn create(&self, _: &ObjectAssignment) -> Result<ObjectAssignment> {
        anyhow::bail!("these requests must not create an object")
    }
}

#[tokio::test]
async fn authentication_precedes_directory_access_and_returns_a_json_error() -> Result<()> {
    let (app, directory) = fixture()?;
    let response = app
        .oneshot(
            HttpRequest::builder()
                .uri(format!(
                    "/v1/projects/project/objects/{}",
                    directory.object.object_id
                ))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), 4096).await?;
    let error: Value = serde_json::from_slice(&body)?;
    assert_eq!(error["error"]["code"], "401");
    assert_eq!(directory.reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn global_id_lookup_is_read_only_and_scoped_to_the_project() -> Result<()> {
    let (app, directory) = fixture()?;
    let id = &directory.object.object_id;
    for (project, object_id, expected) in [
        ("project", id.clone(), StatusCode::OK),
        ("other", id.clone(), StatusCode::NOT_FOUND),
        (
            "project",
            uuid::Uuid::new_v4().to_string(),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/v1/projects/{project}/objects/{object_id}"))
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), expected);
    }
    assert_eq!(directory.reads.load(Ordering::SeqCst), 3);
    Ok(())
}

fn fixture() -> Result<(Router, Arc<Directory>)> {
    let directory = Arc::new(Directory {
        object: ObjectAssignment {
            object_id: uuid::Uuid::new_v4().to_string(),
            actor: ActorKey {
                project_id: "project".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
            },
            home_region: Region::West,
            first_ingress_region: Region::West,
        },
        reads: AtomicUsize::new(0),
    });
    let endpoints = Arc::new(HttpRegionalEndpoints::new(
        Region::ALL
            .into_iter()
            .map(|region| (region.as_str().into(), "http://127.0.0.1:1".into()))
            .collect(),
        "secret".into(),
    )?);
    let gateway = Arc::new(Gateway::new(
        Arc::new(ActorDirectory::new(directory.clone())),
        Region::East,
        endpoints.clone(),
    ));
    Ok((
        router(GatewayApi {
            gateway,
            endpoints,
            secret: "secret".into(),
        }),
        directory,
    ))
}

#[test]
fn gateway_requires_distinct_internal_and_client_credentials() -> Result<()> {
    let mut config = GatewayConfig {
        bind: "127.0.0.1:0".parse()?,
        ingress: Region::East,
        control_plane_urls: Default::default(),
        secret: "client".into(),
        control_plane_secret: "client".into(),
        cloud_run_auth: false,
    };
    assert!(config.validate_credentials().is_err());
    config.control_plane_secret = "internal".into();
    config.validate_credentials()?;
    config.control_plane_secret.clear();
    assert!(config.validate_credentials().is_err());
    Ok(())
}

#[tokio::test]
async fn customer_credentials_cannot_forward_unlisted_internal_routes() -> Result<()> {
    let (app, directory) = fixture()?;
    for path in [
        "/v1/projects/project/release",
        "/v1/projects/project/internal/hosts",
        "/v1/projects/project/actors/Counter/one/find-proxy",
        "/durableactors.ActorControlPlaneService/Execute",
    ] {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    assert_eq!(directory.reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn gateway_serves_the_public_api_contract() -> Result<()> {
    let (app, _) = fixture()?;
    let response = app
        .oneshot(
            HttpRequest::builder()
                .uri("/openapi.yaml")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 256 * 1024).await?;
    let spec = std::str::from_utf8(&body)?;
    assert!(spec.contains("operationId: findActor"));
    assert!(spec.contains("operationId: resolveActorIdentity"));
    assert!(!spec.contains("find-proxy"));
    assert!(!spec.contains("ActorControlPlaneService"));
    Ok(())
}

#[test]
fn gateway_requires_its_own_regions_control_plane() -> Result<()> {
    let mut config = GatewayConfig {
        bind: "127.0.0.1:0".parse()?,
        ingress: Region::East,
        control_plane_urls: [("us-west".into(), "http://127.0.0.1:1".into())].into(),
        secret: "client".into(),
        control_plane_secret: "internal".into(),
        cloud_run_auth: false,
    };
    assert!(config.validate().is_err());
    config.ingress = Region::West;
    config.validate()?;
    config.control_plane_urls = [("north-america-west".into(), "http://127.0.0.1:1".into())].into();
    config.validate()?;
    Ok(())
}

#[tokio::test]
async fn gateway_forwards_only_supported_admin_methods_with_internal_credentials() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let upstream = Router::new().fallback(|request: HttpRequest| async move {
        assert_eq!(request.headers()["authorization"], "Bearer internal");
        (StatusCode::ACCEPTED, request.uri().to_string())
    });
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await });
    let (_, directory) = fixture()?;
    let endpoints = Arc::new(HttpRegionalEndpoints::new(
        Region::ALL
            .into_iter()
            .map(|region| (region.as_str().into(), origin.clone()))
            .collect(),
        "internal".into(),
    )?);
    let app = router(GatewayApi {
        gateway: Arc::new(Gateway::new(
            Arc::new(ActorDirectory::new(directory)),
            Region::East,
            endpoints.clone(),
        )),
        endpoints,
        secret: "customer".into(),
    });
    for (method, path, status) in [
        (
            "GET",
            "/v1/projects/project/deployment",
            StatusCode::ACCEPTED,
        ),
        (
            "PUT",
            "/v1/projects/project/deployment",
            StatusCode::ACCEPTED,
        ),
        (
            "DELETE",
            "/v1/projects/project/deployment",
            StatusCode::ACCEPTED,
        ),
        (
            "GET",
            "/v1/projects/project/deployment/contract",
            StatusCode::ACCEPTED,
        ),
        (
            "GET",
            "/v1/projects/project/observe/events?limit=5",
            StatusCode::ACCEPTED,
        ),
        (
            "POST",
            "/v1/projects/project/observe/events",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            "POST",
            "/v1/projects/project/deployment",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", "Bearer customer")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), status, "{method} {path}");
        if status == StatusCode::ACCEPTED {
            assert_eq!(
                axum::body::to_bytes(response.into_body(), 4096)
                    .await?
                    .as_ref(),
                path.as_bytes()
            );
        }
    }
    server.abort();
    Ok(())
}

struct HostEndpoints {
    host: String,
    resolutions: AtomicUsize,
}

#[async_trait]
impl RegionalEndpoints for HostEndpoints {
    async fn actor_operation(
        &self,
        _: Region,
        _: &ActorKey,
        operation: &str,
        _: Value,
    ) -> Result<Value> {
        assert_eq!(operation, "find-actor");
        let resolution = self.resolutions.fetch_add(1, Ordering::SeqCst) + 1;
        let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)?;
        Ok(serde_json::json!({
            "route": self.host, "token": format!("ticket-{resolution}"),
            "ownerEpoch": 7, "expiresAtMs": now + 60_000
        }))
    }
}

async fn fake_host(post_status: StatusCode) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let invoke = axum::routing::head(|headers: axum::http::HeaderMap| async move {
        if headers["authorization"] == "Bearer ticket-1" {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::NO_CONTENT
        }
    })
    .post(
        move |headers: axum::http::HeaderMap, body: axum::Json<Value>| async move {
            assert_eq!(headers["authorization"], "Bearer ticket-2");
            assert_eq!(headers["x-durable-actors-timing"], "1");
            assert_eq!(body["ownerEpoch"], 7);
            assert_eq!(body["method"], "increment");
            if post_status != StatusCode::OK {
                return (post_status, axum::Json(Value::Null));
            }
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({"type": "completed", "result": body["args"][0]})),
            )
        },
    );
    let app = Router::new().route("/v1/projects/{project}/actors/{name}/{id}/invoke", invoke);
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(origin)
}

fn invoke_fixture(host: String) -> Result<(Router, Arc<HostEndpoints>)> {
    let (_, directory) = fixture()?;
    let endpoints = Arc::new(HostEndpoints {
        host,
        resolutions: AtomicUsize::new(0),
    });
    let gateway = Arc::new(Gateway::new(
        Arc::new(ActorDirectory::new(directory)),
        Region::West,
        endpoints.clone(),
    ));
    let internal = Arc::new(HttpRegionalEndpoints::new(
        [("us-west".into(), "http://127.0.0.1:1".into())].into(),
        "internal".into(),
    )?);
    Ok((
        router(GatewayApi {
            gateway,
            endpoints: internal,
            secret: "secret".into(),
        }),
        endpoints,
    ))
}

async fn invoke(app: Router) -> Result<(StatusCode, Value)> {
    let response = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/v1/projects/project/actors/Counter/one/invoke")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"requestId":"request-1","method":"increment","args":[5]}"#,
                ))?,
        )
        .await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 65_536).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

#[tokio::test]
async fn gateway_invokes_in_one_request_and_refreshes_a_rejected_ticket_once() -> Result<()> {
    let (app, endpoints) = invoke_fixture(fake_host(StatusCode::OK).await?)?;
    let (status, reply) = invoke(app).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply["type"], "completed");
    assert_eq!(reply["result"], 5);
    assert_eq!(reply["target"]["token"], "ticket-2");
    assert_eq!(reply["target"]["ownerEpoch"], 7);
    assert!(
        reply["target"]["route"]
            .as_str()
            .unwrap()
            .starts_with("http://127.0.0.1:")
    );
    assert!(reply["target"]["expiresAtMs"].as_u64().unwrap() > 0);
    assert_eq!(endpoints.resolutions.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn gateway_never_retries_an_invocation_that_may_have_executed() -> Result<()> {
    let (app, endpoints) = invoke_fixture(fake_host(StatusCode::INTERNAL_SERVER_ERROR).await?)?;
    let (status, reply) = invoke(app).await?;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(reply["error"]["code"], "outcome_unknown");
    assert_eq!(endpoints.resolutions.load(Ordering::SeqCst), 2);
    Ok(())
}
