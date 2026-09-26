use super::*;
use crate::regional::DirectoryStore;
use std::sync::Mutex;

struct Directory(ObjectAssignment);
#[async_trait]
impl DirectoryStore for Directory {
    async fn by_name(&self, actor: &ActorKey) -> Result<Option<ObjectAssignment>> {
        Ok((actor == &self.0.actor).then(|| self.0.clone()))
    }
    async fn by_id(&self, id: &str) -> Result<Option<ObjectAssignment>> {
        Ok((id == self.0.object_id).then(|| self.0.clone()))
    }
    async fn create(&self, _: &ObjectAssignment) -> Result<ObjectAssignment> {
        panic!("an existing object must remain read-only")
    }
}

#[derive(Default)]
struct Endpoints {
    calls: Mutex<Vec<(Region, String)>>,
    proxy_requests: Mutex<Vec<Value>>,
}
#[async_trait]
impl RegionalEndpoints for Endpoints {
    async fn actor_operation(
        &self,
        region: Region,
        _: &ActorKey,
        operation: &str,
        body: Value,
    ) -> Result<Value> {
        self.calls.lock().unwrap().push((region, operation.into()));
        Ok(match operation {
            "find-actor" => {
                json!({"route":"https://primary.test","token":"primary","ownerEpoch":7,"expiresAtMs":1000})
            }
            "find-websocket" => json!({
                "websocketUrl": "wss://primary.test/v1/socket?key=primary-socket",
                "connectByMs": 1000, "authorizedUntilMs": 2000,
            }),
            "find-proxy" => {
                self.proxy_requests.lock().unwrap().push(body);
                json!({"route":"https://proxy.test","token":"proxy","ownerEpoch":7,"expiresAtMs":1000})
            }
            _ => anyhow::bail!("unexpected request"),
        })
    }
}

struct SlowProxy(Endpoints);

#[async_trait]
impl RegionalEndpoints for SlowProxy {
    async fn actor_operation(
        &self,
        region: Region,
        actor: &ActorKey,
        operation: &str,
        body: Value,
    ) -> Result<Value> {
        let result = self
            .0
            .actor_operation(region, actor, operation, body)
            .await?;
        if operation == "find-proxy" && self.0.calls.lock().unwrap().len() == 2 {
            let response = axum::http::Response::builder().status(409).body("")?;
            return Err(reqwest::Response::from(response)
                .error_for_status()
                .unwrap_err()
                .into());
        }
        Ok(result)
    }
}

#[tokio::test]
async fn a_proxy_cold_start_can_refresh_an_expired_grant_without_invoking_the_actor() -> Result<()>
{
    let object = ObjectAssignment {
        object_id: uuid::Uuid::new_v4().to_string(),
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        home_region: Region::West,
        first_ingress_region: Region::West,
    };
    let endpoints = Arc::new(SlowProxy(Endpoints::default()));
    let gateway = Gateway::new(
        Arc::new(ActorDirectory::new(Arc::new(Directory(object.clone())))),
        Region::East,
        endpoints.clone(),
    );
    let result = gateway.resolve(&object.actor, None).await?;
    assert_eq!(result["route"], "https://proxy.test");
    assert_eq!(
        *endpoints.0.calls.lock().unwrap(),
        [
            (Region::West, "find-actor".into()),
            (Region::East, "find-proxy".into()),
            (Region::West, "find-actor".into()),
            (Region::East, "find-proxy".into()),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn remote_ingress_activates_the_home_and_only_provisions_a_local_proxy() -> Result<()> {
    let assignment = ObjectAssignment {
        object_id: uuid::Uuid::new_v4().to_string(),
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        home_region: Region::West,
        first_ingress_region: Region::West,
    };
    for ingress in Region::ALL {
        let endpoints = Arc::new(Endpoints::default());
        let directory = Arc::new(ActorDirectory::new(Arc::new(Directory(assignment.clone()))));
        let gateway = Gateway::new(directory, ingress, endpoints.clone());
        let result = gateway.resolve(&assignment.actor, None).await?;
        assert_eq!(result["objectId"], assignment.object_id);
        assert_eq!(result["homeRegion"], "north-america-west");
        let calls = endpoints.calls.lock().unwrap();
        assert_eq!(calls[0], (Region::West, "find-actor".into()));
        if ingress == Region::West {
            assert_eq!(calls.len(), 1);
        } else {
            assert_eq!(calls[1], (ingress, "find-proxy".into()));
            assert_eq!(result["route"], "https://proxy.test");
            let requests = endpoints.proxy_requests.lock().unwrap();
            assert_eq!(requests[0]["objectId"], assignment.object_id);
            assert_eq!(requests[0]["homeRegion"], "north-america-west");
            assert_eq!(requests[0]["destination"]["route"], "https://primary.test");
            assert_eq!(requests[0]["destination"]["kind"], "invocation");
        }
    }
    Ok(())
}

#[tokio::test]
async fn socket_proxy_receives_the_assignment_and_primary_grant() -> Result<()> {
    let assignment = ObjectAssignment {
        object_id: uuid::Uuid::new_v4().to_string(),
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        home_region: Region::West,
        first_ingress_region: Region::West,
    };
    let endpoints = Arc::new(Endpoints::default());
    let gateway = Gateway::new(
        Arc::new(ActorDirectory::new(Arc::new(Directory(assignment.clone())))),
        Region::East,
        endpoints.clone(),
    );
    let result = gateway
        .resolve(&assignment.actor, Some(json!({"metadata": {}})))
        .await?;
    let requests = endpoints.proxy_requests.lock().unwrap();
    let request: crate::regional::proxy::ProxyRequest =
        serde_json::from_value(requests[0].clone())?;
    assert_eq!(request.object_id, assignment.object_id);
    assert_eq!(request.home_region, Region::West);
    assert_eq!(request.destination.route, "https://primary.test");
    assert_eq!(request.destination.token, "primary-socket");
    assert_eq!(
        request.destination.kind,
        crate::regional::proxy::ProxyTransport::Socket
    );
    assert_eq!(request.destination.authorized_until_ms, Some(2000));
    assert_eq!(result["authorizedUntilMs"], 2000);
    Ok(())
}

#[tokio::test]
async fn endpoints_accept_a_subset_and_reject_unconfigured_regions() -> Result<()> {
    let endpoints = HttpRegionalEndpoints::new(
        [(
            "north-america-west".to_owned(),
            "http://127.0.0.1:1".to_owned(),
        )]
        .into(),
        "secret".into(),
    )?;
    let error = endpoints
        .request(
            Region::East,
            reqwest::Method::GET,
            "/v1/projects/project/deployment",
            Default::default(),
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("no control plane"));
    assert!(HttpRegionalEndpoints::new(HashMap::new(), "secret".into()).is_err());
    Ok(())
}

#[derive(Debug)]
struct ServiceToken;
impl google_cloud_auth::credentials::idtoken::IDTokenCredentialsProvider for ServiceToken {
    async fn id_token(
        &self,
    ) -> std::result::Result<String, google_cloud_auth::errors::CredentialsError> {
        Ok("google-id-token".into())
    }
}
#[tokio::test]
async fn regional_http_sends_service_identity_alongside_internal_authorization() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let app = axum::Router::new().route(
        "/v1/projects/test/deployment",
        axum::routing::get(|headers: axum::http::HeaderMap| async move {
            assert_eq!(headers["authorization"], "Bearer internal-secret");
            assert_eq!(
                headers["x-serverless-authorization"],
                "Bearer google-id-token"
            );
            axum::http::StatusCode::NO_CONTENT
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut endpoints = HttpRegionalEndpoints::new(
        Region::ALL
            .into_iter()
            .map(|r| (r.as_str().into(), origin.clone()))
            .collect(),
        "internal-secret".into(),
    )?;
    endpoints
        .identities
        .insert(Region::West, ServiceToken.into());
    let result = endpoints
        .request(
            Region::West,
            reqwest::Method::GET,
            "/v1/projects/test/deployment",
            bytes::Bytes::new(),
        )
        .await;
    task.abort();
    assert_eq!(result?.status(), reqwest::StatusCode::NO_CONTENT);
    Ok(())
}

#[derive(Debug)]
struct RejectedIdentity;
impl google_cloud_auth::credentials::idtoken::IDTokenCredentialsProvider for RejectedIdentity {
    async fn id_token(
        &self,
    ) -> std::result::Result<String, google_cloud_auth::errors::CredentialsError> {
        Err(google_cloud_auth::errors::CredentialsError::from_source(
            false,
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "identity rejected"),
        ))
    }
}
#[tokio::test]
async fn failed_service_identity_does_not_send_an_unauthenticated_request() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let mut endpoints = HttpRegionalEndpoints::new(
        [(Region::West.as_str().into(), origin)].into(),
        "internal".into(),
    )?;
    endpoints
        .identities
        .insert(Region::West, RejectedIdentity.into());
    let error = endpoints
        .request(
            Region::West,
            reqwest::Method::GET,
            "/v1/projects/test/deployment",
            bytes::Bytes::new(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("obtain service identity"));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn hinted_replies_report_the_route_before_returning_the_target() -> Result<()> {
    let body = "{\"routeHint\":\"https://host.test\"}\n{\"route\":\"https://host.test\",\"token\":\"t\",\"ownerEpoch\":1,\"expiresAtMs\":9}\n";
    let (send, receive) = std::sync::mpsc::channel();
    let target = read_hinted_reply(
        reqwest::Response::from(axum::http::Response::new(body)),
        Box::new(move |route| send.send(route).unwrap()),
    )
    .await?;
    assert_eq!(receive.try_recv()?, "https://host.test");
    assert_eq!(target["token"], "t");
    let failure = "{\"failure\":{\"status\":503,\"body\":null}}\n";
    assert!(
        read_hinted_reply(
            reqwest::Response::from(axum::http::Response::new(failure)),
            Box::new(|_| {}),
        )
        .await
        .is_err()
    );
    Ok(())
}
