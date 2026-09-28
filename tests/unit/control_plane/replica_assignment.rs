use super::*;
use axum::{
    Router,
    extract::ConnectInfo,
    http::{HeaderMap, Method, StatusCode},
    routing::any,
};
use std::{net::SocketAddr, sync::Mutex};

#[tokio::test]
async fn replica_warmup_and_assignment_use_the_same_connection() -> Result<()> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let route = format!("http://{}", listener.local_addr()?);
    let router = Router::new().route(
        "/assign",
        any(
            move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                  method: Method,
                  headers: HeaderMap| {
                let observed = observed.clone();
                async move {
                    observed.lock().unwrap().push((
                        peer,
                        method.clone(),
                        headers.get("authorization").cloned(),
                    ));
                    if method == Method::GET {
                        StatusCode::METHOD_NOT_ALLOWED
                    } else {
                        StatusCode::NO_CONTENT
                    }
                }
            },
        ),
    );
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    });
    let client = assignment_client(std::time::Duration::from_secs(600))?;
    warm_assignment(&client, &route).await?;
    let spare = crate::sandbox::SpareHandle {
        name: "spare".into(),
        resource_id: "sb-test".into(),
        route: route.clone(),
        control_route: route,
        control_token: "secret".into(),
        canonical_region: "north-america-west".into(),
    };
    let assignment = ReplicaAssignment {
        host_id: "replica".into(),
        secret: "replica-secret".into(),
        scope: ReplicaScope {
            actor: crate::actor::ActorKey {
                project_id: "p".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
            },
            host: crate::host::HostId::new("host"),
            session: "session".into(),
            region: "north-america-west".into(),
        },
    };
    assign_replica(&client, &spare, &assignment).await?;
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].1, Method::GET);
    assert!(
        calls[0].2.is_none(),
        "warming must not assign a session or send credentials"
    );
    assert_eq!(calls[1].1, Method::POST);
    assert_eq!(calls[1].2.as_ref().unwrap(), "Bearer secret");
    assert_eq!(
        calls[0].0, calls[1].0,
        "assignment opened another TCP connection"
    );
    server.abort();
    Ok(())
}
