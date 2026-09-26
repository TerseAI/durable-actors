use super::*;
use crate::postgres::testing::with_postgres;

#[tokio::test]
async fn proxy_claims_are_exclusive_and_expired_workers_cannot_publish() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let provider = Arc::new(CommandSandboxProvider::new(
            "fixture".into(),
            "false".into(),
            Default::default(),
        )?);
        let pool = ProxyPool::new(database, provider, "im-runtime".into())?;
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
        let mut contender = config.clone();
        contender.session = uuid::Uuid::new_v4().to_string();
        let (first, second) = tokio::join!(
            pool.claim("object", &config),
            pool.claim("object", &contender)
        );
        assert_ne!(first?, second?);
        pool.expire("object", &config).await?;
        pool.expire("object", &contender).await?;
        let mut replacement = config.clone();
        replacement.session = uuid::Uuid::new_v4().to_string();
        assert!(pool.claim("object", &replacement).await?);
        let handle = SpareHandle {
            name: "proxy".into(),
            resource_id: "sb-test".into(),
            route: "http://127.0.0.1:1".into(),
            canonical_region: config.region.as_str().into(),
            control_route: String::new(),
            control_token: String::new(),
        };
        assert!(pool.publish("object", &config, &handle).await.is_err());
        pool.publish("object", &replacement, &handle).await?;
        assert_eq!(
            pool.current("object", &config)
                .await?
                .unwrap()
                .config
                .session,
            replacement.session
        );
        assert!(!pool.claim("object", &config).await?);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_transient_health_failure_does_not_retire_a_proxy() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/healthz",
                axum::routing::get(move || {
                    let requests = requests.clone();
                    async move {
                        if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                            axum::http::StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            axum::http::StatusCode::OK
                        }
                    }
                }),
            ),
        )
        .await
    });
    let pool = ProxyPool::new(
        PostgresDatabase::lazy("postgresql://localhost:1/unavailable?sslmode=disable")?,
        Arc::new(CommandSandboxProvider::new(
            "test".into(),
            "false".into(),
            Default::default(),
        )?),
        "im-runtime".into(),
    )?;
    assert!(pool.healthy(&origin).await);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
    Ok(())
}
