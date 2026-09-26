use super::*;
use crate::{
    postgres::{PostgresDatabase, testing::with_postgres},
    regional::proxy::pool::{ProxyLaunch, ProxyProvisioner},
    sandbox::{SocketCredentials, SpareHandle},
};
use anyhow::Result;
use async_trait::async_trait;
use base64::Engine;
use tower::ServiceExt;

struct Provider;
#[async_trait]
impl ProxyProvisioner for Provider {
    async fn create(&self, _: &ProxyLaunch) -> Result<SpareHandle> {
        Ok(SpareHandle {
            name: "proxy".into(),
            resource_id: "sandbox".into(),
            route: "https://proxy.test".into(),
            canonical_region: Region::West.as_str().into(),
            control_route: String::new(),
            control_token: String::new(),
        })
    }
    async fn retire(&self, _: &SpareHandle) -> Result<()> {
        Ok(())
    }
    async fn socket_credentials(&self, _: &SpareHandle, _: &str) -> Result<SocketCredentials> {
        Ok(SocketCredentials {
            url: "https://proxy.test".into(),
            token: "connect".into(),
        })
    }
}

#[tokio::test]
async fn proxy_api_requires_internal_authentication_and_reports_signed_expiry() -> Result<()> {
    with_postgres(async |fixture| {
        let key = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(&aws_lc_rs::rand::SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(&base64::engine::general_purpose::STANDARD.encode(key.as_ref()), "test", "issuer", "authority", "invoke", std::time::Duration::from_secs(86400))?;
        let admin = AdminService::new(Some("internal".into()), Arc::new(super::super::admin::LocalAdminRegistry::default()), issuer.clone())?;
        let pool = Arc::new(ProxyPool::new(PostgresDatabase::connect(&fixture.url).await?, Arc::new(Provider), "im-runtime".into())?);
        let routes = router(pool, issuer, admin, Region::West, "issuer".into());
        let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
        let id = uuid::Uuid::new_v4().to_string();
        for (secret, kind) in [("client", "invocation"), ("internal", "invocation"), ("internal", "socket")] {
            let body = json!({"objectId":id,"homeRegion":Region::East,"destination":{"route":"https://primary.test","token":"grant","ownerEpoch":1,"expiresAtMs":now+3_600_000,"authorizedUntilMs":if kind == "socket" { Some(now+3_600_000) } else {None},"kind":kind}});
            let request = axum::http::Request::builder().method("POST")
                .uri("/v1/projects/project/actors/Counter/one/find-proxy")
                .header("authorization", format!("Bearer {secret}")).header("content-type", "application/json")
                .body(axum::body::Body::from(serde_json::to_vec(&body)?))?;
            let response = routes.clone().oneshot(request).await?;
            if secret == "client" { assert_eq!(response.status(), StatusCode::UNAUTHORIZED); continue; }
            assert_eq!(response.status(), StatusCode::OK);
            let body: Value = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 65536).await?)?;
            let payload = body["token"].as_str().unwrap().split('.').nth(1).unwrap();
            let claims: Value = serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload)?)?;
            let expires = claims["exp"].as_i64().unwrap() * 1000;
            assert_eq!(body["expiresAtMs"], expires);
            assert!(expires <= claims["iat"].as_i64().unwrap() * 1000 + 60_000);
            if kind == "socket" { assert_eq!(body["connectByMs"], expires); }
        }
        Ok(())
    }).await
}
