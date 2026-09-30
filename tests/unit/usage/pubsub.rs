use super::*;
use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde_json::{Value, json};

struct Credentials;
#[async_trait]
impl gcp_auth::TokenProvider for Credentials {
    async fn token(
        &self,
        scopes: &[&str],
    ) -> std::result::Result<Arc<gcp_auth::Token>, gcp_auth::Error> {
        assert_eq!(scopes, &["https://www.googleapis.com/auth/pubsub"]);
        Ok(Arc::new(
            serde_json::from_value(json!({"access_token":"test-token","expires_in":3600})).unwrap(),
        ))
    }
    async fn project_id(&self) -> std::result::Result<Arc<str>, gcp_auth::Error> {
        Ok("project".into())
    }
}

#[tokio::test]
async fn publishes_encoded_usage_and_requires_confirmation_for_every_message() -> Result<()> {
    let event = UsageInterval {
        id: "sandbox_usage_v1:s:1000:2000".into(),
        assignment: crate::usage::tests::fixture(),
        start_ms: 1000,
        end_ms: 2000,
    };
    let expected = serde_json::to_value(&event)?;
    let app = Router::new()
        .route(
            "/ok",
            post(move |headers: HeaderMap, Json(body): Json<Value>| {
                let expected = expected.clone();
                async move {
                    assert_eq!(headers["authorization"], "Bearer test-token");
                    let encoded = body["messages"][0]["data"].as_str().unwrap();
                    assert_eq!(
                        serde_json::from_slice::<Value>(&STANDARD.decode(encoded).unwrap())
                            .unwrap(),
                        expected
                    );
                    Json(json!({"messageIds":["accepted"]}))
                }
            }),
        )
        .route(
            "/incomplete",
            post(|| async { Json(json!({"messageIds":[]})) }),
        )
        .route(
            "/failed",
            post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut sink = PubSubUsageSink::new("projects/project/topics/usage", Arc::new(Credentials))?;
    for (path, success) in [("ok", true), ("incomplete", false), ("failed", false)] {
        sink.url = format!("http://{address}/{path}").parse()?;
        assert_eq!(
            sink.deliver(std::slice::from_ref(&event)).await.is_ok(),
            success
        );
    }
    server.abort();
    for invalid in [
        "https://attacker/topics/a",
        "projects/p/topics/a?key=x",
        "projects/p/topics/../bad",
        "projects//topics/a",
    ] {
        assert!(PubSubUsageSink::new(invalid, Arc::new(Credentials)).is_err());
    }
    Ok(())
}
