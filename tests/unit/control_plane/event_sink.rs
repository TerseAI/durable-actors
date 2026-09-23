use super::*;
use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
use tokio::sync::mpsc;

#[tokio::test]
async fn callbacks_use_the_configured_secret_or_omit_authorization() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/events", listener.local_addr()?);
    let (sender, mut received) = mpsc::unbounded_channel();
    let app = Router::new()
        .route("/events", post(receive_event))
        .with_state(sender);
    let server = tokio::spawn(async { axum::serve(listener, app).await });
    for secret in [None, Some("callback-secret")] {
        let sink = HttpSocketMessageEventSink::new(url.clone(), secret.map(str::to_owned))?;
        let event = SocketMessageEvent::new(
            &ActorKey {
                project_id: "test".into(),
                actor_name: "Room".into(),
                actor_id: "one".into(),
            },
            None,
            "connection",
            &ActorSocketMessage::Text {
                data: "hello".into(),
            },
        );
        let expected = serde_json::to_value(&event)?;
        sink.deliver(event).await?;
        let (headers, body) = received.recv().await.context("receive callback")?;
        assert_eq!(
            headers
                .get("authorization")
                .map(|value| value.to_str().unwrap().to_owned()),
            secret.map(|secret| format!("Bearer {secret}"))
        );
        assert_eq!(body, expected);
    }
    server.abort();
    Ok(())
}

async fn receive_event(
    State(sender): State<mpsc::UnboundedSender<(HeaderMap, serde_json::Value)>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) {
    sender.send((headers, body)).unwrap();
}
