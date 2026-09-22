use axum::{Router, extract::WebSocketUpgrade, response::Response, routing::get};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::*;

#[test]
fn actor_idle_timeout_uses_bounded_seconds() -> Result<()> {
    assert_eq!(actor_idle_timeout_seconds(&mut |_| None)?, 60);
    for value in ["1", "10", "86400"] {
        let parsed = actor_idle_timeout_seconds(&mut |name| {
            assert_eq!(name, "DURABLE_ACTORS_ACTOR_IDLE_TIMEOUT_SECONDS");
            Some(value.into())
        })?;
        assert_eq!(parsed, value.parse::<u64>()?);
    }
    for value in ["0", "-1", "1.5", "86401", "not-a-number"] {
        assert!(actor_idle_timeout_seconds(&mut |_| Some(value.into())).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn server_carries_websocket_upgrades() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let routes = tonic::service::Routes::from(Router::new().route("/socket", get(echo_websocket)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_routes(listener, routes, async {
        let _ = shutdown_rx.await;
    }));

    let (mut socket, _) = connect_async(format!("ws://{address}/socket")).await?;
    socket.send(Message::Text("hello".into())).await?;
    assert_eq!(
        socket.next().await.transpose()?,
        Some(Message::Text("hello".into()))
    );
    socket.close(None).await?;
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[test]
fn parses_the_minimal_storage_configuration() -> Result<()> {
    let values = HashMap::from([
        ("DURABLE_ACTORS_JWT_SIGNING_KEY", "c2lnbmluZw=="),
        ("DURABLE_ACTORS_API_KEY", "api-key"),
        ("DURABLE_ACTORS_BUCKET", "actor-state-test"),
        ("DURABLE_ACTORS_SANDBOX_PROVIDER", "modal"),
        ("DURABLE_ACTORS_RUNTIME_IMAGE", "im-runtime"),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            "https://objects.example.com",
        ),
        ("MODAL_TOKEN_ID", "modal-token-id"),
        ("MODAL_TOKEN_SECRET", "modal-token-secret"),
        (
            "DURABLE_ACTORS_POSTGRES_URL",
            "postgresql://localhost/actors",
        ),
    ]);
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert_eq!(config.storage.bucket, "actor-state-test");
    assert_eq!(config.jwt_max_lifetime, Duration::from_secs(86_400));
    Ok(())
}

#[test]
fn mutable_modal_network_requires_explicit_boolean_configuration() -> Result<()> {
    let mut values = HashMap::from([
        ("DURABLE_ACTORS_SANDBOX_PROVIDER", "modal"),
        ("DURABLE_ACTORS_RUNTIME_IMAGE", "im-runtime"),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            "https://control.example",
        ),
        ("MODAL_TOKEN_ID", "id"),
        ("MODAL_TOKEN_SECRET", "secret"),
    ]);
    let configure = |values: &HashMap<&str, &str>| {
        sandbox_provider_config(
            &mut |name| values.get(name).map(|v| (*v).into()),
            "issuer",
            "audience",
        )
    };
    assert!(
        !configure(&values)?
            .environment
            .contains_key("DURABLE_ACTORS_MODAL_MUTABLE_NETWORK")
    );
    values.insert("DURABLE_ACTORS_MODAL_MUTABLE_NETWORK", "true");
    assert_eq!(
        configure(&values)?.environment["DURABLE_ACTORS_MODAL_MUTABLE_NETWORK"],
        "true"
    );
    values.insert("DURABLE_ACTORS_MODAL_MUTABLE_NETWORK", "yes");
    assert!(configure(&values).is_err());
    Ok(())
}

#[test]
fn configures_socket_events_without_a_separate_key() -> Result<()> {
    let mut complete = HashMap::from([(
        "DURABLE_ACTORS_SOCKET_EVENT_URL",
        "https://api.example.com/events",
    )]);
    let sink =
        socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
            .context("socket event sink was not configured")?;
    assert_eq!(sink.url, "https://api.example.com/events");
    complete.remove("DURABLE_ACTORS_SOCKET_EVENT_URL");
    assert!(
        socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
            .is_none()
    );
    Ok(())
}

async fn echo_websocket(upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(async |mut socket| {
        if let Some(Ok(message)) = socket.recv().await {
            let _ = socket.send(message).await;
        }
    })
}
