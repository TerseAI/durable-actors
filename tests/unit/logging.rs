use super::{ActorLogRouter, LogExportConfig};
use crate::actor::ActorKey;
use anyhow::Result;
use axum::{Router, body::Bytes, http::HeaderMap, routing::post};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn actor_output_and_runtime_events_export_as_otlp_without_control_plane_events() -> Result<()>
{
    if std::env::var_os("ACTOR_LOG_TEST_CHILD").is_none() {
        let output = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "logging::tests::actor_output_and_runtime_events_export_as_otlp_without_control_plane_events",
            ])
            .env("ACTOR_LOG_TEST_CHILD", "1")
            .env(
                "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
                "authorization=Bearer wrong-secret",
            )
            .env(
                "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
                "http://127.0.0.1:1/wrong",
            )
            .output()
            .await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/v1/logs", listener.local_addr()?);
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/v1/logs",
                post(move |headers: HeaderMap, body: Bytes| {
                    let send = send.clone();
                    async move {
                        send.send((headers, body)).unwrap();
                        (
                            [("content-type", "application/x-protobuf")],
                            Vec::<u8>::new(),
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let router = ActorLogRouter::default();
    let config = LogExportConfig {
        endpoint,
        headers_env: Some("LOG_HEADERS".into()),
    };
    let actor = ActorKey {
        project_id: "customer-a".into(),
        actor_name: "Chat".into(),
        actor_id: "private-id".into(),
    };
    let session = router
        .start(&config, &actor, "host-1", "canada", |name| {
            (name == "LOG_HEADERS").then(|| r#"{"authorization":"Bearer secret"}"#.into())
        })
        .await?;
    let subscriber = tracing_subscriber::registry().with(router.clone());
    tracing::subscriber::with_default(subscriber, || {
        tracing::warn!(target: "durable_actors::host", error = "private-error", "host warning");
        tracing::info!(target: "durable_actors::control_plane", "control-plane-private-data");
        tracing::warn!(target: "opentelemetry_sdk", "exporter diagnostic");
    });
    router
        .capture(&b"customer stdout\nfinal partial line"[..], "stdout")
        .await;
    router.capture(&b"customer stderr\n"[..], "stderr").await;
    session.shutdown().await;
    let mut records = vec![];
    while let Ok((headers, body)) = receive.try_recv() {
        assert_eq!(headers["authorization"], "Bearer secret");
        assert_eq!(headers["content-type"], "application/x-protobuf");
        let document = ExportLogsServiceRequest::decode(body)?;
        for resource in document.resource_logs {
            let attrs = format!("{:?}", resource.resource);
            assert!(
                attrs.contains("customer-a")
                    && attrs.contains("canada")
                    && attrs.contains("host-1")
            );
            for scope in resource.scope_logs {
                records.extend(scope.log_records);
            }
        }
    }
    assert_eq!(records.len(), 4, "{records:?}");
    let exported = format!("{records:?}");
    for expected in [
        "customer stdout",
        "final partial line",
        "customer stderr",
        "private-error",
    ] {
        assert!(exported.contains(expected));
    }
    assert!(!exported.contains("control-plane-private-data"));
    assert!(!exported.contains("exporter diagnostic"));
    server.abort();
    Ok(())
}

#[test]
fn log_configuration_rejects_credentials_in_urls_and_invalid_secret_references() {
    for endpoint in [
        "file:///tmp/logs",
        "https://user:secret@example.test/v1/logs",
        "https://example.test/v1/logs?key=secret",
        "not a URL",
    ] {
        let config = LogExportConfig {
            endpoint: endpoint.into(),
            headers_env: None,
        };
        let error = config.validate().unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
    let config = LogExportConfig {
        endpoint: "https://example.test/v1/logs".into(),
        headers_env: Some("bad-name".into()),
    };
    assert!(config.validate().is_err());
}
