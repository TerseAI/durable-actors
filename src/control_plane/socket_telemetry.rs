//! Exports a gateway's WebSocket connection events to the collector's internal OTLP receiver,
//! authenticated with the pod's projected service-account token.

use std::{
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    logs::{LogRecord, Logger, LoggerProvider, Severity},
};
use opentelemetry_otlp::{LogExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{SdkLogger, SdkLoggerProvider},
};
use tonic::{
    Request, Status, metadata::MetadataValue, service::Interceptor, transport::ClientTlsConfig,
};

use crate::{
    request_traces::otlp::GATEWAY_ID,
    sockets::events::{SocketEvent, SocketEvents},
};

const SCOPE: &str = "durable-actors-socket-gateway";

pub struct SocketTelemetryConfig {
    pub endpoint: String,
    /// A projected service-account token the kubelet keeps fresh; read on every export.
    pub token_file: PathBuf,
}

pub(super) struct OtlpSocketEvents {
    logger: SdkLogger,
    // Flushes buffered events when the gateway shuts down.
    _provider: SdkLoggerProvider,
}

impl OtlpSocketEvents {
    pub(super) fn connect(config: &SocketTelemetryConfig, gateway_id: &str) -> Result<Self> {
        let exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .with_interceptor(TokenFile(config.token_file.clone()));
        let exporter = if config.endpoint.starts_with("https:") {
            exporter.with_tls_config(ClientTlsConfig::new().with_webpki_roots())
        } else {
            exporter
        }
        .build()?;
        let resource = Resource::builder()
            .with_service_name(SCOPE)
            .with_attribute(KeyValue::new(GATEWAY_ID, gateway_id.to_owned()))
            .build();
        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();
        Ok(Self {
            logger: provider.logger(SCOPE),
            _provider: provider,
        })
    }
}

impl SocketEvents for OtlpSocketEvents {
    fn record(&self, event: SocketEvent) {
        let mut log = self.logger.create_log_record();
        log.set_timestamp(UNIX_EPOCH + Duration::from_millis(event.at_ms()));
        log.set_event_name(event.name());
        log.set_severity_number(Severity::Info);
        for (key, value) in event.attributes() {
            log.add_attribute(key, value);
        }
        self.logger.emit(log);
    }
}

#[derive(Clone)]
struct TokenFile(PathBuf);

impl Interceptor for TokenFile {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = std::fs::read_to_string(&self.0)
            .map_err(|_| Status::unauthenticated("socket telemetry token is unreadable"))?;
        let header = MetadataValue::try_from(format!("Bearer {}", token.trim()))
            .map_err(|_| Status::unauthenticated("socket telemetry token is not a header value"))?;
        request.metadata_mut().insert("authorization", header);
        Ok(request)
    }
}
