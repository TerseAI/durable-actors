use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry::trace::{Span, SpanKind, Status, Tracer, TracerProvider};
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{self, BatchLogProcessor, SdkLogger, SdkLoggerProvider},
    trace::{self, BatchSpanProcessor, SdkTracer, SdkTracerProvider},
};
use tonic::transport::ClientTlsConfig;

use crate::{
    actor::{ActorLogRecord, ActorLogSink, LogLevel, StdioLogSink},
    control_plane::HostAuthorization,
    request_traces::{
        RequestKind,
        otlp::{self, EXPORT_DELAY, LOG_ID, REQUEST_ID},
    },
    request_tracking::{RequestCompletion, RequestObserver, RequestOutcome},
};

const ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const SCOPE: &str = "durable-actors-host";

pub(crate) struct HostTelemetry {
    traces: SdkTracerProvider,
    logs: SdkLoggerProvider,
}

impl HostTelemetry {
    pub(crate) fn from_env(authorization: HostAuthorization) -> Result<Option<Self>> {
        match std::env::var(ENDPOINT) {
            Ok(endpoint) if !endpoint.is_empty() => {
                Ok(Some(Self::connect(&endpoint, authorization)?))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn tracer(&self) -> SdkTracer {
        self.traces.tracer(SCOPE)
    }

    pub(crate) fn log_sink(&self) -> Arc<dyn ActorLogSink> {
        Arc::new(OtelLogSink {
            logger: self.logs.logger(SCOPE),
        })
    }

    pub(crate) fn shutdown(&self) {
        if let Err(error) = self.traces.shutdown() {
            tracing::warn!(%error, "request span export did not flush");
        }
        if let Err(error) = self.logs.shutdown() {
            tracing::warn!(%error, "actor log export did not flush");
        }
    }

    fn connect(endpoint: &str, authorization: HostAuthorization) -> Result<Self> {
        let resource = Resource::builder().with_service_name(SCOPE).build();
        let spans = exporter(
            SpanExporter::builder().with_tonic(),
            endpoint,
            &authorization,
        )
        .build()?;
        let logs = exporter(
            LogExporter::builder().with_tonic(),
            endpoint,
            &authorization,
        )
        .build()?;
        Ok(Self {
            traces: SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(
                    BatchSpanProcessor::builder(spans)
                        .with_batch_config(
                            trace::BatchConfigBuilder::default()
                                .with_scheduled_delay(EXPORT_DELAY)
                                .build(),
                        )
                        .build(),
                )
                .build(),
            logs: SdkLoggerProvider::builder()
                .with_resource(resource)
                .with_log_processor(
                    BatchLogProcessor::builder(logs)
                        .with_batch_config(
                            logs::BatchConfigBuilder::default()
                                .with_scheduled_delay(EXPORT_DELAY)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        })
    }
}

fn exporter<B: WithExportConfig + WithTonicConfig>(
    builder: B,
    endpoint: &str,
    authorization: &HostAuthorization,
) -> B {
    let builder = builder
        .with_endpoint(endpoint)
        .with_interceptor(authorization.clone());
    if endpoint.starts_with("https:") {
        builder.with_tls_config(ClientTlsConfig::new().with_webpki_roots())
    } else {
        builder
    }
}

pub(crate) struct TraceRecorder {
    tracer: SdkTracer,
    operation: String,
    attributes: Vec<KeyValue>,
}

impl TraceRecorder {
    pub(crate) fn new(
        tracer: SdkTracer,
        invocation: &crate::actor::ActorInvocation,
        kind: RequestKind,
        connection_id: Option<String>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        let mut attributes = vec![
            KeyValue::new(otlp::REQUEST_ID, invocation.request_id.clone()),
            KeyValue::new(otlp::KIND, kind.as_str()),
        ];
        attributes.extend(connection_id.map(|id| KeyValue::new(otlp::CONNECTION_ID, id)));
        attributes.extend(metadata.map(|value| KeyValue::new(otlp::METADATA, value.to_string())));
        Self {
            tracer,
            operation: invocation.method.clone(),
            attributes,
        }
    }
}

impl RequestObserver for TraceRecorder {
    fn record(self: Box<Self>, completion: &RequestCompletion) {
        let started = UNIX_EPOCH + Duration::from_millis(completion.started_at_ms);
        let ended = started + Duration::from_secs_f64(completion.timings.duration_ms / 1000.0);
        let mut attributes = self.attributes;
        attributes.extend(completion_attributes(completion));
        let mut span = self
            .tracer
            .span_builder(self.operation)
            .with_kind(SpanKind::Server)
            .with_start_time(started)
            .with_attributes(attributes)
            .start_with_context(&self.tracer, &opentelemetry::Context::new());
        if matches!(completion.outcome, RequestOutcome::Failed) {
            span.set_status(Status::error(completion.outcome.as_str()));
        }
        span.end_with_timestamp(ended);
    }
}

fn completion_attributes(completion: &RequestCompletion) -> impl Iterator<Item = KeyValue> {
    let timings = &completion.timings;
    [
        Some(KeyValue::new(otlp::OUTCOME, completion.outcome.as_str())),
        Some(KeyValue::new(otlp::HOST_STATE, timings.host_state.as_str())),
        Some(KeyValue::new(otlp::ROUTING_MS, timings.routing_ms)),
        timings
            .queue_wait_ms
            .map(|wait| KeyValue::new(otlp::QUEUE_WAIT_MS, wait)),
        completion
            .state_version
            .and_then(|version| i64::try_from(version).ok())
            .map(|version| KeyValue::new(otlp::STATE_VERSION, version)),
    ]
    .into_iter()
    .flatten()
}

struct OtelLogSink {
    logger: SdkLogger,
}

impl ActorLogSink for OtelLogSink {
    fn emit(&self, record: ActorLogRecord) {
        StdioLogSink::write(&record);
        let mut log = self.logger.create_log_record();
        log.set_timestamp(std::time::SystemTime::now());
        log.set_severity_number(severity(record.level));
        log.set_severity_text(record.level.as_str());
        log.add_attribute(LOG_ID, uuid::Uuid::now_v7().to_string());
        if let Some(request_id) = record.request_id {
            log.add_attribute(REQUEST_ID, request_id);
        }
        log.set_body(AnyValue::from(record.message));
        self.logger.emit(log);
    }
}

fn severity(level: LogLevel) -> Severity {
    match level {
        LogLevel::Debug => Severity::Debug,
        LogLevel::Info => Severity::Info,
        LogLevel::Warn => Severity::Warn,
        LogLevel::Error => Severity::Error,
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/telemetry.rs"]
mod tests;
