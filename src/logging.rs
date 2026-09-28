use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime},
};

use anyhow::{Result, ensure};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, Response, header::CONTENT_TYPE};
use opentelemetry::{
    KeyValue,
    logs::{LogRecord, Logger, LoggerProvider, Severity},
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_http::{Bytes, HttpClient, HttpError};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{BatchConfigBuilder, BatchLogProcessor, SdkLogger, SdkLoggerProvider},
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use crate::actor::ActorKey;

const MAX_LINE_BYTES: usize = 16 * 1024;
const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);
type Bridge = OpenTelemetryTracingBridge<SdkLoggerProvider, SdkLogger>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogExportConfig {
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers_env: Option<String>,
}

#[derive(Clone, Default)]
pub struct ActorLogRouter(Arc<LogState>);

#[derive(Default)]
struct LogState {
    sink: Mutex<Option<LogSink>>,
    readers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

struct LogSink {
    bridge: Bridge,
    logger: SdkLogger,
}

pub(crate) struct LogSession {
    router: ActorLogRouter,
    provider: SdkLoggerProvider,
}

#[derive(Debug)]
struct ProjectLogClient {
    client: reqwest::blocking::Client,
    headers: HeaderMap,
}

impl ActorLogRouter {
    pub fn global() -> &'static Self {
        static ROUTER: OnceLock<ActorLogRouter> = OnceLock::new();
        ROUTER.get_or_init(Self::default)
    }

    pub(crate) async fn start(
        &self,
        config: &LogExportConfig,
        actor: &ActorKey,
        host: &str,
        region: &str,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<LogSession> {
        config.validate()?;
        let headers = config.headers(&get)?;
        let endpoint = config.endpoint.clone();
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", "durable-actors-host"),
                KeyValue::new("service.instance.id", host.to_owned()),
                KeyValue::new("cloud.region", region.to_owned()),
                KeyValue::new("durable_actors.project_id", actor.project_id.clone()),
                KeyValue::new("durable_actors.actor_name", actor.actor_name.clone()),
                KeyValue::new("durable_actors.actor_id", actor.actor_id.clone()),
            ])
            .build();
        let provider =
            tokio::task::spawn_blocking(move || build_provider(endpoint, headers, resource))
                .await??;
        let mut sink = self.0.sink.lock().unwrap();
        ensure!(sink.is_none(), "actor log exporter is already assigned");
        *sink = Some(LogSink {
            bridge: Bridge::new(&provider),
            logger: provider.logger("durable-actors.console"),
        });
        Ok(LogSession {
            router: self.clone(),
            provider,
        })
    }

    async fn capture(&self, input: impl AsyncRead + Unpin, stream: &'static str) {
        let mut reader = BufReader::new(input);
        let mut line = Vec::new();
        loop {
            let available = match reader.fill_buf().await {
                Ok(bytes) if !bytes.is_empty() => bytes,
                _ => break,
            };
            let end = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|index| index + 1)
                .unwrap_or(available.len());
            let count = end.min(MAX_LINE_BYTES - line.len());
            line.extend_from_slice(&available[..count]);
            reader.consume(count);
            if line.ends_with(b"\n") || line.len() == MAX_LINE_BYTES {
                self.emit_line(&line, stream);
                line.clear();
            }
        }
        if !line.is_empty() {
            self.emit_line(&line, stream);
        }
    }

    pub(crate) fn capture_child(&self, child: &mut tokio::process::Child) {
        let mut readers = self.0.readers.lock().unwrap();
        if let Some(stdout) = child.stdout.take() {
            let router = self.clone();
            readers.push(tokio::spawn(async move {
                router.capture(stdout, "stdout").await
            }));
        }
        if let Some(stderr) = child.stderr.take() {
            let router = self.clone();
            readers.push(tokio::spawn(async move {
                router.capture(stderr, "stderr").await
            }));
        }
    }

    fn emit_line(&self, bytes: &[u8], stream: &'static str) {
        let sink = self.0.sink.lock().unwrap();
        let Some(sink) = sink.as_ref() else {
            return;
        };
        let mut record = sink.logger.create_log_record();
        record.set_timestamp(SystemTime::now());
        record.set_body(
            String::from_utf8_lossy(bytes)
                .trim_end_matches(['\r', '\n'])
                .to_owned()
                .into(),
        );
        record.set_severity_number(if stream == "stderr" {
            Severity::Error
        } else {
            Severity::Info
        });
        record.set_severity_text(if stream == "stderr" { "ERROR" } else { "INFO" });
        record.add_attribute("log.iostream", stream);
        sink.logger.emit(record);
    }
}

impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for ActorLogRouter {
    fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
        let target = event.metadata().target();
        if !target.starts_with("durable_actors")
            || target.starts_with("durable_actors::control_plane")
        {
            return;
        }
        if let Some(sink) = self.0.sink.lock().unwrap().as_ref() {
            sink.bridge.on_event(event, context);
        }
    }
}

impl LogSession {
    pub async fn shutdown(self) {
        let readers = std::mem::take(&mut *self.router.0.readers.lock().unwrap());
        for mut reader in readers {
            if tokio::time::timeout(Duration::from_secs(1), &mut reader)
                .await
                .is_err()
            {
                reader.abort();
            }
        }
        self.router.0.sink.lock().unwrap().take();
        let provider = self.provider.clone();
        let _ = tokio::task::spawn_blocking(move || {
            provider.shutdown_with_timeout(Duration::from_secs(5))
        })
        .await;
    }
}

impl LogExportConfig {
    pub fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|_| anyhow::anyhow!("invalid log export endpoint"))?;
        ensure!(
            self.endpoint.len() <= 2048
                && matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "log export endpoint must be an HTTP(S) URL without credentials, query, or fragment"
        );
        if let Some(name) = &self.headers_env {
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name.bytes().enumerate().all(|(index, c)| c == b'_'
                        || c.is_ascii_alphabetic()
                        || (index > 0 && c.is_ascii_digit())),
                "invalid log export headers environment variable name"
            );
        }
        Ok(())
    }

    fn headers(&self, get: &impl Fn(&str) -> Option<String>) -> Result<HeaderMap> {
        let Some(name) = &self.headers_env else {
            return Ok(HeaderMap::new());
        };
        let value =
            get(name).ok_or_else(|| anyhow::anyhow!("log export authentication is missing"))?;
        ensure!(
            value.len() <= 8192,
            "log export authentication is too large"
        );
        let values: HashMap<String, String> = serde_json::from_str(&value)
            .map_err(|_| anyhow::anyhow!("log export headers must be a JSON object of strings"))?;
        let mut headers = HeaderMap::new();
        for (key, value) in values {
            let name = HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("invalid log export authentication header"))?;
            let mut value = HeaderValue::from_str(&value)
                .map_err(|_| anyhow::anyhow!("invalid log export authentication header"))?;
            ensure!(
                !matches!(
                    name.as_str(),
                    "host" | "content-type" | "content-length" | "transfer-encoding"
                ),
                "invalid log export authentication header"
            );
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        Ok(headers)
    }
}

fn build_provider(
    endpoint: String,
    headers: HeaderMap,
    resource: Resource,
) -> Result<SdkLoggerProvider> {
    let client = reqwest::blocking::Client::builder()
        .timeout(EXPORT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_endpoint(endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .with_http_client(ProjectLogClient { client, headers })
        .build()
        .map_err(|_| anyhow::anyhow!("could not configure actor log exporter"))?;
    let batch = BatchLogProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(1024)
                .with_max_export_batch_size(64)
                .with_scheduled_delay(Duration::from_secs(1))
                .build(),
        )
        .build();
    Ok(SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_log_processor(batch)
        .build())
}

#[async_trait::async_trait]
impl HttpClient for ProjectLogClient {
    async fn send_bytes(&self, mut request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        // The OTLP exporter otherwise lets ambient environment headers override project credentials.
        *request.headers_mut() = self.headers.clone();
        request.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        self.client.send_bytes(request).await
    }
}

#[cfg(test)]
#[path = "../tests/unit/logging.rs"]
mod tests;
