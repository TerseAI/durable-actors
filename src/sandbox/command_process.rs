use std::{collections::HashMap, process::Stdio, time::Instant};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, Command},
    sync::Mutex,
};

use super::{MAX_PROVIDER_OUTPUT_BYTES, ProviderCommandTimings, elapsed_ms};

#[derive(Default)]
pub(super) struct Process(Mutex<Option<Worker>>);

impl Process {
    pub async fn exchange<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        command: &str,
        environment: &HashMap<String, String>,
        request: &Request,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
    ) -> Result<Reply> {
        let document = serde_json::to_vec(request)?;
        ensure!(
            document.len() <= MAX_PROVIDER_OUTPUT_BYTES,
            "sandbox provider command is too large"
        );
        let client = self
            .client(command, environment, started_at, timings)
            .await?;
        let response = client
            .post("http://localhost/")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(document)
            .send()
            .await
            .context("sandbox provider request failed; outcome may be unknown")?;
        ensure!(
            response.status().is_success(),
            "sandbox provider returned HTTP {}; outcome may be unknown",
            response.status()
        );
        let response = read_response(response).await?;
        let response: ProviderResponse<Reply> =
            serde_json::from_slice(&response).context("decode provider response")?;
        timings.response_decoded_at_ms = Some(elapsed_ms(started_at));
        match response {
            ProviderResponse::Success { result } => Ok(result),
            ProviderResponse::Failure { error } => {
                anyhow::bail!("sandbox provider failed: {error}")
            }
        }
    }

    async fn client(
        &self,
        command: &str,
        environment: &HashMap<String, String>,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
    ) -> Result<reqwest::Client> {
        let mut worker = self.0.lock().await;
        if let Some(worker) = worker.as_mut() {
            if worker.child.try_wait()?.is_none() {
                return Ok(worker.client.clone());
            }
        }
        *worker = None;
        let started = Worker::start(command, environment, started_at, timings).await?;
        let client = started.client.clone();
        *worker = Some(started);
        Ok(client)
    }
}

struct Worker {
    child: Child,
    client: reqwest::Client,
    _directory: tempfile::TempDir,
}

impl Worker {
    async fn start(
        command: &str,
        environment: &HashMap<String, String>,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
    ) -> Result<Self> {
        let directory = tempfile::Builder::new().prefix("da-provider-").tempdir()?;
        let socket = directory.path().join("socket");
        let mut child = Command::new(command)
            .arg("--socket")
            .arg(&socket)
            .env_clear()
            .envs(environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("start sandbox provider")?;
        timings.spawned_at_ms = Some(elapsed_ms(started_at));
        let stdout = child.stdout.take().context("open provider readiness")?;
        let mut reader = BufReader::new(stdout.take(1025));
        let mut document = Vec::new();
        reader
            .read_until(b'\n', &mut document)
            .await
            .context("read provider readiness")?;
        ensure!(
            document.len() <= 1024 && document.ends_with(b"\n"),
            "sandbox provider did not become ready"
        );
        let ready: Readiness =
            serde_json::from_slice(&document).context("decode provider readiness")?;
        ensure!(ready.protocol == 1, "unsupported sandbox provider protocol");
        let client = reqwest::Client::builder()
            .unix_socket(socket)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self {
            child,
            client,
            _directory: directory,
        })
    }
}

async fn read_response(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut document = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("read provider response; outcome may be unknown")?
    {
        ensure!(
            document.len() + chunk.len() <= MAX_PROVIDER_OUTPUT_BYTES,
            "provider response exceeds {MAX_PROVIDER_OUTPUT_BYTES} bytes"
        );
        document.extend_from_slice(&chunk);
    }
    Ok(document)
}

#[derive(Deserialize)]
struct Readiness {
    protocol: u32,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ProviderResponse<T> {
    Success { result: T },
    Failure { error: String },
}
