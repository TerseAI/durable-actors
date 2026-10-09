use super::*;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, ResourceExt};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) struct UsageWorker {
    journal: UsageJournal,
    observer: Arc<dyn UsageObserver>,
    sink: Option<Arc<dyn UsageSink>>,
}

impl UsageWorker {
    pub(crate) fn new(
        journal: UsageJournal,
        observer: Arc<dyn UsageObserver>,
        sink: Option<Arc<dyn UsageSink>>,
    ) -> Self {
        Self {
            journal,
            observer,
            sink,
        }
    }

    pub(crate) fn start(self, stop: CancellationToken) {
        let worker = Arc::new(self);
        for job in [UsageJob::Observe, UsageJob::Deliver] {
            tokio::spawn(worker.clone().run(job, stop.clone()));
        }
    }

    async fn run(self: Arc<Self>, job: UsageJob, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { ()=stop.cancelled()=>break, _=interval.tick()=>{} }
            let result = tokio::select! {
                ()=stop.cancelled()=>break,
                result=async {match job {UsageJob::Observe=>self.observe().await, UsageJob::Deliver=>self.deliver().await}}=>result,
            };
            if let Err(error) = result {
                tracing::warn!(%error,"sandbox usage reconciliation failed");
            }
        }
    }

    async fn observe(&self) -> Result<()> {
        let observations = stream::iter(self.journal.active().await?)
            .map(|assignment| async move {
                let observed_at = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
                let observation = self.observer.observe(&assignment).await?;
                self.journal
                    .observe(&assignment.session_id, observation, observed_at)
                    .await
            })
            .buffer_unordered(16);
        tokio::pin!(observations);
        while let Some(result) = observations.next().await {
            if let Err(error) = result {
                tracing::warn!(%error,"sandbox usage observation unavailable");
            }
        }
        Ok(())
    }

    async fn deliver(&self) -> Result<()> {
        let Some(sink) = &self.sink else {
            return Ok(());
        };
        for _ in 0..10 {
            let pending = self.journal.pending().await?;
            if pending.is_empty() {
                break;
            }
            sink.deliver(&pending).await?;
            self.journal.ack(&pending).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum UsageJob {
    Observe,
    Deliver,
}

#[async_trait]
pub(crate) trait UsageObserver: Send + Sync {
    async fn observe(&self, assignment: &UsageAssignment) -> Result<Observation>;
}

pub(crate) struct KubernetesUsageObserver(pub kube::Client);
#[async_trait]
impl UsageObserver for KubernetesUsageObserver {
    async fn observe(&self, assignment: &UsageAssignment) -> Result<Observation> {
        let parts: Vec<_> = assignment.resource_id.split('/').collect();
        ensure!(parts.len() == 3, "invalid sandbox resource identity");
        let pods: Api<Pod> = Api::namespaced(self.0.clone(), parts[0]);
        let pod = tokio::time::timeout(Duration::from_secs(10), pods.get_opt(parts[1])).await??;
        let Some(pod) = pod.filter(|pod| pod.uid().as_deref() == Some(parts[2])) else {
            return Ok(Observation::Stopped(None));
        };
        let state = pod
            .status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .and_then(|statuses| statuses.iter().find(|status| status.name == "runtime"))
            .and_then(|status| status.state.as_ref());
        if let Some(terminated) = state.and_then(|state| state.terminated.as_ref()) {
            return Ok(Observation::Stopped(
                terminated
                    .finished_at
                    .as_ref()
                    .map(|time| time.0.timestamp_millis()),
            ));
        }
        ensure!(
            state.is_some_and(|state| state.running.is_some()),
            "sandbox running state unavailable"
        );
        Ok(Observation::Running)
    }
}

pub struct HttpUsageSink {
    client: reqwest::Client,
    url: reqwest::Url,
    token: String,
}
impl HttpUsageSink {
    pub fn new(url: &str, token: String) -> Result<Self> {
        let url = reqwest::Url::parse(url)?;
        ensure!(
            url.scheme() == "https" && url.username().is_empty() && url.password().is_none(),
            "usage sink requires an HTTPS URL without credentials"
        );
        ensure!(!token.trim().is_empty(), "usage sink token required");
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            url,
            token,
        })
    }
}
#[async_trait]
impl UsageSink for HttpUsageSink {
    async fn deliver(&self, events: &[UsageInterval]) -> Result<()> {
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(&self.token)
            .json(&serde_json::json!({"events":events}))
            .send()
            .await?
            .error_for_status()
            .context("deliver sandbox usage")?;
        ensure!(
            response.status().is_success(),
            "usage sink returned HTTP {}",
            response.status()
        );
        Ok(())
    }
}

pub(crate) struct HttpUsageAuthorizer {
    sink: HttpUsageSink,
}
impl HttpUsageAuthorizer {
    pub(crate) fn new(url: &str, token: String) -> Result<Self> {
        Ok(Self {
            sink: HttpUsageSink::new(url, token)?,
        })
    }
}
#[async_trait]
impl UsageAuthorizer for HttpUsageAuthorizer {
    async fn authorize(&self, _project_id: &str, billing_account_id: Option<&str>) -> Result<bool> {
        let mut url = self.sink.url.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid authorization URL"))?
            .pop_if_empty()
            .push(
                billing_account_id.context("billing account identity is required for admission")?,
            );
        #[derive(Deserialize)]
        struct Decision {
            allowed: bool,
        }
        let response: Decision = self
            .sink
            .client
            .get(url)
            .bearer_auth(&self.sink.token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(response.allowed)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/usage/worker.rs"]
mod tests;
