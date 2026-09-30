use super::*;
use crate::sandbox::gke::source_builds::{BuildWorkers, WorkerReply, WorkerRequest};
use kube::api::{Patch, PatchParams};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub(crate) struct BuilderConfig {
    pub idle: u32,
    pub concurrent: u32,
    pub resources: ResourceLimits,
}

pub(crate) struct WorkerPool {
    cluster: Arc<Kubernetes>,
    image: String,
    owner: String,
    config: BuilderConfig,
    slots: Semaphore,
    http: reqwest::Client,
}

impl WorkerPool {
    pub(in crate::sandbox::gke) fn new(
        cluster: Arc<Kubernetes>,
        image: String,
        config: BuilderConfig,
    ) -> Result<Arc<Self>> {
        super::super::validate_image(&image)?;
        Ok(Arc::new(Self {
            cluster,
            image,
            owner: uuid::Uuid::new_v4().simple().to_string(),
            slots: Semaphore::new(config.concurrent as usize),
            config,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(660))
                .connect_timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        }))
    }

    pub(in crate::sandbox::gke) fn start(self: &Arc<Self>, stop: CancellationToken) {
        let pool = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    result = pool.replenish() => if let Err(error) = result {
                        tracing::warn!(%error, "source build worker replenishment failed");
                    }
                }
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        });
    }

    async fn replenish(&self) -> Result<()> {
        let pods = self.idle_pods().await?;
        for zone in self.cluster.config.zones.values() {
            let available = pods
                .iter()
                .filter(|pod| worker_zone(pod) == Some(zone.as_str()) && worker_fresh(pod))
                .count();
            for _ in available..self.config.idle as usize {
                self.cluster
                    .pods
                    .create(&PostParams::default(), &self.pod(zone)?)
                    .await?;
            }
        }
        for pod in pods.iter().filter(|pod| !worker_fresh(pod)) {
            self.delete_observed(pod).await?;
        }
        let workers = self
            .cluster
            .pods
            .list(&ListParams::default().labels("terse.ai/purpose=build,terse.ai/build-owner"))
            .await?;
        for pod in workers.items {
            if worker_finished(&pod) || worker_age(&pod).is_some_and(|age| age >= 900) {
                self.delete_observed(&pod).await?;
            }
        }
        Ok(())
    }

    async fn delete_observed(&self, pod: &Pod) -> Result<()> {
        let delete = DeleteParams {
            preconditions: Some(Preconditions {
                uid: pod.uid(),
                resource_version: pod.resource_version(),
            }),
            ..Default::default()
        };
        match self.cluster.pods.delete(&pod.name_any(), &delete).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(error)) if matches!(error.code, 404 | 409) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn idle_pods(&self) -> Result<Vec<Pod>> {
        Ok(self
            .cluster
            .pods
            .list(&ListParams::default().labels(&format!(
                "terse.ai/build-owner={},terse.ai/build-state=idle",
                self.owner
            )))
            .await?
            .items)
    }

    async fn reserve(&self, region: &str) -> Result<(Pod, bool)> {
        let zone = self.cluster.zone(region)?;
        for pod in self.idle_pods().await? {
            if worker_zone(&pod) != Some(zone) || !worker_fresh(&pod) || !is_ready(&pod) {
                continue;
            }
            let patch = json!({"metadata": {"resourceVersion": pod.resource_version().context("worker resource version missing")?, "labels": {"terse.ai/build-state": "busy"}}});
            match self
                .cluster
                .pods
                .patch(
                    &pod.name_any(),
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                )
                .await
            {
                Ok(claimed) => return Ok((claimed, true)),
                Err(kube::Error::Api(error)) if matches!(error.code, 404 | 409) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        let mut pod = self.pod(zone)?;
        pod.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("terse.ai/build-state".into(), "busy".into());
        Ok((self.cluster.start(pod).await?, false))
    }

    fn pod(&self, zone: &str) -> Result<Pod> {
        worker_pod(&self.image, zone, &self.owner, &self.config.resources)
    }

    async fn execute(&self, pod: &Pod, request: &WorkerRequest) -> Result<WorkerReply> {
        let address: std::net::IpAddr = pod
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
            .context("worker IP missing")?
            .parse()?;
        let token = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .and_then(|container| container.env.as_ref())
            .and_then(|env| {
                env.iter()
                    .find(|item| item.name == "DURABLE_ACTORS_BUILD_TOKEN")
            })
            .and_then(|env| env.value.as_deref())
            .context("worker token missing")?;
        let endpoint = std::net::SocketAddr::new(address, 7102);
        let mut response = self
            .http
            .post(format!("http://{endpoint}/build"))
            .bearer_auth(token)
            .json(request)
            .send()
            .await?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            bytes.extend_from_slice(&chunk);
            ensure!(
                bytes.len() <= 4 * 1024 * 1024,
                "build worker response exceeds the size limit"
            );
        }
        if !status.is_success() {
            let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            anyhow::bail!(
                "actor source build failed: {}",
                error["error"].as_str().unwrap_or("worker request failed")
            );
        }
        serde_json::from_slice(&bytes).context("decode source build result")
    }
}

#[async_trait]
impl BuildWorkers for WorkerPool {
    async fn build(&self, region: &str, request: &WorkerRequest) -> Result<WorkerReply> {
        let _slot = tokio::time::timeout(Duration::from_secs(60), self.slots.acquire())
            .await
            .context("source build queue is full; retry shortly")??;
        let started = std::time::Instant::now();
        let (pod, reused) = self.reserve(region).await?;
        let cleanup = PodCleanup::new(self.cluster.pods.clone(), &pod)?;
        tracing::info!(worker = %pod.name_any(), reused, ready_ms = started.elapsed().as_millis() as u64, "source build worker reserved");
        let result = self.execute(&pod, request).await;
        if let Err(error) = cleanup.delete().await {
            tracing::warn!(%error, "source build worker cleanup failed");
        }
        result
    }
}

fn worker_pod(image: &str, zone: &str, owner: &str, resources: &ResourceLimits) -> Result<Pod> {
    let mut pod = base_pod(
        &format!("do-source-build-{}", uuid::Uuid::new_v4()),
        image,
        zone,
        resources,
    );
    pod["metadata"]["labels"]["terse.ai/purpose"] = json!("build");
    pod["metadata"]["labels"]["terse.ai/build-owner"] = json!(owner);
    pod["metadata"]["labels"]["terse.ai/build-state"] = json!("idle");
    pod["spec"]["activeDeadlineSeconds"] = json!(900);
    pod["spec"]["terminationGracePeriodSeconds"] = json!(0);
    let container = &mut pod["spec"]["containers"][0];
    container["command"] = json!(["python3", "/opt/durable-actors/source-build.py"]);
    container["env"] = json!([{"name": "DURABLE_ACTORS_BUILD_TOKEN", "value": uuid::Uuid::new_v4().simple().to_string()}]);
    container["readinessProbe"] = json!({"httpGet": {"path": "/ready", "port": 7102}, "periodSeconds": 1, "timeoutSeconds": 1});
    Ok(serde_json::from_value(pod)?)
}

fn worker_zone(pod: &Pod) -> Option<&str> {
    pod.spec
        .as_ref()?
        .node_selector
        .as_ref()?
        .get("topology.kubernetes.io/zone")
        .map(String::as_str)
}

fn worker_fresh(pod: &Pod) -> bool {
    !worker_finished(pod) && worker_age(pod).is_some_and(|age| age < 240)
}

fn worker_finished(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .is_some_and(|phase| matches!(phase, "Failed" | "Succeeded"))
}

fn worker_age(pod: &Pod) -> Option<u64> {
    let created = pod.metadata.creation_timestamp.as_ref()?.0.timestamp();
    Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs()
            .saturating_sub(created.max(0) as u64),
    )
}

#[cfg(test)]
#[path = "../../../../tests/unit/sandbox/build_workers.rs"]
mod tests;
