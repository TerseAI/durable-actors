use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions},
    runtime::wait::await_condition,
};
use serde_json::json;

use super::*;

// Keeps a metered Pod, and its runtime container's finishedAt, until pool cleanup records the usage stop.
const USAGE_FINALIZER: &str = "terse.ai/sandbox-usage";
// Releases completed Pods whose stop pool cleanup never recorded.
const COMPLETED_POD_RETENTION_MS: i64 = 5 * 60_000;

#[derive(Clone, Debug)]
pub(crate) struct GkeConfig {
    pub namespace: String,
    pub zones: BTreeMap<String, Vec<String>>,
    pub public_origin: String,
}

pub(super) struct Kubernetes {
    pods: Api<Pod>,
    secrets: Api<Secret>,
    config: GkeConfig,
    track_usage: bool,
    _cleanup: tokio_util::task::AbortOnDropHandle<()>,
}
impl Kubernetes {
    pub fn new(client: Client, config: GkeConfig, track_usage: bool) -> Self {
        let pods = Api::namespaced(client.clone(), &config.namespace);
        let cleanup_pods = pods.clone();
        let cleanup = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let reaped = async { reap_completed(&cleanup_pods, now_ms()? as i64).await };
                if let Err(error) = reaped.await {
                    tracing::warn!(%error, "completed pod cleanup failed");
                }
            }
        }));
        Self {
            pods,
            secrets: Api::namespaced(client, &config.namespace),
            config,
            track_usage,
            _cleanup: cleanup,
        }
    }

    async fn start(&self, pod: Pod) -> Result<Pod> {
        let created = self.pods.create(&PostParams::default(), &pod).await?;
        let mut cleanup = PodCleanup::new(self.pods.clone(), &created)?;
        let ready = tokio::time::timeout(
            Duration::from_secs(120),
            await_condition(
                self.pods.clone(),
                &created.name_any(),
                |pod: Option<&Pod>| pod.is_none_or(terminal_or_ready),
            ),
        )
        .await
        .context("sandbox pod readiness timed out")??
        .context("sandbox pod disappeared")?;
        ensure!(
            ready.uid() == created.uid(),
            "sandbox pod was replaced during startup"
        );
        ensure!(is_ready(&ready), "sandbox pod failed: {:?}", ready.status);
        cleanup.disarm();
        Ok(ready)
    }

    fn zones(&self, region: &str) -> Result<&[String]> {
        self.config
            .zones
            .get(region)
            .map(Vec::as_slice)
            .with_context(|| format!("no GKE zone configured for {region}"))
    }

    fn identity<'a>(&self, spare: &'a SpareHandle) -> Result<(&'a str, &'a str)> {
        let (namespace, name, uid) = resource_identity(&spare.resource_id)?;
        ensure!(
            namespace == self.config.namespace && name == spare.name,
            "sandbox resource identity mismatch"
        );
        Ok((name, uid))
    }
}

#[async_trait]
impl SandboxCluster for Kubernetes {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let pod = spare_pod(
            request,
            self.zones(&request.canonical_region)?,
            &token,
            self.track_usage,
        )?;
        let ready = self.start(pod).await?;
        let ip: std::net::IpAddr = ready
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
            .context("ready pod has no IP")?
            .parse()?;
        let authority = if ip.is_ipv6() {
            format!("[{ip}]")
        } else {
            ip.to_string()
        };
        Ok(SpareHandle {
            name: ready.name_any(),
            resource_id: format!(
                "{}/{}/{}",
                self.config.namespace,
                ready.name_any(),
                ready.uid().context("pod UID missing")?
            ),
            control_route: format!("http://{authority}:7102"),
            control_token: token,
            route: format!("http://{authority}:7101"),
            canonical_region: request.canonical_region.clone(),
        })
    }

    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        if spare.resource_id.is_empty() {
            let Some(pod) = self.pods.get_opt(&spare.name).await? else {
                return Ok(());
            };
            ensure!(
                pod.labels()
                    .get("app.kubernetes.io/managed-by")
                    .is_some_and(|owner| owner == "terse"),
                "refusing to retire an unmanaged pod"
            );
            return discard_pod(
                &self.pods,
                &spare.name,
                &pod.uid().context("pod UID missing")?,
            )
            .await;
        }
        let (name, uid) = self.identity(spare)?;
        discard_pod(&self.pods, name, uid).await
    }

    async fn stop_spare(&self, spare: &SpareHandle) -> Result<()> {
        let (name, uid) = self.identity(spare)?;
        delete_pod(&self.pods, name, uid).await
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<StoppedSpare>> {
        let pods: HashMap<_, _> = self
            .pods
            .list(&ListParams::default())
            .await?
            .items
            .into_iter()
            .map(|pod| (pod.name_any(), pod))
            .collect();
        let mut stopped = Vec::new();
        for spare in spares {
            let (name, uid) = self.identity(spare)?;
            let pod = pods
                .get(name)
                .filter(|pod| pod.uid().as_deref() == Some(uid));
            if pod.is_none_or(has_exited) {
                stopped.push(StoppedSpare {
                    resource_id: spare.resource_id.clone(),
                    stopped_at_ms: pod.and_then(finished_at_ms),
                });
            }
        }
        Ok(stopped)
    }

    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>> {
        let loaded =
            futures_util::future::try_join_all(names.iter().map(|name| self.secrets.get(name)))
                .await?;
        let mut environment = HashMap::new();
        for secret in loaded {
            ensure!(
                secret
                    .labels()
                    .get("terse.ai/customer-secret")
                    .is_some_and(|value| value == "true"),
                "secret is not marked for customer code"
            );
            for (name, value) in secret.data.unwrap_or_default() {
                ensure!(
                    !environment.contains_key(&name),
                    "duplicate customer environment variable {name}"
                );
                environment.insert(
                    name,
                    String::from_utf8(value.0).context("customer secret must be UTF-8")?,
                );
            }
        }
        Ok(environment)
    }
}

async fn reap_completed(pods: &Api<Pod>, now_ms: i64) -> Result<()> {
    for phase in ["Failed", "Succeeded"] {
        let params = ListParams::default()
            .labels("app.kubernetes.io/managed-by=terse")
            .fields(&format!("status.phase={phase}"));
        for pod in pods.list(&params).await? {
            if finished_at_ms(&pod)
                .is_some_and(|finished| now_ms - finished < COMPLETED_POD_RETENTION_MS)
            {
                continue;
            }
            discard_pod(
                pods,
                &pod.name_any(),
                &pod.uid().context("pod UID missing")?,
            )
            .await?;
        }
    }
    Ok(())
}

fn spare_pod(
    request: &CreateSpareRequest,
    zones: &[String],
    token: &str,
    track_usage: bool,
) -> Result<Pod> {
    let mut pod = base_pod(&request.name, &request.image_ref, zones, &request.resources);
    pod["metadata"]["annotations"] =
        json!({"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"});
    if track_usage {
        pod["metadata"]["finalizers"] = json!([USAGE_FINALIZER]);
    }
    pod["spec"]["containers"][0]["env"] = json!([
        {"name":"DURABLE_ACTORS_PROCESS_ROLE", "value":"spare"},
        {"name":"DURABLE_ACTORS_SPARE_TOKEN", "value":token},
        {"name":"DURABLE_ACTORS_CONTROL_PLANE_URL", "value":request.control_plane_url.as_ref().context("spare control plane URL required")?}
    ]);
    pod["spec"]["containers"][0]["readinessProbe"] = json!({"exec":{"command":["/usr/bin/test", "-f", "/tmp/durable-actors-spare-ready"]}, "periodSeconds":1, "failureThreshold":3});
    Ok(serde_json::from_value(pod)?)
}

fn base_pod(
    name: &str,
    image: &str,
    zones: &[String],
    resources: &ResourceLimits,
) -> serde_json::Value {
    json!({
        "apiVersion":"v1", "kind":"Pod", "metadata":{"name":name, "labels":{"app.kubernetes.io/managed-by":"terse", "terse.ai/purpose":"actor"}},
        "spec":{
            "runtimeClassName":"gvisor", "automountServiceAccountToken":false, "serviceAccountName":"sandbox",
            "restartPolicy":"Never", "terminationGracePeriodSeconds":45,
            "nodeSelector":{"sandbox.gke.io/runtime":"gvisor"},
            "affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/zone","operator":"In","values":zones}]}]}}},
            "topologySpreadConstraints":[{"maxSkew":1,"topologyKey":"topology.kubernetes.io/zone","whenUnsatisfiable":"ScheduleAnyway","labelSelector":{"matchLabels":{"terse.ai/purpose":"actor"}}}],
            "securityContext":{"runAsNonRoot":true, "runAsUser":10000, "runAsGroup":10000, "fsGroup":10000},
            "containers":[{"name":"runtime", "image":image, "imagePullPolicy":"IfNotPresent", "command":["/usr/local/bin/durable-actors"],
                "securityContext":{"allowPrivilegeEscalation":false, "readOnlyRootFilesystem":true, "capabilities":{"drop":["ALL"]}},
                "resources":{"requests":{"ephemeral-storage":"1Gi","cpu":format!("{}m", resources.cpu_millis),"memory":format!("{}Mi", resources.memory_mib)},"limits":{"ephemeral-storage":"8Gi","cpu":format!("{}m", resources.cpu_millis),"memory":format!("{}Mi", resources.memory_mib)}},
                "volumeMounts":[{"name":"tmp","mountPath":"/tmp"},{"name":"customer","mountPath":"/customer"}]
            }], "volumes":[{"name":"tmp","emptyDir":{"sizeLimit":"7Gi"}},{"name":"customer","emptyDir":{"sizeLimit":"1Gi"}}]
        }
    })
}

fn terminal_or_ready(pod: &Pod) -> bool {
    is_ready(pod) || is_terminal(pod)
}
fn is_terminal(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .is_some_and(|phase| matches!(phase, "Failed" | "Succeeded"))
}
fn has_exited(pod: &Pod) -> bool {
    is_terminal(pod) || finished_at_ms(pod).is_some()
}
fn finished_at_ms(pod: &Pod) -> Option<i64> {
    let finished_at = pod
        .status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|status| status.name == "runtime")?
        .state
        .as_ref()?
        .terminated
        .as_ref()?
        .finished_at
        .as_ref()?;
    Some(finished_at.0.timestamp_millis())
}
fn is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}
fn resource_identity(resource: &str) -> Result<(&str, &str, &str)> {
    let mut parts = resource.split('/');
    let namespace = parts.next().context("namespace missing")?;
    let name = parts.next().context("pod name missing")?;
    let uid = parts.next().context("pod UID missing")?;
    ensure!(
        !namespace.is_empty() && !name.is_empty() && !uid.is_empty() && parts.next().is_none(),
        "invalid pod identity"
    );
    Ok((namespace, name, uid))
}
async fn discard_pod(pods: &Api<Pod>, name: &str, uid: &str) -> Result<()> {
    release_pod(pods, name, uid).await?;
    delete_pod(pods, name, uid).await
}
async fn release_pod(pods: &Api<Pod>, name: &str, uid: &str) -> Result<()> {
    let patch = serde_json::from_value(json!([
        {"op":"test","path":"/metadata/uid","value":uid},
        {"op":"add","path":"/metadata/finalizers","value":[]}
    ]))?;
    match pods
        .patch(name, &PatchParams::default(), &Patch::Json::<()>(patch))
        .await
    {
        Ok(_) => Ok(()),
        // 422 means the UID test failed: the name now belongs to a replacement Pod.
        Err(kube::Error::Api(error)) if matches!(error.code, 404 | 422) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
async fn delete_pod(pods: &Api<Pod>, name: &str, uid: &str) -> Result<()> {
    let params = DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(uid.into()),
            resource_version: None,
        }),
        ..DeleteParams::default()
    };
    match pods.delete(name, &params).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(error)) if error.code == 404 => Ok(()),
        Err(error) => Err(error.into()),
    }
}
struct PodCleanup {
    pods: Api<Pod>,
    identity: Option<(String, String)>,
}
impl PodCleanup {
    fn new(pods: Api<Pod>, pod: &Pod) -> Result<Self> {
        Ok(Self {
            pods,
            identity: Some((pod.name_any(), pod.uid().context("pod UID missing")?)),
        })
    }
    fn disarm(&mut self) {
        self.identity = None;
    }
}
impl Drop for PodCleanup {
    fn drop(&mut self) {
        if let Some((name, uid)) = self.identity.take() {
            let pods = self.pods.clone();
            tokio::spawn(async move {
                if let Err(error) = discard_pod(&pods, &name, &uid).await {
                    tracing::warn!(%error, %name, "sandbox cleanup failed");
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/kubernetes.rs"]
mod tests;
