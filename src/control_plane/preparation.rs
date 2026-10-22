use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;
use tokio_util::{sync::CancellationToken, task::JoinMap};

use super::service::ControlPlaneService;
use crate::sandbox::SandboxDeployment;

const RETRY_INTERVAL: Duration = Duration::from_secs(30);
const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

pub(super) fn start(
    service: ControlPlaneService,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut changes = service.changes.subscribe();
        let mut interval = tokio::time::interval(RETRY_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut worker = Preparation::default();
        let mut dirty = true;
        let mut reconcile_at = Instant::now();
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return,
                Some((deployment, result)) = worker.jobs.join_next(), if !worker.jobs.is_empty() => {
                    let delay = match result {
                        Ok(Ok(())) => Some(REFRESH_INTERVAL),
                        Err(error) if error.is_cancelled() => None,
                        result => {
                            tracing::warn!(event = "deployment_preparation_failed", project_id = %deployment.project_id, error = ?result, "background preparation failed; GCS delivery remains available");
                            Some(RETRY_INTERVAL)
                        }
                    };
                    if let Some(delay) = delay {
                        worker.next.insert(deployment, Instant::now() + delay);
                    }
                    dirty = true;
                },
                _ = tokio::time::sleep_until(reconcile_at), if dirty => {
                    dirty = false;
                    let result = tokio::select! {
                        _ = stop.cancelled() => return,
                        result = worker.reconcile(&service) => result,
                    };
                    if let Err(error) = result {
                        tracing::warn!(%error, "deployment preparation reconciliation failed");
                    }
                    reconcile_at = Instant::now() + Duration::from_secs(1);
                },
                _ = changes.changed() => { dirty = true; },
                _ = interval.tick() => { dirty = true; },
            }
        }
    })
}

#[derive(Default)]
struct Preparation {
    jobs: JoinMap<SandboxDeployment, Result<()>>,
    next: HashMap<SandboxDeployment, Instant>,
}

impl Preparation {
    async fn reconcile(&mut self, service: &ControlPlaneService) -> Result<()> {
        // Registered deployments are the durable queue, including after a controller restart.
        let desired: HashSet<_> = service
            .registry
            .launch_specs()
            .await?
            .into_iter()
            .filter(|spec| spec.source.is_none() && spec.code_snapshot.is_some())
            .map(|spec| SandboxDeployment {
                project_id: spec.project_id,
                image_ref: spec.image_ref,
                code_snapshot: spec.code_snapshot,
                canonical_region: service.default_region().into(),
            })
            .collect();
        self.jobs.abort_matching(|key| !desired.contains(key));
        self.next.retain(|key, _| desired.contains(key));
        for deployment in desired {
            if self.jobs.contains_key(&deployment)
                || self
                    .next
                    .get(&deployment)
                    .is_some_and(|next| *next > Instant::now())
            {
                continue;
            }
            let service = service.clone();
            self.jobs.spawn(deployment.clone(), async move {
                service.prepare_registered_deployment(&deployment).await
            });
        }
        Ok(())
    }
}
