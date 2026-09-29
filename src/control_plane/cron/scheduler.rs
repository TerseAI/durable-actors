use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::{
    admin::AdminRegistry,
    service::{ControlPlaneService, TargetResolutionTimings},
};
use super::*;
use crate::clock::Clock;

#[async_trait]
pub(in crate::control_plane) trait CronDispatcher: Send + Sync {
    async fn dispatch(&self, occurrence: &Occurrence) -> Result<CronOutcome>;
}

pub(in crate::control_plane) struct CronScheduler {
    store: Arc<dyn CronStore>,
    registry: Arc<dyn AdminRegistry>,
    dispatcher: Arc<dyn CronDispatcher>,
    clock: Arc<dyn Clock>,
}

impl CronScheduler {
    pub(in crate::control_plane) fn new(
        store: Arc<dyn CronStore>,
        registry: Arc<dyn AdminRegistry>,
        dispatcher: Arc<dyn CronDispatcher>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            registry,
            dispatcher,
            clock,
        }
    }

    pub(in crate::control_plane) fn start(self, stop: CancellationToken) {
        let scheduler = Arc::new(self);
        tokio::spawn(async move {
            tokio::join!(
                scheduler.clone().run(stop.clone()),
                scheduler.reconcile_loop(stop)
            );
        });
    }

    async fn run(self: Arc<Self>, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                    if let Err(error) = result { warn!(%error, "cron task failed"); }
                }
                _ = interval.tick() => {
                    let result = tokio::select! {
                        _ = stop.cancelled() => break,
                        result = self.schedule(&mut jobs) => result,
                    };
                    if let Err(error) = result {
                        warn!(error = %format!("{error:#}"), "cron scheduling failed");
                    }
                }
            }
        }
        jobs.abort_all();
        while jobs.join_next().await.is_some() {}
    }

    async fn schedule(self: &Arc<Self>, jobs: &mut JoinSet<()>) -> Result<()> {
        let capacity = 32usize.saturating_sub(jobs.len());
        if capacity == 0 {
            return Ok(());
        }
        for occurrence in self.store.claim(self.now()?, capacity as i64).await? {
            let scheduler = self.clone();
            jobs.spawn(async move {
                if let Err(error) = scheduler.execute(&occurrence).await {
                    warn!(error = %format!("{error:#}"), cron_id = %occurrence.id, "cron execution interrupted; lease will expire");
                }
            });
        }
        Ok(())
    }

    async fn reconcile_loop(&self, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = interval.tick() => {
                    let result = tokio::select! {
                        _ = stop.cancelled() => break,
                        result = self.reconcile() => result,
                    };
                    if let Err(error) = result {
                        warn!(error = %format!("{error:#}"), "cron reconciliation failed");
                    }
                }
            }
        }
    }

    pub(in crate::control_plane) async fn reconcile(&self) -> Result<()> {
        if !self.store.claim_reconciliation(self.now()?).await? {
            return Ok(());
        }
        let mut projects: BTreeSet<_> = self.store.projects().await?.into_iter().collect();
        projects.extend(
            self.registry
                .launch_specs()
                .await?
                .into_iter()
                .map(|spec| spec.project_id),
        );
        for project in projects {
            let _update = self.registry.lock_deployment(&project).await?;
            if self.registry.launch_spec(&project).await?.is_none() {
                self.store.remove_project(&project).await?;
                continue;
            }
            let definitions = self
                .registry
                .deployment_contract(&project)
                .await?
                .map(|contract| contract.crons())
                .transpose()?
                .unwrap_or_default();
            self.store
                .publish(&project, &definitions, self.now()?)
                .await?;
        }
        Ok(())
    }

    pub(in crate::control_plane) async fn execute(&self, occurrence: &Occurrence) -> Result<()> {
        if !self.store.renew(occurrence, self.now()?).await? {
            return Ok(());
        }
        let dispatch = tokio::time::timeout(
            Duration::from_secs(900),
            self.dispatcher.dispatch(occurrence),
        );
        tokio::pin!(dispatch);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
        heartbeat.tick().await;
        let outcome = loop {
            tokio::select! {
                result = &mut dispatch => break result.context("cron execution exceeded 15 minutes").and_then(|result| result),
                _ = heartbeat.tick() => {
                    if !self.store.renew(occurrence, self.now()?).await? { return Ok(()); }
                }
            }
        };
        let outcome = outcome.unwrap_or_else(|error| {
            warn!(error = %format!("{error:#}"), actor_id = %occurrence.actor.actor_id, method = %occurrence.method,
                scheduled_time = occurrence.scheduled_time, attempt = occurrence.attempt, "cron delivery unconfirmed; redelivery scheduled");
            CronOutcome::DeliveryFailed
        });
        self.store.finish(occurrence, self.now()?, outcome).await?;
        info!(cron_id = %occurrence.id, scheduled_time = occurrence.scheduled_time, ?outcome, "cron attempt finished");
        Ok(())
    }

    fn now(&self) -> Result<i64> {
        Ok(i64::try_from(self.clock.now_ms()?)?)
    }
}

pub(in crate::control_plane) struct HttpCronDispatcher {
    service: ControlPlaneService,
    client: reqwest::Client,
}

impl HttpCronDispatcher {
    pub(in crate::control_plane) fn new(service: ControlPlaneService) -> Result<Self> {
        Ok(Self {
            service,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(900))
                .build()?,
        })
    }
}

#[async_trait]
impl CronDispatcher for HttpCronDispatcher {
    async fn dispatch(&self, occurrence: &Occurrence) -> Result<CronOutcome> {
        let mut service = self.service.clone();
        // A saved instance's region survives changes to the deployment's default region.
        service.region = Some(occurrence.region.clone());
        let target = service
            .resolve_actor_target_timed(
                &occurrence.actor,
                Some(&occurrence.region),
                &mut TargetResolutionTimings::new(),
                None,
            )
            .await?;
        let invocation = crate::actor::ActorInvocation {
            actor: occurrence.actor.clone(),
            request_id: occurrence.request_id(),
            method: occurrence.method.clone(),
            args: vec![
                serde_json::json!({"cron": occurrence.expression, "scheduledTime": occurrence.scheduled_time}),
            ],
        };
        let outcome = super::super::invocation::dispatch(
            &self.client,
            &target.route,
            &target.token,
            target.owner_epoch,
            &invocation,
        )
        .await
        .map_err(|_| anyhow::anyhow!("cron invocation outcome is unknown"))?;
        match outcome["type"].as_str() {
            Some("completed") => Ok(CronOutcome::Completed),
            Some("failed") if outcome["code"] == "actor_method_failed" => {
                warn!(actor_id = %occurrence.actor.actor_id, method = %occurrence.method,
                    scheduled_time = occurrence.scheduled_time, message = %outcome["message"], "cron handler failed");
                Ok(CronOutcome::HandlerFailed)
            }
            _ => anyhow::bail!("cron delivery unconfirmed: {outcome}"),
        }
    }
}
