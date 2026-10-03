use super::*;
use crate::clock::{Clock, SystemClock};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub(in crate::control_plane) struct AlarmScheduler {
    store: Arc<dyn AlarmStore>,
    service: super::super::ControlPlaneService,
    client: reqwest::Client,
}
impl AlarmScheduler {
    pub(in crate::control_plane) fn start(
        store: Arc<dyn AlarmStore>,
        service: super::super::ControlPlaneService,
        stop: CancellationToken,
    ) -> Result<()> {
        let scheduler = Arc::new(Self {
            store,
            service,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(900))
                .build()?,
        });
        tokio::spawn(scheduler.run(stop));
        Ok(())
    }
    async fn run(self: Arc<Self>, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                    if let Err(error) = result {tracing::warn!(%error, "alarm task failed");}
                }
                _ = interval.tick(), if jobs.len()<32 => {
                    match self.store.claim(now().unwrap_or(0), (32-jobs.len()) as i64).await {
                        Ok(deliveries) => for delivery in deliveries {
                            let scheduler = self.clone();
                            jobs.spawn(async move {
                                if let Err(error) = scheduler.execute(&delivery).await {tracing::warn!(%error, generation=%delivery.alarm.generation,"alarm delivery interrupted; lease will expire");}
                            });
                        },
                        Err(error) => tracing::warn!(%error,"alarm scheduling failed"),
                    }
                }
            }
        }
        jobs.abort_all();
        while jobs.join_next().await.is_some() {}
    }
    async fn execute(&self, delivery: &Delivery) -> Result<()> {
        let dispatch = self.dispatch(delivery);
        tokio::pin!(dispatch);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
        let completed = loop {
            tokio::select! {
                outcome = &mut dispatch => match outcome {
                    Ok(completed) => break completed,
                    Err(error) => {tracing::warn!(%error,generation=%delivery.alarm.generation,"alarm delivery will retry");break false;}
                },
                _ = heartbeat.tick() => if !self.store.renew(delivery,now()?).await? {return Ok(());}
            }
        };
        self.store.finish(delivery, now()?, completed).await?;
        Ok(())
    }
    async fn dispatch(&self, delivery: &Delivery) -> Result<bool> {
        let target = self
            .service
            .resolve_alarm_target(&delivery.actor, &delivery.region)
            .await?;
        let invocation = crate::actor::ActorInvocation {
            actor: delivery.actor.clone(),
            request_id: format!("alarm.{}", delivery.alarm.generation),
            method: "__alarm".into(),
            args: vec![serde_json::json!(delivery.alarm.generation)],
        };
        let outcome = super::super::invocation::dispatch(
            &self.client,
            &target.route,
            &target.token,
            target.owner_epoch,
            &invocation,
        )
        .await
        .map_err(|_| anyhow::anyhow!("alarm invocation outcome unknown"))?;
        Ok(outcome["type"] == "completed")
    }
}
fn now() -> Result<i64> {
    Ok(i64::try_from(SystemClock.now_ms()?)?)
}
