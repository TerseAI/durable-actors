use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use futures_util::StreamExt;
use moka::{future::Cache, ops::compute::Op};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{actor::ActorKey, host::HostId, placement::ObjectPlacement, redis_client::Redis};

#[derive(Clone)]
pub(super) struct RouteCache {
    entries: Cache<ActorKey, CachedPlacement>,
    connected: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    redis: Option<Redis>,
    channel: String,
    instance: String,
}

impl Default for RouteCache {
    fn default() -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(100_000)
                .support_invalidation_closures()
                .build(),
            connected: Arc::new(AtomicBool::new(true)),
            generation: Arc::new(AtomicU64::new(0)),
            redis: None,
            channel: String::new(),
            instance: uuid::Uuid::new_v4().to_string(),
        }
    }
}

impl RouteCache {
    pub(super) fn with_redis(
        mut self,
        redis: Redis,
        channel: &str,
        stop: CancellationToken,
    ) -> Self {
        self.redis = Some(redis);
        self.channel = channel.into();
        self.connected.store(false, Ordering::SeqCst);
        let cache = self.clone();
        tokio::spawn(async move { cache.listen(stop).await });
        self
    }

    pub(super) async fn get(&self, actor: &ActorKey) -> Option<CachedRoute> {
        if !self.connected.load(Ordering::SeqCst) {
            return None;
        }
        self.entries.get(actor).await.map(|entry| entry.route)
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub(super) async fn remember(&self, actor: &ActorKey, route: CachedRoute, generation: u64) {
        if !self.connected.load(Ordering::SeqCst) || generation != self.generation() {
            return;
        }
        self.entries
            .entry(actor.clone())
            .and_compute_with(|current| {
                let route = route.clone();
                async move {
                    if current.as_ref().is_some_and(|entry| {
                        entry.value().generation > generation
                            || entry.value().route.placement.owner_epoch
                                > route.placement.owner_epoch
                    }) {
                        Op::Nop
                    } else {
                        Op::Put(CachedPlacement { route, generation })
                    }
                }
            })
            .await;
        if generation != self.generation() {
            self.entries
                .entry(actor.clone())
                .and_compute_with(|entry| async move {
                    if entry.is_some_and(|entry| entry.value().generation == generation) {
                        Op::Remove
                    } else {
                        Op::Nop
                    }
                })
                .await;
        }
    }

    pub(super) async fn invalidate(&self, identity: RouteIdentity) {
        self.broadcast(Invalidation::Session { identity }).await;
    }

    pub(super) async fn invalidate_route(&self, actor: &ActorKey, route: &str, epoch: u64) {
        if let Some(cached) = self.get(actor).await {
            let placement = cached.placement;
            if placement.owner_epoch == epoch
                && placement.lease.route.trim_end_matches('/') == route.trim_end_matches('/')
            {
                self.invalidate(RouteIdentity::new(actor, &placement)).await;
            }
        }
    }

    pub(super) async fn invalidate_project(&self, project: &str) {
        self.broadcast(Invalidation::Project {
            project: project.into(),
        })
        .await;
    }

    async fn broadcast(&self, event: Invalidation) {
        self.apply(&event).await;
        if let Some(redis) = &self.redis {
            let notification = Notification {
                instance: self.instance.clone(),
                event,
            };
            let mut command = redis::cmd("PUBLISH");
            command.arg(&self.channel).arg(
                serde_json::to_vec(&notification).expect("route invalidation is serializable"),
            );
            if let Err(error) = redis.run::<u64>(command).await {
                tracing::warn!(%error, "route invalidation could not be published");
            }
        }
    }

    async fn apply(&self, event: &Invalidation) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        match event {
            Invalidation::Session { identity } => {
                self.entries
                    .entry(identity.actor.clone())
                    .and_compute_with(|entry| async move {
                        if entry.is_some_and(|entry| {
                            RouteIdentity::new(&identity.actor, &entry.value().route.placement)
                                == *identity
                        }) {
                            Op::Remove
                        } else {
                            Op::Nop
                        }
                    })
                    .await;
            }
            Invalidation::Project { project } => {
                let project = project.clone();
                self.entries
                    .invalidate_entries_if(move |actor, _| actor.project_id == project)
                    .expect("invalidation closures enabled");
            }
        }
    }

    async fn listen(&self, stop: CancellationToken) {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                result = self.subscribe(&stop) => {
                    if let Err(error) = result { tracing::warn!(%error, "route invalidation subscription lost"); }
                }
            }
            self.connected.store(false, Ordering::SeqCst);
            self.generation.fetch_add(1, Ordering::SeqCst);
            self.entries.invalidate_all();
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }
        self.connected.store(false, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.entries.invalidate_all();
    }

    async fn subscribe(&self, stop: &CancellationToken) -> anyhow::Result<()> {
        let mut subscription = self.redis.as_ref().unwrap().pubsub().await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            subscription.subscribe(&self.channel),
        )
        .await??;
        self.connected.store(true, Ordering::SeqCst);
        let mut messages = subscription.on_message();
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                message = messages.next() => {
                    let message = message.ok_or_else(|| anyhow::anyhow!("route invalidation stream ended"))?;
                    let notification: Notification = serde_json::from_slice(message.get_payload_bytes())?;
                    if notification.instance != self.instance { self.apply(&notification.event).await; }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct CachedRoute {
    pub placement: ObjectPlacement,
    pub host_config_key: String,
    pub overrides_regions: bool,
}

#[derive(Clone)]
struct CachedPlacement {
    route: CachedRoute,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RouteIdentity {
    pub actor: ActorKey,
    pub host: HostId,
    pub session: String,
    pub epoch: u64,
}

impl RouteIdentity {
    pub(super) fn new(actor: &ActorKey, placement: &ObjectPlacement) -> Self {
        Self {
            actor: actor.clone(),
            host: placement.owner.clone(),
            session: placement.lease.session_id.clone(),
            epoch: placement.owner_epoch,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Notification {
    instance: String,
    event: Invalidation,
}

#[derive(Serialize, Deserialize)]
enum Invalidation {
    Session { identity: RouteIdentity },
    Project { project: String },
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/routes.rs"]
mod tests;
