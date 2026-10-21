use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::Serialize;
use tracing::warn;

use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    host::HostId,
    sandbox::ResourceLimits,
};

mod pubsub;

#[derive(Clone)]
pub(crate) struct UsageEvents(Arc<dyn UsagePublisher>);

impl UsageEvents {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let publisher: Arc<dyn UsagePublisher> = match url.split_once("://") {
            Some(("pubsub", topic)) => Arc::new(pubsub::PubSubPublisher::connect(topic).await?),
            _ => bail!(
                "DURABLE_ACTORS_USAGE_EVENTS_URL must be pubsub://projects/<project>/topics/<topic>"
            ),
        };
        Ok(Self::new(publisher))
    }

    pub(crate) fn new(publisher: Arc<dyn UsagePublisher>) -> Self {
        Self(publisher)
    }

    pub(crate) fn started(&self, sandbox: SandboxUsage) {
        self.emit(
            sandbox.host_id.clone(),
            None,
            UsageEventKind::Started(sandbox),
        );
    }

    pub(crate) fn ended(&self, host_id: String, finished_at_ms: Option<u64>) {
        self.emit(
            host_id.clone(),
            finished_at_ms,
            UsageEventKind::Ended { host_id },
        );
    }

    fn emit(&self, key: String, occurred_at_ms: Option<u64>, kind: UsageEventKind) {
        let publisher = self.0.clone();
        let payload =
            UsageEvent::new(occurred_at_ms, kind).and_then(|event| Ok(serde_json::to_vec(&event)?));
        tokio::spawn(async move {
            let result = async { publisher.publish(&key, payload?).await }.await;
            if let Err(error) = result {
                warn!(event = "usage_event_dropped", host_id = %key, error = %format!("{error:#}"), "usage event was not published");
            }
        });
    }
}

#[async_trait]
pub(crate) trait UsagePublisher: Send + Sync {
    async fn publish(&self, key: &str, payload: Vec<u8>) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SandboxUsage {
    host_id: String,
    provider: String,
    project_id: String,
    actor_name: String,
    actor_id: String,
    region: String,
    cpu_millis: u32,
    memory_mib: u32,
}

impl SandboxUsage {
    pub(crate) fn new(
        host: &HostId,
        provider: &str,
        actor: &ActorKey,
        region: &str,
        resources: &ResourceLimits,
    ) -> Self {
        Self {
            host_id: host.to_string(),
            provider: provider.into(),
            project_id: actor.project_id.clone(),
            actor_name: actor.actor_name.clone(),
            actor_id: actor.actor_id.clone(),
            region: region.into(),
            cpu_millis: resources.cpu_millis,
            memory_mib: resources.memory_mib,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageEvent {
    event_id: String,
    occurred_at_ms: u64,
    #[serde(flatten)]
    kind: UsageEventKind,
}

impl UsageEvent {
    fn new(occurred_at_ms: Option<u64>, kind: UsageEventKind) -> Result<Self> {
        Ok(Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            occurred_at_ms: match occurred_at_ms {
                Some(at) => at,
                None => SystemClock.now_ms()?,
            },
            kind,
        })
    }
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum UsageEventKind {
    #[serde(rename = "sandbox.started")]
    Started(SandboxUsage),
    #[serde(rename = "sandbox.ended", rename_all = "camelCase")]
    Ended { host_id: String },
}

#[cfg(test)]
#[path = "../tests/support/usage.rs"]
pub(crate) mod testing;

#[cfg(test)]
#[path = "../tests/unit/usage.rs"]
mod tests;
