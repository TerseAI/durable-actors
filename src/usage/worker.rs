use super::*;
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) struct UsageRelay {
    outbox: UsageOutbox,
    sink: Arc<dyn UsageSink>,
}

impl UsageRelay {
    pub(crate) fn new(outbox: UsageOutbox, sink: Arc<dyn UsageSink>) -> Self {
        Self { outbox, sink }
    }

    pub(crate) fn start(self, stop: CancellationToken) {
        tokio::spawn(self.run(stop));
    }

    async fn run(self, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { ()=stop.cancelled()=>break, _=interval.tick()=>{} }
            let result = tokio::select! {
                ()=stop.cancelled()=>break,
                result=self.deliver()=>result,
            };
            if let Err(error) = result {
                tracing::warn!(%error,"sandbox usage publication failed");
            }
        }
    }

    async fn deliver(&self) -> Result<()> {
        for _ in 0..10 {
            let pending = self.outbox.pending().await?;
            if pending.is_empty() {
                break;
            }
            self.sink.deliver(&pending).await?;
            self.outbox.ack(&pending).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/usage/worker.rs"]
mod tests;
