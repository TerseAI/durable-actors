use super::*;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

pub(crate) type Published = (String, serde_json::Value);

struct RecordingPublisher(UnboundedSender<Published>);

#[async_trait]
impl UsagePublisher for RecordingPublisher {
    async fn publish(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        self.0
            .send((key.into(), serde_json::from_slice(&payload)?))?;
        Ok(())
    }
}

pub(crate) fn recorded() -> (UsageEvents, UnboundedReceiver<Published>) {
    let (sender, receiver) = unbounded_channel();
    (
        UsageEvents::new(Arc::new(RecordingPublisher(sender))),
        receiver,
    )
}
