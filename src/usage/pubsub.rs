use anyhow::{Result, ensure};
use async_trait::async_trait;
use google_cloud_pubsub::{client::Publisher, model::Message};

use super::UsagePublisher;

pub(super) struct PubSubPublisher(Publisher);

impl PubSubPublisher {
    pub(super) async fn connect(topic: &str) -> Result<Self> {
        ensure!(
            matches!(
                topic.split('/').collect::<Vec<_>>().as_slice(),
                ["projects", project, "topics", name] if !project.is_empty() && !name.is_empty()
            ),
            "usage events topic must be projects/<project>/topics/<topic>"
        );
        Ok(Self(Publisher::builder(topic).build().await?))
    }
}

#[async_trait]
impl UsagePublisher for PubSubPublisher {
    async fn publish(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        let message = Message::new()
            .set_data(payload)
            .set_attributes([("hostId", key)]);
        self.0.publish(message).await?;
        Ok(())
    }
}
