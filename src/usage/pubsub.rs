use super::{UsageInterval, UsageSink};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use gcp_auth::TokenProvider;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

pub struct PubSubUsageSink {
    client: reqwest::Client,
    url: reqwest::Url,
    credentials: Arc<dyn TokenProvider>,
}

impl PubSubUsageSink {
    pub fn new(topic: &str, credentials: Arc<dyn TokenProvider>) -> Result<Self> {
        let parts: Vec<_> = topic.split('/').collect();
        ensure!(
            parts.len() == 4 && parts[0] == "projects" && parts[2] == "topics",
            "expected projects/PROJECT/topics/TOPIC"
        );
        for part in [parts[1], parts[3]] {
            ensure!(
                !part.is_empty()
                    && part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.~+".contains(&byte))
                    && part != "."
                    && part != "..",
                "invalid Pub/Sub topic resource"
            );
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            url: format!("https://pubsub.googleapis.com/v1/{topic}:publish").parse()?,
            credentials,
        })
    }
}

#[async_trait]
impl UsageSink for PubSubUsageSink {
    async fn deliver(&self, events: &[UsageInterval]) -> Result<()> {
        let messages = events
            .iter()
            .map(|event| {
                Ok(serde_json::json!({"data": STANDARD.encode(serde_json::to_vec(event)?)}))
            })
            .collect::<Result<Vec<_>>>()?;
        let token = tokio::time::timeout(
            Duration::from_secs(10),
            self.credentials
                .token(&["https://www.googleapis.com/auth/pubsub"]),
        )
        .await??;
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(token.as_str())
            .json(&serde_json::json!({"messages":messages}))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            response.status().is_success(),
            "Pub/Sub rejected usage publication"
        );
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Published {
            message_ids: Vec<String>,
        }
        let published: Published = response.json().await?;
        ensure!(
            published.message_ids.len() == events.len()
                && published.message_ids.iter().all(|id| !id.is_empty()),
            "Pub/Sub did not confirm every usage message"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/usage/pubsub.rs"]
mod tests;
