use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::{ActorDirectory, ObjectAssignment, Region};
use crate::actor::ActorKey;

mod process;
pub use process::{GatewayConfig, serve_gateway};

#[async_trait]
pub(crate) trait RegionalEndpoints: Send + Sync {
    async fn actor_operation(
        &self,
        region: Region,
        actor: &ActorKey,
        operation: &str,
        body: Value,
    ) -> Result<Value>;
}

pub(crate) struct Gateway {
    pub directory: Arc<ActorDirectory>,
    pub ingress: Region,
    endpoints: Arc<dyn RegionalEndpoints>,
}

impl Gateway {
    pub(crate) fn new(
        directory: Arc<ActorDirectory>,
        ingress: Region,
        endpoints: Arc<dyn RegionalEndpoints>,
    ) -> Self {
        Self {
            directory,
            ingress,
            endpoints,
        }
    }

    pub(crate) async fn resolve(&self, actor: &ActorKey, socket: Option<Value>) -> Result<Value> {
        let assignment = self.directory.get_or_create(actor, self.ingress).await?;
        self.resolve_assignment(&assignment, socket).await
    }

    pub(crate) async fn resolve_assignment(
        &self,
        assignment: &ObjectAssignment,
        socket: Option<Value>,
    ) -> Result<Value> {
        let route = self.resolve_route(assignment, socket.clone()).await;
        let mut result = match route {
            // Cold proxy provisioning can outlast the upstream grant. Resolution executes no actor call.
            Err(error)
                if error
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|error| error.status() == Some(reqwest::StatusCode::CONFLICT)) =>
            {
                self.resolve_route(assignment, socket).await?
            }
            result => result?,
        };
        result["objectId"] = assignment.object_id.clone().into();
        result["homeRegion"] = serde_json::to_value(assignment.home_region)?;
        result["ingressRegion"] = serde_json::to_value(self.ingress)?;
        Ok(result)
    }

    async fn resolve_route(
        &self,
        assignment: &ObjectAssignment,
        socket: Option<Value>,
    ) -> Result<Value> {
        let home = assignment.home_region;
        let primary = self
            .endpoints
            .actor_operation(
                home,
                &assignment.actor,
                "find-actor",
                json!({"homeRegion": home}),
            )
            .await?;
        match socket {
            Some(mut request) => {
                request["homeRegion"] = serde_json::to_value(home)?;
                let socket = self
                    .endpoints
                    .actor_operation(home, &assignment.actor, "find-websocket", request)
                    .await?;
                self.socket_route(assignment, &primary, socket).await
            }
            None if home == self.ingress => Ok(primary),
            None => {
                let destination = json!({
                    "kind": "invocation", "route": primary["route"], "token": primary["token"],
                    "ownerEpoch": primary["ownerEpoch"], "expiresAtMs": primary["expiresAtMs"],
                });
                self.proxy_route(assignment, destination).await
            }
        }
    }

    async fn socket_route(
        &self,
        assignment: &ObjectAssignment,
        primary: &Value,
        socket: Value,
    ) -> Result<Value> {
        if assignment.home_region == self.ingress {
            return Ok(socket);
        }
        let url = reqwest::Url::parse(
            socket["websocketUrl"]
                .as_str()
                .context("primary socket URL missing")?,
        )?;
        let token = url
            .query_pairs()
            .find(|(name, _)| name == "key")
            .context("primary socket capability missing")?
            .1
            .to_string();
        let destination = json!({
            "kind": "socket", "route": primary["route"], "token": token,
            "ownerEpoch": primary["ownerEpoch"], "expiresAtMs": socket["connectByMs"],
            "authorizedUntilMs": socket["authorizedUntilMs"],
        });
        let mut result = self.proxy_route(assignment, destination).await?;
        result["authorizedUntilMs"] = socket["authorizedUntilMs"].clone();
        Ok(result)
    }

    async fn proxy_route(
        &self,
        assignment: &ObjectAssignment,
        destination: Value,
    ) -> Result<Value> {
        let request = json!({
            "objectId": assignment.object_id,
            "homeRegion": assignment.home_region,
            "destination": destination,
        });
        self.endpoints
            .actor_operation(self.ingress, &assignment.actor, "find-proxy", request)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct HttpRegionalEndpoints {
    client: reqwest::Client,
    urls: HashMap<Region, String>,
    secret: String,
    identities: HashMap<Region, google_cloud_auth::credentials::idtoken::IDTokenCredentials>,
}

impl HttpRegionalEndpoints {
    pub(crate) fn new(urls: HashMap<String, String>, secret: String) -> Result<Self> {
        let mut mapped = HashMap::new();
        for (region, url) in urls {
            super::proxy::validate_origin(&url)?;
            ensure!(
                mapped
                    .insert(region.parse()?, url.trim_end_matches('/').into())
                    .is_none(),
                "duplicate regional endpoint"
            );
        }
        ensure!(!mapped.is_empty(), "a control-plane endpoint is required");
        ensure!(
            !secret.is_empty() && secret.trim() == secret,
            "gateway authentication is required"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            urls: mapped,
            secret,
            identities: HashMap::new(),
        })
    }

    pub(crate) fn with_cloud_run_auth(mut self, enabled: bool) -> Result<Self> {
        if enabled {
            for (region, origin) in &self.urls {
                self.identities.insert(
                    *region,
                    crate::service_identity::metadata_credentials(origin)?,
                );
            }
        }
        Ok(self)
    }

    pub(crate) async fn request(
        &self,
        region: Region,
        method: reqwest::Method,
        path: &str,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response> {
        ensure!(
            path.starts_with("/v1/projects/") && !path.contains('#'),
            "invalid regional API path"
        );
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.url(region)?))
            .bearer_auth(&self.secret)
            .header("content-type", "application/json")
            .body(body);
        if let Some(identity) = self.identities.get(&region) {
            request = request.header(
                "x-serverless-authorization",
                crate::service_identity::authorization(identity).await?,
            );
        }
        Ok(request.send().await?)
    }
}

impl HttpRegionalEndpoints {
    fn url(&self, region: Region) -> Result<&str> {
        self.urls
            .get(&region)
            .map(String::as_str)
            .with_context(|| format!("no control plane is configured for {}", region.as_str()))
    }
}

#[async_trait]
impl RegionalEndpoints for HttpRegionalEndpoints {
    async fn actor_operation(
        &self,
        region: Region,
        actor: &ActorKey,
        operation: &str,
        body: Value,
    ) -> Result<Value> {
        actor.validate()?;
        ensure!(
            matches!(operation, "find-actor" | "find-websocket" | "find-proxy"),
            "unsupported regional operation"
        );
        let path = format!(
            "/v1/projects/{}/actors/{}/{}/{operation}",
            actor.project_id, actor.actor_name, actor.actor_id
        );
        Ok(self
            .request(
                region,
                reqwest::Method::POST,
                &path,
                serde_json::to_vec(&body)?.into(),
            )
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/regional/gateway.rs"]
mod tests;
