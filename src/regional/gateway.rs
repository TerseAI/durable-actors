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
    hosts: reqwest::Client,
}

pub(crate) enum InvocationOutcome {
    Reply(Value),
    Unavailable,
    OutcomeUnknown,
}

enum Dispatch {
    Reply(Value),
    RefreshTarget,
}

#[derive(Default)]
struct DispatchTiming {
    resolved_ms: f64,
    ping_ms: Option<f64>,
    invoke_ms: Option<f64>,
    host: Option<String>,
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
            hosts: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .pool_idle_timeout(Duration::from_secs(90))
                .build()
                .expect("static actor-host client configuration"),
        }
    }

    /// Resolves and invokes in one client request. Only rejections that precede dispatch are retried.
    pub(crate) async fn invoke(&self, actor: &ActorKey, call: &Value) -> InvocationOutcome {
        let started = std::time::Instant::now();
        let mut timing = DispatchTiming::default();
        let mut attempts = 0;
        let outcome = loop {
            attempts += 1;
            let Ok(target) = self.resolve(actor, None).await else {
                break InvocationOutcome::Unavailable;
            };
            timing.resolved_ms = started.elapsed().as_secs_f64() * 1_000.0;
            match self
                .dispatch(actor, &target, call, &mut timing, started)
                .await
            {
                Ok(Dispatch::Reply(mut reply)) => {
                    reply["target"] = direct_target(&target);
                    break InvocationOutcome::Reply(reply);
                }
                Ok(Dispatch::RefreshTarget) if attempts == 1 => continue,
                Ok(Dispatch::RefreshTarget) => break InvocationOutcome::Unavailable,
                Err(()) => break InvocationOutcome::OutcomeUnknown,
            }
        };
        tracing::info!(
            event = "gateway_invocation",
            actor = %actor.storage_key(),
            request_id = call["requestId"].as_str().unwrap_or_default(),
            attempts,
            resolved_ms = timing.resolved_ms,
            ping_completed_ms = timing.ping_ms,
            invoke_completed_ms = timing.invoke_ms,
            host_timing = timing.host.as_deref().unwrap_or_default(),
            completed_ms = started.elapsed().as_secs_f64() * 1_000.0,
            outcome = match outcome {
                InvocationOutcome::Reply(_) => "replied",
                InvocationOutcome::Unavailable => "unavailable",
                InvocationOutcome::OutcomeUnknown => "outcome_unknown",
            },
            "gateway invocation completed"
        );
        outcome
    }

    async fn dispatch(
        &self,
        actor: &ActorKey,
        target: &Value,
        call: &Value,
        timing: &mut DispatchTiming,
        started: std::time::Instant,
    ) -> Result<Dispatch, ()> {
        let elapsed = || started.elapsed().as_secs_f64() * 1_000.0;
        let (Some(route), Some(token), Some(owner_epoch)) = (
            target["route"].as_str(),
            target["token"].as_str(),
            target["ownerEpoch"].as_u64(),
        ) else {
            return Ok(Dispatch::RefreshTarget);
        };
        let url = format!(
            "{}/v1/projects/{}/actors/{}/{}/invoke",
            route.trim_end_matches('/'),
            actor.project_id,
            actor.actor_name,
            actor.actor_id
        );
        let ready = self
            .hosts
            .head(&url)
            .bearer_auth(token)
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        timing.ping_ms = Some(elapsed());
        if !ready.is_ok_and(|response| response.status() == reqwest::StatusCode::NO_CONTENT) {
            return Ok(Dispatch::RefreshTarget);
        }
        let mut body = call.clone();
        body["ownerEpoch"] = owner_epoch.into();
        let sent = self
            .hosts
            .post(&url)
            .bearer_auth(token)
            .header(crate::host::http::TIMING_REQUEST, "1")
            .json(&body)
            .send()
            .await;
        timing.invoke_ms = Some(elapsed());
        let response = match sent {
            Ok(response) => response,
            Err(error) if error.is_connect() => return Ok(Dispatch::RefreshTarget),
            Err(_) => return Err(()),
        };
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Ok(Dispatch::RefreshTarget);
        }
        if !response.status().is_success() {
            return Err(());
        }
        timing.host = response
            .headers()
            .get("server-timing")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let reply: Value = response.json().await.map_err(|_| ())?;
        match reply["type"].as_str() {
            Some("reroute") => Ok(Dispatch::RefreshTarget),
            Some("completed" | "failed") => Ok(Dispatch::Reply(reply)),
            _ => Err(()),
        }
    }

    pub(crate) async fn resolve(&self, actor: &ActorKey, socket: Option<Value>) -> Result<Value> {
        let started = std::time::Instant::now();
        let assignment = self.directory.get_or_create(actor, self.ingress).await?;
        let directory_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let result = self.resolve_assignment(&assignment, socket).await;
        tracing::info!(
            event = "gateway_actor_resolution",
            actor = %actor.storage_key(),
            home_region = assignment.home_region.as_str(),
            ingress_region = self.ingress.as_str(),
            directory_ms,
            completed_ms = started.elapsed().as_secs_f64() * 1_000.0,
            outcome = if result.is_ok() { "resolved" } else { "failed" },
            "gateway actor resolution completed"
        );
        result
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

fn direct_target(target: &Value) -> Value {
    json!({
        "route": target["route"], "token": target["token"],
        "ownerEpoch": target["ownerEpoch"], "expiresAtMs": target["expiresAtMs"],
    })
}
