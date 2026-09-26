use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::{ActorDirectory, ObjectAssignment, Region};
use crate::actor::ActorKey;

mod process;
pub use process::{GatewayConfig, serve_gateway};

pub(crate) type RouteHint = Box<dyn FnOnce(String) + Send>;

#[async_trait]
pub(crate) trait RegionalEndpoints: Send + Sync {
    async fn actor_operation(
        &self,
        region: Region,
        actor: &ActorKey,
        operation: &str,
        body: Value,
    ) -> Result<Value>;

    /// Resolves the primary, reporting a claimed spare's route before activation completes.
    async fn find_actor_hinted(
        &self,
        region: Region,
        actor: &ActorKey,
        body: Value,
        _hint: RouteHint,
    ) -> Result<Value> {
        self.actor_operation(region, actor, "find-actor", body)
            .await
    }
}

pub(crate) struct Gateway {
    pub directory: Arc<ActorDirectory>,
    pub ingress: Region,
    endpoints: Arc<dyn RegionalEndpoints>,
    hosts: reqwest::Client,
    routes: moka::future::Cache<String, Value>,
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
    cached: bool,
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
            routes: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(60))
                .build(),
        }
    }

    /// Resolves and invokes in one client request. Only rejections that precede dispatch are retried.
    pub(crate) async fn invoke(&self, actor: &ActorKey, call: &Value) -> InvocationOutcome {
        let started = std::time::Instant::now();
        let mut timing = DispatchTiming::default();
        let mut attempts = 0;
        let key = actor.storage_key().as_str().to_owned();
        let outcome = loop {
            attempts += 1;
            let cached = match attempts {
                1 => self.cached_route(&key).await,
                _ => None,
            };
            timing.cached = cached.is_some();
            let target = match cached {
                Some(target) => target,
                None => match self.resolve(actor, None).await {
                    Ok(target) => target,
                    Err(_) => break InvocationOutcome::Unavailable,
                },
            };
            timing.resolved_ms = started.elapsed().as_secs_f64() * 1_000.0;
            match self
                .dispatch(actor, &target, call, &mut timing, started)
                .await
            {
                Ok(Dispatch::Reply(reply)) => {
                    self.routes.insert(key, target).await;
                    break InvocationOutcome::Reply(reply);
                }
                Ok(Dispatch::RefreshTarget) => {
                    self.routes.invalidate(&key).await;
                    if attempts == 1 {
                        continue;
                    }
                    break InvocationOutcome::Unavailable;
                }
                Err(()) => {
                    self.routes.invalidate(&key).await;
                    break InvocationOutcome::OutcomeUnknown;
                }
            }
        };
        tracing::info!(
            event = "gateway_invocation",
            actor = %actor.storage_key(),
            request_id = call["requestId"].as_str().unwrap_or_default(),
            attempts,
            cached_route = timing.cached,
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

    /// Opens the gateway's connection to a claimed host while it is still activating.
    fn warm_connection(&self, actor: &ActorKey) -> RouteHint {
        let hosts = self.hosts.clone();
        let path = format!(
            "/v1/projects/{}/actors/{}/{}/invoke",
            actor.project_id, actor.actor_name, actor.actor_id
        );
        Box::new(move |route| {
            if super::proxy::validate_origin(&route).is_err() {
                return;
            }
            let url = format!("{}{path}", route.trim_end_matches('/'));
            tokio::spawn(async move {
                let _ = hosts.head(url).timeout(Duration::from_secs(5)).send().await;
            });
        })
    }

    async fn cached_route(&self, key: &str) -> Option<Value> {
        let target = self.routes.get(key).await?;
        let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock).ok()? as i64;
        match target["expiresAtMs"].as_i64() {
            Some(expires) if expires > now + 5_000 => Some(target),
            _ => {
                self.routes.invalidate(key).await;
                None
            }
        }
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
        let body = json!({"homeRegion": home});
        let primary = if socket.is_none() && home == self.ingress {
            let warm = self.warm_connection(&assignment.actor);
            self.endpoints
                .find_actor_hinted(home, &assignment.actor, body, warm)
                .await?
        } else {
            self.endpoints
                .actor_operation(home, &assignment.actor, "find-actor", body)
                .await?
        };
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
        Ok(self
            .prepare(region, method, path, body)
            .await?
            .send()
            .await?)
    }

    async fn prepare(
        &self,
        region: Region,
        method: reqwest::Method,
        path: &str,
        body: bytes::Bytes,
    ) -> Result<reqwest::RequestBuilder> {
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
        Ok(request)
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

    async fn find_actor_hinted(
        &self,
        region: Region,
        actor: &ActorKey,
        body: Value,
        hint: RouteHint,
    ) -> Result<Value> {
        actor.validate()?;
        let path = format!(
            "/v1/projects/{}/actors/{}/{}/find-actor",
            actor.project_id, actor.actor_name, actor.actor_id
        );
        let response = self
            .prepare(
                region,
                reqwest::Method::POST,
                &path,
                serde_json::to_vec(&body)?.into(),
            )
            .await?
            .header(crate::control_plane::ROUTE_HINT_REQUEST, "1")
            .send()
            .await?
            .error_for_status()?;
        read_hinted_reply(response, hint).await
    }
}

async fn read_hinted_reply(response: reqwest::Response, hint: RouteHint) -> Result<Value> {
    use futures_util::StreamExt;
    let mut hint = Some(hint);
    let mut buffer = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk?);
        while let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = buffer.drain(..=end).collect();
            let document: Value = serde_json::from_slice(&line)?;
            if let Some(route) = document["routeHint"].as_str() {
                if let Some(hint) = hint.take() {
                    hint(route.to_owned());
                }
                continue;
            }
            if let Some(failure) = document.get("failure") {
                anyhow::bail!("control plane find-actor failed: {failure}");
            }
            return Ok(document);
        }
    }
    anyhow::bail!("control plane find-actor ended without a reply")
}

#[cfg(test)]
#[path = "../../tests/unit/regional/gateway.rs"]
mod tests;
