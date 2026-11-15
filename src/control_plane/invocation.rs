use crate::{
    request_tracking::HostState,
    sandbox::assignment::{InitialOperation, InitialRequest},
    sockets::operations::SocketOperation,
};
use anyhow::Context;
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{future::Future, time::Duration};
use tokio::time::Instant;

use super::public_api::{
    ActorPath, ApiError, FindActorRequest, PublicApiState, resolve_actor_target,
};
use super::service::ActorTarget;
use crate::actor::{ActorInvocation, ActorKey};

pub(super) async fn invoke_alarm(
    service: &super::ControlPlaneService,
    client: &reqwest::Client,
    alarm: &super::alarms::ClaimedAlarm,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let target = service
        .resolve_actor_route(&alarm.actor, None, None, None, None)
        .await
        .context("could not route alarm")?;
    let invocation = ActorInvocation {
        actor: alarm.actor.clone(),
        request_id: format!("alarm-{}", alarm.revision),
        method: "onAlarm".into(),
        args: vec![],
    };
    let outcome = dispatch(
        service,
        &alarm.actor,
        &target,
        send_invocation(
            client,
            target.clone(),
            &invocation,
            target.host_state,
            started.elapsed().as_secs_f64() * 1000.0,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("alarm outcome could not be confirmed"))?;
    match outcome["type"].as_str() {
        Some("completed" | "failed") => Ok(()),
        _ => anyhow::bail!("alarm was not executed: {outcome}"),
    }
}

pub(super) async fn invoke(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<InvokeRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    let Json(request) = body.map_err(ApiError::json)?;
    let invocation = ActorInvocation {
        actor,
        request_id: request.request_id,
        method: request.method,
        args: request.args,
    };
    invocation.validate().map_err(ApiError::bad_request)?;
    let outcome = resolve_and_dispatch(&state, &headers, request.home_region, &invocation).await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"outcome": outcome})),
    )
        .into_response())
}

pub(super) async fn warm_host(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<WarmHostRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    let Json(request) = body.map_err(ApiError::json)?;
    if request.request_id.is_empty() || request.request_id.len() > 255 {
        return Err(ApiError::bad_request("requestId must be 1 to 255 bytes"));
    }
    if request.warm_ms > 86_400_000 {
        return Err(ApiError::bad_request("warmMs must be 0 to 86400000"));
    }
    let wait = request.wait()?;
    authorize_warm(&state, &actor, &headers, request.home_region.as_deref())?;
    let (actor_name, actor_id) = (actor.actor_name.clone(), actor.actor_id.clone());
    let request_id = request.request_id.clone();
    let warming = warm(state, actor, headers, request);
    let warmed = match wait {
        None => Some(warming.await),
        Some(limit) => tokio::time::timeout(
            limit,
            tokio::spawn(async move {
                let result = warming.await;
                if let Err(error) = &result {
                    tracing::warn!(
                        event = "actor_warm",
                        request_id,
                        actor_name,
                        actor_id,
                        outcome = "failed",
                        error_code = %error.code,
                        error = %error.message,
                        "background warm failed"
                    );
                }
                result
            }),
        )
        .await
        .ok()
        .map(|joined| joined.unwrap_or_else(|error| Err(ApiError::internal(error)))),
    };
    let (code, status) = match warmed.transpose()? {
        Some(()) => (StatusCode::OK, "OK"),
        None => (StatusCode::ACCEPTED, "accepted"),
    };
    Ok((
        code,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"status": status})),
    )
        .into_response())
}

fn authorize_warm(
    state: &PublicApiState,
    actor: &ActorKey,
    headers: &HeaderMap,
    home_region: Option<&str>,
) -> Result<(), ApiError> {
    state
        .admin
        .authorize_discovery(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
            &actor.project_id,
        )
        .map_err(|_| ApiError::unauthorized("actor discovery credential was rejected"))?;
    actor.validate().map_err(ApiError::bad_request)?;
    state
        .invocations
        .validate_home_region(home_region)
        .map_err(ApiError::assignment)
}

async fn warm(
    state: PublicApiState,
    actor: ActorKey,
    headers: HeaderMap,
    request: WarmHostRequest,
) -> Result<(), ApiError> {
    let resolve = || {
        resolve_actor_target(
            &state,
            &actor,
            &headers,
            Ok(Json(FindActorRequest {
                home_region: request.home_region.clone(),
            })),
            None,
        )
    };
    let deadline = Instant::now() + super::CONTROL_PLANE_REQUEST_TIMEOUT;
    let outcome = settle(
        &state.invocations,
        &actor,
        deadline,
        resolve,
        |target, host_state, routing_ms| {
            send_warm(
                &state.hosts,
                &actor,
                target,
                &request,
                host_state,
                routing_ms,
            )
        },
    )
    .await?;
    match outcome["type"].as_str() {
        Some("completed") => Ok(()),
        _ => Err(ApiError::unavailable("actor host did not accept the warm")),
    }
}

pub(super) async fn invoke_from_host(
    service: &super::ControlPlaneService,
    invocation: &ActorInvocation,
) -> Value {
    resolve_and_dispatch_from_host(service, invocation)
        .await
        .unwrap_or_else(ApiError::into_outcome)
}

async fn resolve_and_dispatch_from_host(
    service: &super::ControlPlaneService,
    invocation: &ActorInvocation,
) -> Result<Value, ApiError> {
    invocation.validate().map_err(ApiError::bad_request)?;
    let started = Instant::now();
    let deadline = started + super::CONTROL_PLANE_REQUEST_TIMEOUT;
    let resolve = || async {
        let operation = InitialOperation {
            request: InitialRequest::Invoke {
                invocation: invocation.clone(),
                grant: None,
            },
            routing_ms: started.elapsed().as_secs_f64() * 1000.0,
        };
        service
            .resolve_actor_route(&invocation.actor, None, None, None, Some(&operation))
            .await
            .map_err(ApiError::routing)
    };
    let hosts = &service.host_client();
    settle(
        service,
        &invocation.actor,
        deadline,
        resolve,
        |target, host_state, routing_ms| {
            send_invocation(hosts, target, invocation, host_state, routing_ms)
        },
    )
    .await
}

async fn resolve_and_dispatch(
    state: &PublicApiState,
    headers: &HeaderMap,
    home_region: Option<String>,
    invocation: &ActorInvocation,
) -> Result<Value, ApiError> {
    let deadline = Instant::now() + super::CONTROL_PLANE_REQUEST_TIMEOUT;
    let resolve = || {
        resolve_actor_target(
            state,
            &invocation.actor,
            headers,
            Ok(Json(FindActorRequest {
                home_region: home_region.clone(),
            })),
            Some(invocation),
        )
    };
    settle(
        &state.invocations,
        &invocation.actor,
        deadline,
        resolve,
        |target, host_state, routing_ms| {
            send_invocation(&state.hosts, target, invocation, host_state, routing_ms)
        },
    )
    .await
}

/// Dispatches until the invocation runs or the deadline passes, re-resolving after refusals.
async fn settle<Resolve, Resolving, Dispatch, Dispatching>(
    service: &super::ControlPlaneService,
    actor: &ActorKey,
    deadline: Instant,
    resolve: Resolve,
    send: Dispatch,
) -> Result<Value, ApiError>
where
    Resolve: Fn() -> Resolving,
    Resolving: Future<Output = Result<ActorTarget, ApiError>>,
    Dispatch: Fn(ActorTarget, HostState, f64) -> Dispatching,
    Dispatching: Future<Output = Result<Value, ApiError>>,
{
    let started = Instant::now();
    let mut host_state = HostState::Warm;
    loop {
        let mut target = resolve().await?;
        if target.host_state == HostState::Cold {
            host_state = HostState::Cold;
        }
        let outcome = match target.initial_reply.take() {
            Some(reply) => {
                let result = validate_reply(reply);
                if result
                    .as_ref()
                    .map_or(true, |reply| reply["type"] == "not_executed")
                {
                    service.routes.remove(actor, &target.identity).await;
                }
                result?
            }
            None => {
                let routing_ms = started.elapsed().as_secs_f64() * 1000.0;
                dispatch(
                    service,
                    actor,
                    &target,
                    send(target.clone(), host_state, routing_ms),
                )
                .await?
            }
        };
        if !matches!(
            outcome["type"].as_str(),
            Some("not_executed" | "unauthenticated")
        ) || Instant::now() >= deadline
        {
            return Ok(outcome);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub(super) async fn publish(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<PublishRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    super::public_api::authorized_admin(&state.admin, &headers)?;
    actor.validate().map_err(ApiError::bad_request)?;
    state
        .invocations
        .ensure_project_access(&actor.project_id)
        .await
        .map_err(ApiError::access)?;
    let gateway = state
        .invocations
        .gateway
        .as_ref()
        .ok_or_else(|| ApiError::unauthorized("gateway is not configured"))?;
    let Json(request) = body.map_err(ApiError::json)?;
    crate::actor::validate_socket_effects(&request.effects).map_err(ApiError::bad_request)?;

    gateway
        .connections
        .operation(
            &actor,
            SocketOperation::Publish {
                effects: request.effects,
            },
        )
        .await
        .map_err(|error| {
            tracing::warn!(error = %format!("{error:#}"), "socket effects could not be delivered");
            ApiError::unavailable("socket effects could not be delivered")
        })?;
    Ok((
        StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    )
        .into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PublishRequest {
    effects: Vec<crate::actor::ActorSocketEffect>,
}

async fn dispatch(
    service: &super::ControlPlaneService,
    actor: &ActorKey,
    target: &ActorTarget,
    sent: impl Future<Output = Result<Value, ApiError>>,
) -> Result<Value, ApiError> {
    let result = sent.await;
    if result.as_ref().map_or(true, |reply| {
        matches!(
            reply["type"].as_str(),
            Some("not_executed" | "unauthenticated")
        )
    }) {
        service.routes.remove(actor, &target.identity).await;
    }
    result
}

async fn send_invocation(
    client: &reqwest::Client,
    target: ActorTarget,
    invocation: &ActorInvocation,
    host_state: HostState,
    routing_ms: f64,
) -> Result<Value, ApiError> {
    let body = json!({
        "requestId":invocation.request_id, "ownerEpoch":target.owner_epoch,
        "method":invocation.method, "args":invocation.args, "hostState":host_state, "routingMs":routing_ms,
    });
    let response = match post(client, &target, &invocation.actor, "invoke", body).await? {
        Ok(response) => response,
        Err(refused) => return Ok(refused),
    };
    if !response.status().is_success() {
        return Err(outcome_unknown());
    }
    let reply = read_reply(response).await?;
    validate_reply(reply)
}

async fn send_warm(
    client: &reqwest::Client,
    actor: &ActorKey,
    target: ActorTarget,
    request: &WarmHostRequest,
    host_state: HostState,
    routing_ms: f64,
) -> Result<Value, ApiError> {
    let body = json!({
        "requestId":request.request_id, "ownerEpoch":target.owner_epoch, "warmMs":request.warm_ms,
        "hostState":host_state, "routingMs":routing_ms,
    });
    match post(client, &target, actor, "warm-host", body).await? {
        Ok(response) if response.status() == StatusCode::OK => Ok(json!({"type":"completed"})),
        Ok(response) if response.status() == StatusCode::SERVICE_UNAVAILABLE => {
            Ok(json!({"type":"not_executed", "reason":"host_unavailable"}))
        }
        Ok(_) => Err(outcome_unknown()),
        Err(refused) => Ok(refused),
    }
}

/// Sends `body` to the actor's host.
/// The inner `Err` is the reply when the host was not reached or refused the token.
async fn post(
    client: &reqwest::Client,
    target: &ActorTarget,
    actor: &ActorKey,
    endpoint: &str,
    body: Value,
) -> Result<Result<reqwest::Response, Value>, ApiError> {
    let url = format!(
        "{}/v1/projects/{}/actors/{}/{}/{endpoint}",
        target.route.trim_end_matches('/'),
        actor.project_id,
        actor.actor_name,
        actor.actor_id
    );
    match client
        .post(url)
        .bearer_auth(&target.token)
        .json(&body)
        .send()
        .await
    {
        Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
            Ok(Err(json!({"type":"unauthenticated"})))
        }
        Ok(response) => Ok(Ok(response)),
        Err(error) if error.is_connect() => Ok(Err(
            json!({"type":"not_executed", "reason":"upstream_not_reached"}),
        )),
        Err(_) => Err(outcome_unknown()),
    }
}

fn validate_reply(reply: Value) -> Result<Value, ApiError> {
    match reply.get("type").and_then(Value::as_str) {
        Some("not_executed")
            if matches!(
                reply["reason"].as_str(),
                Some("host_unavailable" | "upstream_not_reached")
            ) =>
        {
            Ok(reply)
        }
        Some("completed") if reply.get("result").is_some() => Ok(reply),
        Some("failed")
            if reply["code"].as_str().is_some_and(|code| !code.is_empty())
                && reply["message"].is_string() =>
        {
            Ok(reply)
        }
        _ => Err(outcome_unknown()),
    }
}

async fn read_reply(response: reqwest::Response) -> Result<Value, ApiError> {
    response.json().await.map_err(|_| outcome_unknown())
}

fn outcome_unknown() -> ApiError {
    ApiError::new(
        StatusCode::BAD_GATEWAY,
        "outcome_unknown",
        "actor invocation was dispatched but its outcome could not be confirmed",
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WarmHostRequest {
    request_id: String,
    warm_ms: u64,
    wait_for_completion: Option<bool>,
    timeout_ms: Option<u64>,
    home_region: Option<String>,
}

impl WarmHostRequest {
    /// How long the reply waits for the warm; `None` waits until the host has it.
    fn wait(&self) -> Result<Option<Duration>, ApiError> {
        match (self.wait_for_completion, self.timeout_ms) {
            (Some(false), None) => Ok(Some(Duration::ZERO)),
            (Some(false), Some(ms)) if (1..=30_000).contains(&ms) => {
                Ok(Some(Duration::from_millis(ms)))
            }
            (Some(false), Some(_)) => Err(ApiError::bad_request("timeoutMs must be 1 to 30000")),
            (_, Some(_)) => Err(ApiError::bad_request(
                "timeoutMs requires waitForCompletion false",
            )),
            (_, None) => Ok(None),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InvokeRequest {
    request_id: String,
    method: String,
    args: Vec<Value>,
    home_region: Option<String>,
}
