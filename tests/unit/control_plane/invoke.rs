use super::super::super::socket_gateway::{SocketEventReply, SocketEventRequest};
use super::*;
use crate::actor::{ActorSocketEffect, ActorSocketMessage};
use crate::control_plane::access::AccessSource;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

#[path = "shutdown_routing.rs"]
mod shutdown_routing;

#[tokio::test]
async fn assignment_returns_invocation_outcomes_without_a_second_host_request() -> Result<()> {
    for outcome in [
        json!({"type":"completed", "result":7}),
        json!({"type":"failed", "code":"actor_method_failed", "message":"failed"}),
    ] {
        let fixture = Fixture::start(vec![(StatusCode::OK, outcome.clone())]).await?;
        fixture.host.assign_operation.store(true, Ordering::SeqCst);
        let expiry = (super::super::super::auth::unix_seconds()? + 30) * 1000;
        let session = fixture
            .admin
            .issue_session("default".into(), "subject".into(), expiry)?;
        let reply: Value = fixture
            .call(
                &session.token,
                json!({"requestId":"first", "method":"sendMessage", "args":["hello"]}),
            )
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["outcome"], outcome);
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
        let operations = fixture.host.initial_operations.lock().unwrap();
        assert_eq!(operations.len(), 1);
        let crate::sandbox::assignment::InitialRequest::Invoke { invocation, grant } =
            &operations[0].request
        else {
            panic!("expected invocation")
        };
        assert_eq!(invocation.request_id, "first");
        assert_eq!(invocation.args, vec![json!("hello")]);
        assert_eq!(grant.as_ref().unwrap().subject, "subject");
    }
    Ok(())
}

#[tokio::test]
async fn assignment_socket_effects_are_delivered_once_and_never_cached() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    fixture.host.assign_operation.store(true, Ordering::SeqCst);
    let _first = fixture.connect().await?;
    assert_eq!(fixture.host.initial_operations.lock().unwrap().len(), 1);
    assert!(fixture.host.socket_requests.lock().unwrap().is_empty());
    let _second = fixture.connect().await?;
    assert_eq!(fixture.host.initial_operations.lock().unwrap().len(), 1);
    assert_eq!(fixture.host.socket_requests.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn lost_assignment_operation_reply_is_never_redispatched_to_the_ready_host() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    fixture.host.assign_operation.store(true, Ordering::SeqCst);
    fixture
        .host
        .lose_assignment_reply
        .store(true, Ordering::SeqCst);
    let response = fixture
        .call(
            "api-key",
            json!({"requestId":"first", "method":"clear", "args":[]}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let reply: Value = response.json().await?;
    assert_eq!(reply["error"]["code"], "outcome_unknown");
    assert_eq!(fixture.host.initial_operations.lock().unwrap().len(), 1);
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn suspension_closes_existing_websockets() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let mut socket = fixture.connect().await?;
    let before = fixture.host.socket_requests.lock().unwrap().len();
    fixture
        .admin
        .set_organization_access("org-test", AccessSource::Admin, false)
        .await?;
    fixture.gateway.reconcile_access().await?;
    socket.send(Message::Text("{}".into())).await?;
    let message = next_message(&mut socket).await?;
    assert!(matches!(message, Message::Close(_)));
    assert_eq!(fixture.host.socket_requests.lock().unwrap().len(), before);
    Ok(())
}

#[tokio::test]
async fn combined_invocation_resolves_and_dispatches_with_a_scoped_host_token() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":null}),
    )])
    .await?;
    let response = fixture.call("api-key", json!({"requestId":"call-1", "method":"sendMessage", "args":["hello"], "homeRegion":"north-america-east"})).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let reply: Value = response.json().await?;
    assert_eq!(reply["outcome"]["type"], "completed");
    assert!(reply["outcome"]["result"].is_null());
    assert!(reply.get("target").is_none());
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    let requests = fixture.host.requests.lock().unwrap();
    let routing_ms = requests[0].1["routingMs"].as_f64().unwrap();
    assert!(routing_ms >= 25.0);
    assert_eq!(
        requests[0].1,
        json!({"requestId":"call-1", "ownerEpoch":1, "method":"sendMessage", "args":["hello"], "hostState":"cold", "routingMs":routing_ms})
    );
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    assert_eq!(principal.actor.actor_id, "one");
    Ok(())
}

#[tokio::test]
async fn resolving_an_existing_host_marks_the_request_warm() -> Result<()> {
    let fixture = Fixture::start_with_existing_host(
        vec![(StatusCode::OK, json!({"type":"completed", "result":1}))],
        true,
    )
    .await?;
    fixture
        .call(
            "api-key",
            json!({"requestId":"warm", "method":"clear", "args":[]}),
        )
        .await?
        .error_for_status()?;
    assert_eq!(
        fixture.host.requests.lock().unwrap()[0].1["hostState"],
        "warm"
    );
    Ok(())
}

#[tokio::test]
async fn combined_invocation_authenticates_and_validates_requests() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":7}),
    )])
    .await?;
    let request = json!({"requestId":"call-1", "method":"sendMessage", "args":[]});
    assert_eq!(
        fixture.call("invalid", request.clone()).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    for body in [
        json!({"requestId":"", "method":"sendMessage", "args":[]}),
        json!({"requestId":"r", "method":"sendMessage", "args":{}}),
        json!({"requestId":"r", "method":"sendMessage", "args":[], "homeRegion":false}),
    ] {
        assert_eq!(
            fixture.call("api-key", body).await?.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    let expiry = (super::super::super::auth::unix_seconds()? + 30) * 1000;
    let session = fixture
        .admin
        .issue_session("default".into(), "subject".into(), expiry)?;
    let response = fixture.call(&session.token, request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let requests = fixture.host.requests.lock().unwrap();
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    let grant = principal.invocation.unwrap().grant.unwrap();
    assert_eq!(grant.subject, "subject");
    Ok(())
}

#[tokio::test]
async fn invoke_api_key_reaches_only_actor_rpc_and_socket_routes() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":7}),
    )])
    .await?;
    let request = json!({
        "requestId": "call-1",
        "method": "sendMessage",
        "args": [],
        "homeRegion": "north-america-east"
    });
    let response = fixture.call("invoke-key", request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let requests = fixture.host.requests.lock().unwrap();
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    drop(requests);
    let grant = principal.invocation.unwrap().grant.unwrap();
    assert_eq!(grant.subject, "invoke-api-key");
    assert!(grant.methods.iter().any(|method| method == "sendMessage"));
    assert!(!grant.methods.iter().any(|method| method == "onMessage"));

    let reply: Value = fixture
        .post("find-actor", "invoke-key", json!({}))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(reply, json!({"homeRegion":"north-america-east"}));
    let socket: Value = fixture
        .post("find-websocket", "invoke-key", json!({"metadata":{}}))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(socket["websocketUrl"].as_str().is_some());
    assert_eq!(
        fixture
            .post("find-websocket", "wrong-key", json!({"metadata":{}}))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let response = fixture
        .post("socket-effects", "invoke-key", broadcast())
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let error: Value = response.json().await?;
    assert_eq!(error["error"]["code"], "unauthenticated");
    let client = reqwest::Client::new();
    for (method, path) in [
        (reqwest::Method::POST, "/v1/projects/default/sessions"),
        (reqwest::Method::GET, "/v1/projects/default/deployment"),
        (reqwest::Method::PUT, "/v1/projects/default/deployment"),
        (reqwest::Method::DELETE, "/v1/projects/default/deployment"),
        (
            reqwest::Method::GET,
            "/v1/projects/default/deployment/contract",
        ),
        (reqwest::Method::PUT, "/v1/organizations/org-test/access"),
    ] {
        assert_eq!(
            client
                .request(method, format!("{}{path}", fixture.origin))
                .bearer_auth("invoke-key")
                .json(&json!({}))
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED,
            "{path}"
        );
    }
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn combined_invocation_retries_known_non_execution_through_handoff() -> Result<()> {
    for (status, outcome) in [
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"host_unavailable"}),
        ),
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"upstream_not_reached"}),
        ),
        (StatusCode::UNAUTHORIZED, json!({})),
    ] {
        let mut replies = vec![(status, outcome); 4];
        replies.push((StatusCode::OK, json!({"type":"completed", "result":42})));
        let fixture = Fixture::start(replies).await?;
        let started = std::time::Instant::now();
        let reply: Value = fixture
            .call(
                "api-key",
                json!({"requestId":"same-id", "method":"clear", "args":[]}),
            )
            .await?
            .json()
            .await?;
        assert_eq!(reply["outcome"], json!({"type":"completed", "result":42}));
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let requests = fixture.host.requests.lock().unwrap();
        let routing_ms = requests.last().unwrap().1["routingMs"].as_f64().unwrap();
        assert!(routing_ms >= 400.0 && routing_ms <= elapsed_ms);
        drop(requests);
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 5);
        assert!(fixture.host.requests.lock().unwrap().iter().all(
            |(_, request)| request["requestId"] == "same-id" && request["hostState"] == "cold"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn combined_invocation_does_not_replay_failed_or_ambiguous_dispatches() -> Result<()> {
    for (status, body, code) in [
        (
            StatusCode::OK,
            json!({"type":"failed", "code":"actor_error", "message":"boom"}),
            "actor_error",
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"unknown"}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"completed"}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"failed", "code":"", "message":"boom"}),
            "outcome_unknown",
        ),
    ] {
        let fixture = Fixture::start(vec![(status, body)]).await?;
        let reply: Value = fixture
            .call(
                "api-key",
                json!({"requestId":"r", "method":"clear", "args":[]}),
            )
            .await?
            .json()
            .await?;
        assert_eq!(
            reply["outcome"]
                .get("code")
                .unwrap_or(&reply["error"]["code"]),
            code
        );
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn gateway_routes_actor_calls_and_rejects_host_tickets() -> Result<()> {
    let fixture = Fixture::start(vec![
        (StatusCode::OK, json!({"type":"completed","result":1})),
        (StatusCode::OK, json!({"type":"completed","result":2})),
    ])
    .await?;
    let request = json!({"requestId":"call","method":"clear","args":[]});
    fixture
        .call("api-key", request.clone())
        .await?
        .error_for_status()?;
    let token = fixture.host.requests.lock().unwrap()[0]
        .0
        .strip_prefix("Bearer ")
        .unwrap()
        .to_owned();
    fixture.service.routes.remove_project("default").await;
    assert_eq!(
        fixture.call(&token, request.clone()).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let mut host_request = request.clone();
    host_request["ownerEpoch"] = 1.into();
    assert_eq!(
        fixture.call("api-key", host_request).await?.status(),
        StatusCode::BAD_REQUEST
    );
    let reply: Value = fixture
        .call("api-key", request)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(reply, json!({"outcome":{"type":"completed","result":2}}));
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn backend_socket_effects_reach_connected_clients() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let mut socket = fixture.connect().await?;
    let response = fixture
        .post("socket-effects", "api-key", broadcast())
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        next_message(&mut socket).await?,
        Message::Text("hello".into())
    );
    Ok(())
}

#[tokio::test]
async fn backend_socket_effects_reach_clients_connected_to_another_replica() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let mut socket = fixture.connect().await?;
    let response = post_actor(&fixture.replica, "socket-effects", "api-key", broadcast()).await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        next_message(&mut socket).await?,
        Message::Text("hello".into())
    );
    Ok(())
}

#[tokio::test] // TODO: Test 8 warming an actor with no running host starts one host, runs no method, and replies 200 once the host has the warm
async fn warming_an_actor_without_a_host_replies_once_its_host_is_warm() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 60_000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<Value>().await?, json!({"status": "OK"}));
    assert_eq!(fixture.host.launches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    let warms = fixture.host.warms.lock().unwrap();
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0]["warmMs"], 60_000);
    assert_eq!(warms[0]["requestId"], "warm-1");
    assert!(
        warms[0]["ownerEpoch"]
            .as_u64()
            .is_some_and(|epoch| epoch > 0)
    );
    Ok(())
}

#[tokio::test] // TODO: Test 9 warming an actor whose host is running sends the warm to that host and starts no new one
async fn warming_an_awake_actor_launches_no_host() -> Result<()> {
    let fixture = Fixture::start_with_existing_host(vec![], true).await?;
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 60_000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.host.launches.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    let warms = fixture.host.warms.lock().unwrap();
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0]["warmMs"], 60_000);
    assert_eq!(warms[0]["requestId"], "warm-1");
    assert!(
        warms[0]["ownerEpoch"]
            .as_u64()
            .is_some_and(|epoch| epoch > 0)
    );
    Ok(())
}

#[tokio::test] // TODO: Test 10 a missing or malformed duration or quick-reply option gets 400 invalid_request and starts no host
async fn warming_rejects_a_malformed_duration_without_launching() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    for body in [
        json!({"requestId": "warm-1"}),                       // 400
        json!({"requestId": "warm-1", "warmMs": null}),       // 400
        json!({"requestId": "warm-1", "warmMs": -5}),         // 400
        json!({"requestId": "warm-1", "warmMs": 1.5}),        // 400
        json!({"requestId": "warm-1", "warmMs": "60000"}),    // 400
        json!({"requestId": "warm-1", "warmMs": 86_400_001}), // 400
        json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": false, "timeoutMs": 0}), // 400
        json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": false, "timeoutMs": 1.5}), // 400
        json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": false, "timeoutMs": 30_001}), // 400
        json!({"requestId": "warm-1", "warmMs": 60_000, "timeoutMs": 2000}), // 400
        json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": true, "timeoutMs": 2000}), // 400
        json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": "no"}), // 400
    ] {
        let response = fixture.post("warm-host", "api-key", body).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let reply: Value = response.json().await?;
        assert_eq!(reply["error"]["code"], "invalid_request");
    }
    assert_eq!(fixture.host.launches.load(Ordering::SeqCst), 0);
    assert!(fixture.host.warms.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test] // TODO: Test 11 a warm sent with a session token replies 200 and reaches the host, like one sent with the admin key
async fn warming_accepts_a_session_token() -> Result<()> {
    let fixture = Fixture::start_with_existing_host(vec![], true).await?;
    let expiry = (super::super::super::auth::unix_seconds()? + 30) * 1000;
    let session = fixture
        .admin
        .issue_session("default".into(), "subject".into(), expiry)?;
    let response = fixture
        .post(
            "warm-host",
            &session.token,
            json!({"requestId": "warm-1", "warmMs": 60_000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.host.warms.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test] // TODO: Test 12 warming a new id with homeRegion north-america-east creates the actor in north-america-east
async fn warming_places_a_new_actor_in_the_requested_region() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 60_000, "homeRegion": "north-america-east"}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let reply: Value = fixture
        .post("find-actor", "api-key", json!({}))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(reply, json!({"homeRegion": "north-america-east"}));
    Ok(())
}

#[tokio::test] // TODO: Test 13 with the host start held, waitForCompletion false replies 202 at once, and with timeoutMs 100 replies 202 after 100 ms; the host still gets the warm once the start is released
async fn not_waiting_for_completion_replies_202_while_the_host_starts() -> Result<()> {
    for (body, waited) in [
        (
            json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": false}),
            Duration::ZERO,
        ),
        (
            json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": false, "timeoutMs": 100}),
            Duration::from_millis(100),
        ),
    ] {
        let fixture = Fixture::start(vec![]).await?;
        fixture.host.launch_gate.send_replace(false);
        let started = tokio::time::Instant::now();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            fixture.post("warm-host", "api-key", body),
        )
        .await??;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(started.elapsed() >= waited);
        assert_eq!(
            response.json::<Value>().await?,
            json!({"status": "accepted"})
        );
        assert!(fixture.host.warms.lock().unwrap().is_empty());
        fixture.host.launch_gate.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.host.warms.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
    }
    Ok(())
}

#[tokio::test] // TODO: Test 14 with waitForCompletion false and timeoutMs 2000, a start that fails within 2 s replies 503 unavailable, not 202
async fn waiting_with_a_timeout_reports_a_failure_in_time() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    fixture.host.fail_launch.store(true, Ordering::SeqCst);
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 60_000, "waitForCompletion": false, "timeoutMs": 2000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reply: Value = response.json().await?;
    assert_eq!(reply["error"]["code"], "unavailable");
    assert!(fixture.host.warms.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test] // TODO: Test 15 a 0 ms warm to a running host still reaches it
async fn a_zero_ms_warm_still_reaches_the_host() -> Result<()> {
    let fixture = Fixture::start_with_existing_host(vec![], true).await?;
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 0}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let warms = fixture.host.warms.lock().unwrap();
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0]["warmMs"], 0);
    Ok(())
}

#[tokio::test] // TODO: Test 24 a warm sent with the limited key replies 200 and reaches the host, like one sent with the main key
async fn warming_accepts_the_invoke_api_key() -> Result<()> {
    let fixture = Fixture::start_with_existing_host(vec![], true).await?;
    let response = fixture
        .post(
            "warm-host",
            "invoke-key",
            json!({"requestId": "warm-1", "warmMs": 60_000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.host.warms.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test] // TODO: Test 23 warming a sleeping actor starts one host in its saved home, moves the owner epoch to 2, and the host gets the warm
async fn warming_a_sleeping_actor_wakes_it_in_its_saved_home() -> Result<()> {
    let fixture = Fixture::start_with_sleeping_actor(vec![]).await?;
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 60_000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.host.launches.load(Ordering::SeqCst), 1);
    let warms = fixture.host.warms.lock().unwrap();
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0]["ownerEpoch"], 2);
    drop(warms);
    let reply: Value = fixture
        .post("find-actor", "api-key", json!({}))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(reply, json!({"homeRegion": "north-america-east"}));
    Ok(())
}

#[tokio::test] // TODO: Test 25 a warm for a host that is shutting down waits for it, starts one fresh host, and the fresh host gets the warm
async fn warming_a_host_that_is_shutting_down_lands_on_a_fresh_host() -> Result<()> {
    let fixture = Fixture::start_with_draining_actor(vec![]).await?;
    let response = fixture
        .post(
            "warm-host",
            "api-key",
            json!({"requestId": "warm-1", "warmMs": 60_000}),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fixture.host.launches.load(Ordering::SeqCst), 1);
    let warms = fixture.host.warms.lock().unwrap();
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0]["ownerEpoch"], 2);
    Ok(())
}

struct Fixture {
    service: ControlPlaneService,
    gateway: ControlPlaneService,
    placements: Arc<LocalObjectPlacementStore>,
    origin: String,
    replica: String,
    admin: AdminService,
    verifier: ActorJwtVerifier,
    host: Arc<HostFixture>,
    servers: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Fixture {
    async fn start(replies: Vec<(StatusCode, Value)>) -> Result<Self> {
        Self::start_with_existing_host(replies, false).await
    }

    async fn start_with_existing_host(
        replies: Vec<(StatusCode, Value)>,
        existing: bool,
    ) -> Result<Self> {
        Self::start_with_routes(replies, existing, None).await
    }

    async fn start_with_sleeping_actor(replies: Vec<(StatusCode, Value)>) -> Result<Self> {
        let fixture = Self::start(replies).await?;
        fixture.place_chat_room(0).await?;
        Ok(fixture)
    }

    async fn start_with_draining_actor(replies: Vec<(StatusCode, Value)>) -> Result<Self> {
        let fixture = Self::start(replies).await?;
        let expires_at_ms = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? + 300;
        let placement = fixture.place_chat_room(expires_at_ms).await?;
        fixture
            .service
            .routes
            .put(
                &chat_room(),
                crate::control_plane::routes::RouteEntry::Draining(
                    crate::control_plane::routes::RouteIdentity::of(&placement),
                ),
            )
            .await;
        Ok(fixture)
    }

    async fn place_chat_room(&self, expires_at_ms: u64) -> Result<ObjectPlacement> {
        let spec = self.service.registry.launch_spec("default").await?.unwrap();
        let key = chat_room().storage_key();
        self.placements.set_owner(
            &key,
            HostLease {
                expires_at_ms,
                ..test_lease(&HostId::new(format!(
                    "host.v3.{}.fixture",
                    spec.host_config_key()
                )))
            },
            "north-america-east",
        )?;
        Ok(self.placements.get_owner(&key).await?.unwrap())
    }

    async fn start_with_routes(
        replies: Vec<(StatusCode, Value)>,
        existing: bool,
        route_store: Option<Arc<dyn crate::control_plane::routes::RouteStore>>,
    ) -> Result<Self> {
        let host = Arc::new(HostFixture {
            assign_operation: false.into(),
            lose_assignment_reply: false.into(),
            initial_operations: Mutex::new(vec![]),
            socket_requests: Mutex::new(vec![]),
            replies: Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            launches: AtomicUsize::new(0),
            warms: Mutex::new(vec![]),
            fail_launch: false.into(),
            launch_gate: tokio::sync::watch::channel(true).0,
        });
        let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let host_origin = format!("http://{}", host_listener.local_addr()?);
        let routes = Router::new()
            .route(
                "/v1/projects/default/actors/ChatRoom/one/invoke",
                post(host_invoke),
            )
            .route(
                "/v1/projects/default/actors/ChatRoom/one/socket-events",
                post(host_socket_event),
            )
            .route(
                "/v1/projects/default/actors/ChatRoom/one/warm-host",
                post(host_warm),
            )
            .with_state(host.clone());
        let host_server = tokio::spawn(async move { axum::serve(host_listener, routes).await });
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?
            .with_invoke_api_key(Some("invoke-key".into()))?;
        let contract =
            super::super::super::contracts::PublicActorContract::new(serde_json::from_str(
                include_str!("../../../sdk/tests/fixtures/public-contract.json"),
            )?)?;
        admin
            .register_deployment(
                &HostLaunchSpec {
                    image_policy: Default::default(),
                    organization_id: "org-test".into(),
                    sandboxes: Default::default(),
                    project_id: "default".into(),
                    source: None,
                    image_ref: "image".into(),
                    code_snapshot: None,
                    working_directory: "/app".into(),
                    actor_entrypoint: None,
                    secret_refs: vec![],
                },
                Some(&contract),
            )
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        if existing {
            let spec = registry.launch_spec("default").await?.unwrap();
            let actor = ActorKey {
                project_id: "default".into(),
                actor_name: "ChatRoom".into(),
                actor_id: "one".into(),
            };
            placements.set_owner(
                &actor.storage_key(),
                HostLease {
                    route: host_origin.clone(),
                    ..test_lease(&HostId::new(format!(
                        "host.v3.{}.fixture",
                        spec.host_config_key()
                    )))
                },
                "north-america-east",
            )?;
        }
        let mut service = ControlPlaneService::new(
            placements.clone(),
            auth,
            registry,
            issuer.clone(),
            Arc::new(InvocationProvisioner {
                host: host.clone(),
                route: host_origin,
                placements: placements.clone(),
            }),
        );
        if let Some(route_store) = route_store {
            service.routes = route_store;
        }
        let hub = super::super::super::socket_bus::MemorySocketHub::default();
        let (origin, server, gateway) = serve_replica(&service, &admin, &issuer, &hub).await?;
        let (replica, replica_server, _) = serve_replica(&service, &admin, &issuer, &hub).await?;
        Ok(Self {
            service,
            gateway,
            placements,
            origin,
            replica,
            admin,
            verifier,
            host,
            servers: vec![server, replica_server, host_server],
        })
    }

    async fn call(&self, token: &str, body: Value) -> Result<reqwest::Response> {
        self.post("invoke", token, body).await
    }

    async fn post(&self, endpoint: &str, token: &str, body: Value) -> Result<reqwest::Response> {
        post_actor(&self.origin, endpoint, token, body).await
    }

    async fn connect(&self) -> Result<Socket> {
        let grant: Value = self
            .post("find-websocket", "api-key", json!({"metadata":{}}))
            .await?
            .error_for_status()?
            .json()
            .await?;
        let (mut socket, _) =
            tokio_tungstenite::connect_async(grant["websocketUrl"].as_str().unwrap()).await?;
        ensure!(
            next_message(&mut socket).await? == Message::Text("ready".into()),
            "socket connection was not admitted"
        );
        Ok(socket)
    }

    async fn find_actor(&self) -> Result<Value> {
        let reply: Value = self
            .post("find-actor", "api-key", json!({}))
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply, json!({"homeRegion":"north-america-east"}));
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "ChatRoom".into(),
            actor_id: "one".into(),
        };
        let target = self
            .service
            .resolve_actor_route(&actor, None, None, None, None)
            .await?;
        Ok(json!({"token": target.token, "ownerEpoch": target.owner_epoch}))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

async fn serve_replica(
    service: &ControlPlaneService,
    admin: &AdminService,
    issuer: &ActorJwtIssuer,
    hub: &super::super::super::socket_bus::MemorySocketHub,
) -> Result<(
    String,
    tokio::task::JoinHandle<std::io::Result<()>>,
    ControlPlaneService,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let sockets = super::super::super::socket_gateway::SocketGateway::start(
        hub.join(32768),
        true,
        tokio_util::sync::CancellationToken::new(),
        crate::sandbox::connections::HostConnections::new()?.client,
        Arc::new(crate::platform_events::NoPlatformEvents),
    )?;
    let mut service = service.clone();
    service.gateway = Some(super::super::super::gateway::Gateway::new(
        issuer,
        origin.clone(),
        sockets,
    )?);
    let routes = super::super::super::public_api::router(service.clone(), admin.clone());
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    Ok((origin, server, service))
}

async fn post_actor(
    origin: &str,
    endpoint: &str,
    token: &str,
    body: Value,
) -> Result<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!(
            "{origin}/v1/projects/default/actors/ChatRoom/one/{endpoint}"
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?)
}

fn chat_room() -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "ChatRoom".into(),
        actor_id: "one".into(),
    }
}

fn broadcast() -> Value {
    json!({ "effects":[{"type":"broadcast", "message":{"type":"text", "data":"hello"}, "except_connection_ids":[], "tags":[]}]})
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_message(socket: &mut Socket) -> Result<Message> {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next()).await?;
    Ok(message.context("socket closed")??)
}

struct HostFixture {
    assign_operation: std::sync::atomic::AtomicBool,
    lose_assignment_reply: std::sync::atomic::AtomicBool,
    initial_operations: Mutex<Vec<crate::sandbox::assignment::InitialOperation>>,
    socket_requests: Mutex<Vec<String>>,
    replies: Mutex<VecDeque<(StatusCode, Value)>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<(String, Value)>>,
    launches: AtomicUsize,
    warms: Mutex<Vec<Value>>,
    fail_launch: std::sync::atomic::AtomicBool,
    launch_gate: tokio::sync::watch::Sender<bool>,
}

async fn host_invoke(
    State(state): State<Arc<HostFixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    state
        .requests
        .lock()
        .unwrap()
        .push((headers["authorization"].to_str().unwrap().into(), body));
    let (status, reply) = state
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected replay");
    (status, Json(reply)).into_response()
}

async fn host_socket_event(
    State(state): State<Arc<HostFixture>>,
    headers: HeaderMap,
    Json(request): Json<SocketEventRequest>,
) -> Json<SocketEventReply> {
    state
        .socket_requests
        .lock()
        .unwrap()
        .push(headers["authorization"].to_str().unwrap().into());
    let effects = match request.invocation.event {
        ActorSocketEvent::Connect { connection } => vec![ActorSocketEffect::Send {
            connection_id: connection.id,
            message: ActorSocketMessage::Text {
                data: "ready".into(),
            },
        }],
        _ => vec![],
    };
    Json(SocketEventReply::Completed { effects })
}

async fn host_warm(State(state): State<Arc<HostFixture>>, Json(body): Json<Value>) -> StatusCode {
    state.warms.lock().unwrap().push(body);
    StatusCode::OK
}

struct InvocationProvisioner {
    host: Arc<HostFixture>,
    route: String,
    placements: Arc<LocalObjectPlacementStore>,
}
#[async_trait]
impl HostProvisioner for InvocationProvisioner {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _: Option<&HostLaunchSpec>,
        _: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::super::super::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }
    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        _: bool,
        _: Option<&crate::bucket::OwnershipHint>,
        operation: Option<&crate::sandbox::assignment::InitialOperation>,
    ) -> Result<(HostLease, u64, Option<serde_json::Value>)> {
        self.host.launches.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        self.host
            .launch_gate
            .subscribe()
            .wait_for(|open| *open)
            .await?;
        if self.host.fail_launch.load(Ordering::SeqCst) {
            anyhow::bail!("fixture launch failed");
        }
        let lease = HostLease {
            route: self.route.clone(),
            session_id: uuid::Uuid::new_v4().to_string(),
            ..test_lease(&HostId::new(format!(
                "host.v3.{}.fixture",
                spec.host_config_key()
            )))
        };
        let owner_epoch = self
            .placements
            .set_owner(&actor.storage_key(), lease.clone(), region)?;
        let initial_reply = if self.host.assign_operation.load(Ordering::SeqCst) {
            use crate::sandbox::assignment::InitialRequest;
            let operation = operation.context("missing initial operation")?;
            self.host
                .initial_operations
                .lock()
                .unwrap()
                .push(operation.clone());
            if self.host.lose_assignment_reply.load(Ordering::SeqCst) {
                return Err(crate::sandbox::assignment::AssignmentOutcomeUnknown.into());
            }
            Some(match &operation.request {
                InitialRequest::Invoke { .. } => {
                    self.host
                        .replies
                        .lock()
                        .unwrap()
                        .pop_front()
                        .context("unexpected replay")?
                        .1
                }
                InitialRequest::Socket { invocation } => {
                    let ActorSocketEvent::Connect { connection } = &invocation.event else {
                        anyhow::bail!("expected connect")
                    };
                    serde_json::to_value(SocketEventReply::Completed {
                        effects: vec![ActorSocketEffect::Send {
                            connection_id: connection.id.clone(),
                            message: ActorSocketMessage::Text {
                                data: "ready".into(),
                            },
                        }],
                    })?
                }
            })
        } else {
            None
        };
        Ok((lease, owner_epoch, initial_reply))
    }
    async fn terminate_hosts(&self, _: &HostLaunchSpec, _: &[String]) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}
