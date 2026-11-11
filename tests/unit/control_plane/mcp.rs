use std::sync::{Arc, Mutex};

use axum::{
    Json,
    http::{HeaderMap, Uri},
    routing::{get, post},
};

use super::*;

#[tokio::test]
async fn lists_developer_tools() {
    let reply = rpc(local_router(Router::new()), "tools/list", json!({})).await;
    let tools = reply["result"]["tools"].as_array().expect("tools");
    assert_eq!(tools.len(), 9);
    assert!(tools.iter().all(|tool| tool["outputSchema"].is_object()));
    let tool = |name: &str| tools.iter().find(|tool| tool["name"] == name).expect(name);
    assert_eq!(tool("invoke_actor")["annotations"]["destructiveHint"], true);
    assert_eq!(tool("invoke_actor")["annotations"]["openWorldHint"], true);
    assert_eq!(tool("list_logs")["annotations"]["openWorldHint"], false);
}

#[tokio::test]
async fn tool_calls_forward_the_credential_and_filters() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let api = Router::new().route(
        "/v1/projects/demo/observe/requests",
        get({
            let seen = seen.clone();
            move |headers: HeaderMap, uri: Uri| async move {
                seen.lock().unwrap().push((
                    headers[header::AUTHORIZATION].to_str().unwrap().to_owned(),
                    uri.query().unwrap_or_default().to_owned(),
                ));
                Json(json!({ "records": [], "nextCursor": null }))
            }
        }),
    );
    let arguments = json!({ "actorName": "Counter", "outcome": "failed" });
    let reply = call_tool(api, "list_requests", arguments).await;

    assert_eq!(reply["result"]["isError"], false);
    assert_eq!(text(&reply), json!({ "records": [] }));
    assert_eq!(
        reply["result"]["structuredContent"],
        json!({ "records": [] })
    );
    let (authorization, query) = seen.lock().unwrap().pop().expect("API call");
    assert_eq!(authorization, "Bearer secret");
    for pair in ["actorName=Counter", "outcome=failed", "limit=25"] {
        assert!(query.split('&').any(|item| item == pair), "{query}");
    }
}

#[tokio::test]
async fn invoke_returns_the_outcome_without_the_host_target() {
    let seen = Arc::new(Mutex::new(None));
    let api = Router::new().route(
        "/v1/projects/demo/actors/Counter/one/invoke",
        post({
            let seen = seen.clone();
            move |Json(body): Json<Value>| async move {
                *seen.lock().unwrap() = Some(body);
                Json(json!({
                    "target": { "token": "host-ticket" },
                    "outcome": { "type": "completed", "result": 7 }
                }))
            }
        }),
    );
    let arguments =
        json!({ "actorName": "Counter", "actorId": "one", "method": "increment", "args": [2] });
    let reply = call_tool(api, "invoke_actor", arguments).await;

    assert_eq!(text(&reply), json!({ "status": "completed", "result": 7 }));
    let body = seen.lock().unwrap().take().expect("invocation");
    assert_eq!(body["method"], "increment");
    assert_eq!(body["args"], json!([2]));
    assert!(body["requestId"].as_str().is_some_and(|id| !id.is_empty()));
}

#[tokio::test]
async fn sessions_page_with_the_status_gateways_reported() {
    let sessions = json!([
        { "connectionId": "open-one", "actorName": "Chat", "actorId": "one", "status": "open",
            "openedAtMs": 1_794_330_000_000_u64, "closedAtMs": null },
        { "connectionId": "lost-one", "actorName": "Chat", "actorId": "one", "status": "lost",
            "openedAtMs": 1_794_330_000_000_u64, "closedAtMs": 1_794_330_060_000_u64 },
    ]);
    let api = Router::new().route(
        "/v1/projects/demo/observe/websockets",
        get(move || async move { Json(sessions) }),
    );
    let first = text(
        &call_tool(
            api.clone(),
            "list_websocket_sessions",
            json!({ "limit": 1 }),
        )
        .await,
    );
    let cursor = first["nextCursor"].as_str().expect("cursor").to_owned();
    let second =
        text(&call_tool(api, "list_websocket_sessions", json!({ "cursor": cursor })).await);

    assert_eq!(first["total"], 2);
    assert_eq!(first["records"][0]["status"], "open");
    assert_eq!(first["records"][0]["openedAt"], "2026-11-10T17:00:00.000Z");
    assert!(first["records"][0].get("closedAt").is_none());
    assert_eq!(second["records"][0]["status"], "lost");
    assert_eq!(second["records"][0]["closedAt"], "2026-11-10T17:01:00.000Z");
    assert!(second.get("nextCursor").is_none());
}

#[tokio::test]
async fn invalid_cursors_are_tool_errors_the_model_can_fix() {
    let reply = call_tool(
        Router::new(),
        "list_actors",
        json!({ "cursor": "not-a-cursor" }),
    )
    .await;

    assert_eq!(reply["result"]["isError"], true);
    assert_eq!(text(&reply)["error"]["code"], "invalid_request");
}

async fn call_tool(api: Router, name: &str, arguments: Value) -> Value {
    let params = json!({ "name": name, "arguments": arguments });
    rpc(local_router(api), "tools/call", params).await
}

fn text(reply: &Value) -> Value {
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .expect("text");
    serde_json::from_str(text).expect("JSON text")
}

async fn rpc(router: Router, method: &str, mut params: Value) -> Value {
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "1.0" },
        "io.modelcontextprotocol/clientCapabilities": {}
    });
    let mut request = Request::post("/v1/projects/demo/mcp")
        .header(header::HOST, "localhost")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::AUTHORIZATION, "Bearer secret")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    if let Some(name) = params["name"].as_str() {
        request = request.header("mcp-name", name);
    }
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let request = request.body(Body::from(body.to_string())).unwrap();
    let Ok(response) = router.oneshot(request).await;
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).expect("JSON-RPC reply")
}
