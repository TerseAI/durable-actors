use super::*;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct Server {
    requests: Arc<Mutex<Vec<Value>>>,
    pages: Arc<Mutex<Vec<HashMap<String, String>>>>,
    fail: bool,
}
async fn insert(State(server): State<Server>, Json(body): Json<Value>) -> Json<Value> {
    server.requests.lock().unwrap().push(body);
    Json(json!({}))
}
async fn results(
    State(server): State<Server>,
    Path(_job): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    server.pages.lock().unwrap().push(query);
    if server.fail {
        return Json(
            json!({"jobComplete": true, "errors": [{"reason": "billingTierLimitExceeded"}]}),
        );
    }
    Json(
        json!({"jobComplete": true, "cacheHit": true, "totalBytesProcessed": "0", "rows": [{"f": [{"v": "{\"actorName\":\"Room\",\"count\":1}"}]}], "pageToken": "next"}),
    )
}
async fn cancel(State(server): State<Server>, Path(job): Path<String>) -> Json<Value> {
    server.requests.lock().unwrap().push(json!({"cancel":job}));
    Json(json!({}))
}
async fn transport(
    server: Server,
) -> Result<(
    BigQueryTransport,
    tokio::task::JoinHandle<std::io::Result<()>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut client = BigQueryTransport::new(
        google_cloud_auth::credentials::anonymous::Builder::new().build(),
        "billing".into(),
        "US".into(),
        12345,
    )?;
    client.endpoint = format!("http://{}", listener.local_addr()?);
    let routes = Router::new()
        .route("/projects/billing/jobs", post(insert))
        .route("/projects/billing/queries/{job}", get(results))
        .route("/projects/billing/jobs/{job}/cancel", post(cancel))
        .with_state(server);
    let task = tokio::spawn(async move { axum::serve(listener, routes).await });
    Ok((client, task))
}
fn spec() -> QuerySpec {
    QuerySpec {
        sql: "SELECT @project_id".into(),
        parameters: std::collections::BTreeMap::from([
            ("project_id".into(), "tenant".into()),
            ("from_ms".into(), 1000.into()),
            ("actor_name".into(), Value::Null),
        ]),
        limit: 12,
    }
}

#[tokio::test]
async fn jobs_use_cache_budgets_named_parameters_and_page_the_same_result() -> Result<()> {
    let server = Server::default();
    let (client, task) = transport(server.clone()).await?;
    let page = client.query(spec()).await?;
    assert_eq!(page.rows[0]["count"], 1);
    let next = page.next.unwrap();
    let job = next.job_id.clone();
    client.page(next, 12).await?;
    let requests = server.requests.lock().unwrap();
    let config = &requests[0]["configuration"];
    assert_eq!(config["query"]["useQueryCache"], true);
    assert_eq!(config["query"]["maximumBytesBilled"], "12345");
    assert_eq!(config["jobTimeoutMs"], "20000");
    assert_eq!(config["query"]["parameterMode"], "NAMED");
    assert_eq!(
        config["query"]["queryParameters"][1]["parameterType"]["type"],
        "INT64"
    );
    assert_eq!(requests[0]["jobReference"]["jobId"], job);
    assert_eq!(server.pages.lock().unwrap()[1]["pageToken"], "next");
    task.abort();
    Ok(())
}

#[tokio::test]
async fn query_errors_are_not_cached_or_returned_as_empty_success() -> Result<()> {
    let server = Server {
        fail: true,
        ..Default::default()
    };
    let (client, task) = transport(server.clone()).await?;
    assert!(client.query(spec()).await.is_err());
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.get("cancel").is_some())
    );
    task.abort();
    Ok(())
}
