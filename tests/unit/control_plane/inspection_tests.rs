use std::{sync::Arc, time::Duration};

use anyhow::Result;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    actor::ActorKey,
    bucket::{RuntimeStorage, testing::RuntimeFixture},
    host::HostId,
    host_leases::HostLeaseRequest,
};

use super::{ActorJwtIssuer, admin::AdminService, inspection::ActorInspector};

#[tokio::test]
async fn durability_endpoint_is_not_exposed() -> Result<()> {
    let fixture = Fixture::start().await?;
    assert_eq!(
        fixture.get("/v1/durability").await?.status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test]
async fn observability_requires_admin_credentials() -> Result<()> {
    let fixture = Fixture::start().await?;
    let token = fixture
        .issuer
        .issue_host(
            &fixture.host,
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "north-america-east",
            &fixture.actor("one"),
        )?
        .token;
    for path in [
        "/v1/observe/actors",
        "/v1/observe/events",
        "/v1/observe/requests/events",
        "/v1/observe/requests",
    ] {
        for credential in ["", "wrong", &token] {
            assert_eq!(
                fixture
                    .client
                    .get(format!("{}{path}", fixture.origin))
                    .bearer_auth(credential)
                    .send()
                    .await?
                    .status(),
                StatusCode::UNAUTHORIZED
            );
        }
    }
    Ok(())
}

struct Fixture {
    traces: crate::request_traces::TraceStore,
    changes: tokio::sync::watch::Sender<()>,
    admin: AdminService,
    _runtime: RuntimeFixture,
    store: Arc<RuntimeStorage>,
    host: HostId,
    issuer: ActorJwtIssuer,
    origin: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn start() -> Result<Self> {
        Self::start_with_traces(crate::request_traces::TraceStore::default()).await
    }

    async fn start_with_traces(traces: crate::request_traces::TraceStore) -> Result<Self> {
        let runtime = RuntimeFixture::new()?;
        let store = runtime.runtime.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let admin = AdminService::new(
            "api-key".into(),
            Arc::new(super::admin::LocalAdminRegistry::default()),
            issuer.clone(),
        )?;
        let changes = tokio::sync::watch::channel(()).0;
        let inspector =
            ActorInspector::new(store.clone(), changes.clone()).with_traces(traces.clone());
        let routes = super::inspection::router(inspector, admin.clone());
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host.v3.test");
        Ok(Self {
            traces,
            changes,
            admin,
            _runtime: runtime,
            store,
            host,
            issuer,
            origin,
            client: reqwest::Client::new(),
            server,
        })
    }

    fn actor(&self, id: &str) -> ActorKey {
        ActorKey {
            project_id: "default".into(),
            actor_name: "Room.with.dots".into(),
            actor_id: id.into(),
        }
    }

    async fn get(&self, path: &str) -> Result<reqwest::Response> {
        Ok(self
            .client
            .get(format!("{}{path}", self.origin))
            .bearer_auth("api-key")
            .send()
            .await?)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn inventory_stream_reports_host_sockets_and_fences_expired_sessions() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("unsaved");
    let request = HostLeaseRequest {
        id: fixture.host.clone(),
        session_id: "session".into(),
        route: "http://localhost:7101".into(),
        duration_ms: 60_000,
    };
    fixture
        .store
        .register_activation(&actor, &request, "north-america-east", true)
        .await?;
    let mut stream = fixture
        .get("/v1/observe/events")
        .await?
        .error_for_status()?;
    let first = stream_inventory(&mut stream).await?;
    assert!(first.get("namespaceId").is_none());
    assert_eq!(first["actors"][0]["unknown"], 1);
    let queues = vec![crate::host_leases::ActorQueueInventory {
        actor: actor.clone(),
        waiting: vec![crate::host_leases::WaitingOperation {
            id: "queued-1".into(),
            operation: "sendMessage".into(),
        }],
    }];
    let mut sockets = vec![crate::host_leases::ActorSocketInventory {
        actor: actor.clone(),
        connections: vec![crate::actor::ActorSocketConnection {
            id: "socket-one".into(),
            metadata: json!({"userId":"ada"}),
            tags: vec![],
        }],
    }];
    fixture
        .store
        .renew_activation(
            &actor,
            &request,
            crate::host_leases::ActivationInventory {
                resident: Some(true),
                connections: sockets[0].connections.clone(),
                waiting: Some(queues[0].waiting.clone()),
            },
        )
        .await?;
    fixture.changes.send_replace(());
    let connected = stream_inventory(&mut stream).await?;
    assert_eq!(connected["actors"][0]["live"], 1);
    assert_eq!(
        connected["actors"][0]["instances"][0]["waiting"][0]["operation"],
        "sendMessage"
    );
    assert_eq!(
        connected["actors"][0]["instances"][0]["connections"],
        json!([{"id":"socket-one", "metadata":{"userId":"ada"}}])
    );
    sockets[0].connections[0].metadata = json!({"userId":"grace"});
    fixture
        .store
        .renew_activation(
            &actor,
            &request,
            crate::host_leases::ActivationInventory {
                resident: Some(true),
                connections: sockets[0].connections.clone(),
                waiting: Some(queues[0].waiting.clone()),
            },
        )
        .await?;
    fixture.changes.send_replace(());
    let updated = stream_inventory(&mut stream).await?;
    assert_eq!(
        updated["actors"][0]["instances"][0]["connections"][0]["metadata"]["userId"],
        "grace"
    );
    fixture
        .store
        .release_activation(&actor, &fixture.host, "session")
        .await?;
    fixture.changes.send_replace(());
    let expired = stream_inventory(&mut stream).await?;
    assert_eq!(expired["actors"][0]["dormant"], 1);
    assert_eq!(expired["actors"][0]["instances"][0]["waiting"], json!([]));
    assert_eq!(
        expired["actors"][0]["instances"][0]["connections"],
        json!([])
    );
    let replacement = HostLeaseRequest {
        session_id: "replacement".into(),
        ..request
    };
    assert!(
        fixture
            .store
            .renew_activation(&actor, &replacement, Default::default())
            .await
            .is_err()
    );
    let inventory: Value = fixture
        .get("/v1/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(inventory["actors"][0]["dormant"], 1);
    assert_eq!(inventory["actors"][0]["instances"][0]["waiting"], json!([]));
    assert_eq!(
        inventory["actors"][0]["instances"][0]["connections"],
        json!([])
    );
    Ok(())
}

async fn stream_inventory(stream: &mut reqwest::Response) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            bytes.extend(
                stream
                    .chunk()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("stream ended"))?,
            );
            let text = String::from_utf8_lossy(&bytes);
            if text.ends_with("\n\n") {
                if let Some(data) = text.lines().find_map(|line| line.strip_prefix("data: ")) {
                    return serde_json::from_str(data).map_err(Into::into);
                }
            }
        }
    })
    .await?
}

#[tokio::test]
async fn inventory_includes_unused_deployed_types_without_loading_actors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let contract = super::contracts::PublicActorContract::new(serde_json::from_str(
        include_str!("../../../sdk/tests/fixtures/public-contract.json"),
    )?)?;
    fixture
        .admin
        .register_deployment(
            &super::admin::HostLaunchSpec {
                project_id: "default".into(),
                source: None,
                code_snapshot: Some("im-code".into()),
                image_ref: "test-image".into(),
                working_directory: "/customer".into(),
                actor_entrypoint: Some("actors.mjs".into()),
                secret_refs: vec![],
            },
            Some(&contract),
        )
        .await?;
    let inventory: Value = fixture
        .get("/v1/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    let row = inventory["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["actorName"] == "ChatRoom")
        .unwrap();
    assert_eq!(row["live"], 0);
    assert_eq!(row["dormant"], 0);
    assert_eq!(row["instances"], json!([]));
    Ok(())
}

#[tokio::test]
async fn request_history_streams_distinct_records_and_replays_on_reconnect() -> Result<()> {
    use crate::request_traces::{
        RequestKind, RequestOutcome, RequestTrace, TraceStore, persistence::SqliteTracePersistence,
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("request-traces.sqlite3");
    let open = || TraceStore::open(Arc::new(SqliteTracePersistence::new(path.clone())));
    let fixture = Fixture::start_with_traces(open().await?).await?;
    let mut stream = fixture
        .get("/v1/observe/requests/events")
        .await?
        .error_for_status()?;
    assert_eq!(stream.headers()["cache-control"], "no-store");
    let empty = stream_inventory(&mut stream).await?;
    assert_eq!(empty["records"], json!([]));
    for id in ["first", "second"] {
        fixture
            .traces
            .record(
                "host",
                "session",
                vec![RequestTrace {
                    request_id: id.into(),
                    actor_name: "Room".into(),
                    actor_id: "one".into(),
                    kind: RequestKind::Method,
                    operation: "post".into(),
                    connection_id: None,
                    started_at_ms: 1000,
                    duration_ms: 25.0,
                    queue_wait_ms: Some(10.0),
                    outcome: RequestOutcome::Completed,
                }],
                0,
            )
            .await?;
        let page = stream_inventory(&mut stream).await?;
        assert_eq!(page["records"].as_array().unwrap().len(), 1);
        assert_eq!(page["records"][0]["requestId"], id);
        assert_eq!(page["records"][0]["queueWaitMs"], 10.0);
        assert_eq!(page["epoch"], empty["epoch"]);
    }
    drop(stream);
    let snapshot = serde_json::to_value(fixture.traces.replay(&Default::default()).await?)?;
    let mut replay = fixture.get("/v1/observe/requests/events").await?;
    assert_eq!(stream_inventory(&mut replay).await?, snapshot);
    assert_eq!(snapshot["records"].as_array().unwrap().len(), 2);
    drop(replay);
    drop(fixture);
    let restarted = Fixture::start_with_traces(open().await?).await?;
    let mut replay = restarted.get("/v1/observe/requests/events").await?;
    let restored = stream_inventory(&mut replay).await?;
    assert_eq!(restored["records"], snapshot["records"]);
    assert_eq!(restored["epoch"], snapshot["epoch"]);
    Ok(())
}

#[tokio::test]
async fn request_history_validates_filters_and_returns_empty_pages() -> Result<()> {
    let fixture = Fixture::start().await?;
    let response = fixture.get("/v1/observe/requests").await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let page: Value = response.json().await?;
    assert_eq!(page["records"], json!([]));
    assert_eq!(page["nextCursor"], Value::Null);
    assert_eq!(page["capacity"], 100);
    for query in [
        "limit=0",
        "limit=501",
        "limit=-1",
        "limit=1.5",
        "outcome=unknown",
        "fromMs=-1",
        "fromMs=9007199254740992",
        "fromMs=2&toMs=1",
        "actorName=",
        "actorId=bad%2Fid",
        "cursor=broken",
        "unknown=true",
    ] {
        assert_eq!(
            fixture
                .get(&format!("/v1/observe/requests?{query}"))
                .await?
                .status(),
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn request_history_filters_and_pages_without_including_later_appends() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (id, name, actor, time, outcome) in [
        (
            "old",
            "Room",
            "one",
            999,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "first",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "second",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "other-class",
            "Counter",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "other-actor",
            "Room",
            "two",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "completed",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Completed,
        ),
        (
            "future",
            "Room",
            "one",
            1001,
            crate::request_traces::RequestOutcome::Failed,
        ),
    ] {
        record_request(&fixture, id, name, actor, time, outcome).await?;
    }
    let filters = "actorName=Room&actorId=one&outcome=failed&fromMs=1000&toMs=1000&limit=1";
    let first: Value = fixture
        .get(&format!("/v1/observe/requests?{filters}"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["records"].as_array().unwrap().len(), 1);
    assert_eq!(first["records"][0]["requestId"], "second");
    let cursor = first["nextCursor"].as_str().unwrap();
    record_request(
        &fixture,
        "late",
        "Room",
        "one",
        1000,
        crate::request_traces::RequestOutcome::Failed,
    )
    .await?;
    let next: Value = fixture
        .get(&format!("/v1/observe/requests?{filters}&cursor={cursor}"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(next["records"].as_array().unwrap().len(), 1);
    assert_eq!(next["records"][0]["requestId"], "first");
    assert_eq!(next["nextCursor"], Value::Null);
    assert_eq!(next["reset"], false);
    assert_eq!(
        fixture
            .get(&format!("/v1/observe/requests?actorId=two&cursor={cursor}"))
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn record_request(
    fixture: &Fixture,
    id: &str,
    actor_name: &str,
    actor_id: &str,
    time: u64,
    outcome: crate::request_traces::RequestOutcome,
) -> Result<()> {
    fixture
        .traces
        .record(
            "host",
            "session",
            vec![crate::request_traces::RequestTrace {
                request_id: id.into(),
                actor_name: actor_name.into(),
                actor_id: actor_id.into(),
                kind: crate::request_traces::RequestKind::Method,
                operation: "post".into(),
                connection_id: None,
                started_at_ms: time,
                duration_ms: 25.0,
                queue_wait_ms: Some(10.0),
                outcome,
            }],
            0,
        )
        .await
}

#[tokio::test]
async fn request_stream_resumes_saved_cursors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let snapshot = serde_json::to_value(fixture.traces.replay(&Default::default()).await?)?;
    let cursor = snapshot["resumeCursor"].as_str().unwrap();
    let mut replay = fixture
        .get(&format!("/v1/observe/requests/events?after={cursor}"))
        .await?;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(stream_inventory(&mut replay).await?["records"], json!([]));
    assert_eq!(
        fixture
            .get("/v1/observe/requests/events?after=broken")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}
