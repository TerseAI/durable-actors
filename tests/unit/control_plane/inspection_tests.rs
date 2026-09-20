use std::{sync::Arc, time::Duration};

use anyhow::Result;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    actor::ActorKey,
    bucket::{Bucket, RuntimeStorage, testing::RuntimeFixture},
    host::HostId,
    host_leases::HostLeaseRequest,
    placement::ObjectPlacementStore,
    state_log::StateSnapshot,
    state_transport::SnapshotWriter,
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
async fn actor_inspection_separates_metadata_from_optional_state() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("one");
    fixture.save(&actor, 1, json!({"value": 7})).await?;
    let response = fixture
        .get("/v1/projects/default/actors/Room.with.dots/one")
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let metadata: Value = response.json().await?;
    assert_eq!(metadata["homeRegion"], "north-america-east");
    assert!(metadata.get("state").is_none());
    let state: Value = fixture
        .get("/v1/projects/default/actors/Room.with.dots/one?include=state")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(state["state"]["value"], 7);
    let page: Value = fixture
        .get("/v1/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["actors"][0]["actorId"], "one");
    for path in [
        "/v1/objects",
        "/v1/projects/default/actors/Room.with.dots/one/state",
        "/v1/projects/default/actors/Room.with.dots/one/placement",
    ] {
        assert_eq!(fixture.get(path).await?.status(), StatusCode::NOT_FOUND);
    }
    assert_eq!(
        fixture
            .get("/v1/projects/default/actors/Room.with.dots/one?include=unknown")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn inspection_reads_persisted_state_without_a_deployment_or_live_host() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("one");
    fixture
        .save(&actor, 1, json!({"internal": {"password": "saved"}}))
        .await?;
    fixture
        .store
        .release_activation(&actor, &fixture.host, &actor.actor_id)
        .await?;
    let before = fixture.store.get(&actor.storage_key()).await?;

    let response = fixture.get("/v1/actors?limit=1").await?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    let page: Value = response.error_for_status()?.json().await?;
    assert_eq!(page["actors"][0]["actorName"], "Room.with.dots");
    assert_eq!(page["actors"][0]["actorId"], "one");
    assert_eq!(page["actors"][0]["stateVersion"], 1);
    assert_eq!(page["nextCursor"], Value::Null);

    let response = fixture
        .get("/v1/projects/default/actors/Room.with.dots/one?include=state")
        .await?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    let inspected: Value = response.error_for_status()?.json().await?;
    assert_eq!(inspected["stateVersion"], 1);
    assert_eq!(inspected["state"]["internal"]["password"], "saved");
    assert_eq!(fixture.store.get(&actor.storage_key()).await?, before);
    Ok(())
}

#[tokio::test]
async fn inspection_requires_admin_credentials_and_validates_queries_and_missing_state()
-> Result<()> {
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
    for credential in ["", "wrong", &token] {
        assert_eq!(
            fixture
                .client
                .post(format!("{}/v1/observe/query", fixture.origin))
                .bearer_auth(credential)
                .json(&json!({"sql":"SELECT 1"}))
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for path in [
        "/v1/observe/actors",
        "/v1/observe/events",
        "/v1/observe/requests/events",
        "/v1/actors",
        "/v1/projects/default/actors/Room.with.dots/one?include=state",
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
    for query in [
        "limit=0",
        "limit=501",
        "limit=-1",
        "after=bad%2Fcursor",
        "unknown=true",
    ] {
        assert_eq!(
            fixture.get(&format!("/v1/actors?{query}")).await?.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        fixture
            .get("/v1/projects/default/actors/Room.with.dots/missing?include=state")
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let actor = fixture.actor("empty");
    fixture.activate(&actor).await?;
    let response: Value = fixture
        .get("/v1/projects/default/actors/Room.with.dots/empty?include=state")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(response["stateVersion"], 0);
    assert_eq!(response["state"], Value::Null);
    Ok(())
}

#[tokio::test]
async fn inspection_pages_all_actors_without_skipping_results() -> Result<()> {
    let fixture = Fixture::start().await?;
    for id in ["a", "b", "c"] {
        let actor = fixture.actor(id);
        fixture.commit(&actor, json!({"value": id})).await?;
    }
    let first: Value = fixture
        .get("/v1/actors?limit=1")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["actors"].as_array().unwrap().len(), 1);
    assert_eq!(first["actors"][0]["actorId"], "a");
    let cursor = first["nextCursor"].as_str().unwrap();
    let second: Value = fixture
        .get(&format!("/v1/actors?limit=1&after={cursor}"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(second["actors"][0]["actorId"], "b");
    assert!(second["nextCursor"].is_string());
    let global: Value = fixture
        .get("/v1/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(global["actors"].as_array().unwrap().len(), 3);
    Ok(())
}

#[tokio::test]
async fn inspection_reports_inconsistent_snapshots_instead_of_returning_state() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("one");
    fixture.commit(&actor, json!({"internal": "saved"})).await?;
    let placement = fixture.store.get(&actor.storage_key()).await?.unwrap();
    let object = placement.state_object.unwrap();
    let stored = fixture.runtime.bucket.get(&object).await?.unwrap();
    let wrong = StateSnapshot::new(
        2,
        1,
        "request-1".into(),
        json!({"internal": "uncommitted"}),
        Value::Null,
    )?;
    fixture
        .runtime
        .bucket
        .compare_and_swap(&object, Some(stored.generation), wrong.encode()?)
        .await?;
    let response = fixture
        .get("/v1/projects/default/actors/Room.with.dots/one?include=state")
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!response.text().await?.contains("uncommitted"));
    Ok(())
}

struct Fixture {
    traces: crate::request_traces::TraceStore,
    changes: tokio::sync::watch::Sender<()>,
    admin: AdminService,
    runtime: RuntimeFixture,
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
            ActorInspector::new(store.clone(), store.clone(), store.clone(), changes.clone())
                .with_traces(traces.clone());
        let routes = super::inspection::router(inspector, admin.clone());
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host.v3.test");
        Ok(Self {
            traces,
            changes,
            admin,
            runtime,
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

    async fn activate(&self, actor: &ActorKey) -> Result<crate::bucket::LoadedActor> {
        self.store
            .register_activation(
                actor,
                &HostLeaseRequest {
                    id: self.host.clone(),
                    session_id: actor.actor_id.clone(),
                    route: "http://localhost:7101".into(),
                    duration_ms: 60_000,
                },
                "north-america-east",
                true,
            )
            .await
    }

    async fn save(&self, actor: &ActorKey, version: u64, state: Value) -> Result<String> {
        let placement = self.activate(actor).await?.placement;
        let ticket = self
            .store
            .prepare_actor_write(actor, &placement.lease, placement.owner_epoch, version)
            .await?;
        let snapshot =
            StateSnapshot::new(version, 1, format!("request-{version}"), state, Value::Null)?;
        SnapshotWriter::write_snapshot(self.store.as_ref(), &ticket, snapshot.encode()?).await?;
        Ok(ticket.object_name)
    }

    async fn get(&self, path: &str) -> Result<reqwest::Response> {
        Ok(self
            .client
            .get(format!("{}{path}", self.origin))
            .bearer_auth("api-key")
            .send()
            .await?)
    }

    async fn commit(&self, actor: &ActorKey, state: Value) -> Result<()> {
        self.save(actor, 1, state).await?;
        Ok(())
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
                code_revision: "revision".into(),
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
async fn request_history_executes_read_only_sql_and_resumes_saved_cursors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let url = format!("{}/v1/observe/query", fixture.origin);
    let query = json!({"sql":"SELECT COUNT(*) AS total FROM request_events", "params":[]});
    assert_eq!(
        fixture
            .client
            .post(&url)
            .json(&query)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = fixture
        .client
        .post(&url)
        .bearer_auth("api-key")
        .json(&query)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await?,
        json!({"rows":[{"total":0}],"truncated":false})
    );
    for query in [
        json!({"sql":"DELETE FROM traces"}),
        json!({"sql":"SELECT * FROM missing_table"}),
        json!({"sql":"SELECT ?", "params":[{}]}),
        json!({"sql":"SELECT 1; SELECT 2"}),
    ] {
        assert_eq!(
            fixture
                .client
                .post(&url)
                .bearer_auth("api-key")
                .json(&query)
                .send()
                .await?
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
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
