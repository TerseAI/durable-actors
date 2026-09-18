use std::{sync::Arc, time::Duration};

use anyhow::Result;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    actor::{ActorKey, ActorSocketConnection},
    bucket::{Bucket, RuntimeStorage, testing::RuntimeFixture},
    clock::SystemClock,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
    placement::ObjectPlacementStore,
    state_log::StateSnapshot,
    state_transport::SnapshotWriter,
};

use super::{
    ActorJwtIssuer, admin::AdminService, inspection::ActorInspector, websocket::SocketRegistry,
};

#[tokio::test]
async fn inspection_reads_persisted_state_without_a_deployment_or_live_host() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("one");
    fixture
        .save(&actor, 1, json!({"internal": {"password": "saved"}}))
        .await?;
    fixture
        .runtime
        .leases
        .unregister(&fixture.host, "session")
        .await?;
    let before = fixture.store.get(&actor.storage_key()).await?;

    let response = fixture
        .get("/v1/objects?namespace=team.prod&limit=1")
        .await?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    let page: Value = response.error_for_status()?.json().await?;
    assert_eq!(page["objects"][0]["namespaceId"], "team.prod");
    assert_eq!(page["objects"][0]["actorType"], "Room.with.dots");
    assert_eq!(page["objects"][0]["actorId"], "one");
    assert_eq!(page["objects"][0]["stateVersion"], 1);
    assert_eq!(page["nextCursor"], Value::Null);

    let response = fixture
        .get("/v1/namespaces/team.prod/actors/Room.with.dots/one/state")
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
        .issue_workflow(
            "team.prod",
            "run",
            "north-america-east",
            i64::try_from(crate::clock::Clock::now_ms(&SystemClock)?)? + 30_000,
        )?
        .token;
    for path in [
        "/v1/durability",
        "/v1/observe/actors",
        "/v1/objects",
        "/v1/namespaces/team.prod/actors/Room.with.dots/one/state",
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
    let policy: Value = fixture
        .get("/v1/durability")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(policy["mode"], "object_storage");
    assert_eq!(policy["replicaCount"], 0);
    for query in [
        "limit=0",
        "limit=501",
        "namespace=bad%2Fnamespace",
        "after=bad%2Fcursor",
        "unknown=true",
    ] {
        assert_eq!(
            fixture.get(&format!("/v1/objects?{query}")).await?.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        fixture
            .get("/v1/actors/Room.with.dots/missing/state")
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let actor = fixture.actor("empty");
    fixture
        .store
        .claim_actor(&actor, None, &fixture.host, "north-america-east")
        .await?;
    let response: Value = fixture
        .get("/v1/actors/Room.with.dots/empty/state")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(response["stateVersion"], 0);
    assert_eq!(response["state"], Value::Null);
    Ok(())
}

#[tokio::test]
async fn inspection_pages_global_results_and_preserves_exact_namespace_boundaries() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (namespace, id) in [
        ("team.prod", "a"),
        ("team.prod", "b"),
        ("team.prod.nested", "c"),
    ] {
        let actor = ActorKey {
            namespace_id: namespace.into(),
            ..fixture.actor(id)
        };
        fixture.commit(&actor, json!({"value": id})).await?;
    }
    let first: Value = fixture
        .get("/v1/objects?namespace=team.prod&limit=1")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["objects"].as_array().unwrap().len(), 1);
    assert_eq!(first["objects"][0]["actorId"], "a");
    let cursor = first["nextCursor"].as_str().unwrap();
    let second: Value = fixture
        .get(&format!(
            "/v1/objects?namespace=team.prod&limit=1&after={cursor}"
        ))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(second["objects"][0]["actorId"], "b");
    assert_eq!(second["nextCursor"], Value::Null);
    let global: Value = fixture
        .get("/v1/objects")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(global["objects"].as_array().unwrap().len(), 3);
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
    let response = fixture.get("/v1/actors/Room.with.dots/one/state").await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!response.text().await?.contains("uncommitted"));
    Ok(())
}

#[tokio::test]
async fn inventory_counts_unsaved_instances_and_excludes_other_namespaces() -> Result<()> {
    let fixture = Fixture::start().await?;
    for id in ["one", "unsaved"] {
        fixture
            .store
            .claim_actor(
                &fixture.actor(id),
                None,
                &fixture.host,
                "north-america-east",
            )
            .await?;
    }
    let other = ActorKey {
        namespace_id: "team.prod.nested".into(),
        ..fixture.actor("other")
    };
    fixture
        .store
        .claim_actor(&other, None, &fixture.host, "north-america-east")
        .await?;
    fixture
        .runtime
        .leases
        .register_with_residents(
            &HostLeaseRequest {
                id: fixture.host.clone(),
                session_id: "session".into(),
                route: "http://localhost:7101".into(),
                duration_ms: 60_000,
            },
            Some(&[fixture.actor("one")]),
        )
        .await?;
    fixture.connect(&fixture.actor("one")).await;
    let response = fixture.get("/v1/observe/actors").await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let inventory: Value = response.json().await?;
    assert_eq!(inventory["namespaceId"], "team.prod");
    assert_eq!(
        inventory["actors"],
        json!([{
            "actorType": "Room.with.dots",
            "live": 1,
            "dormant": 1,
            "unknown": 0,
            "instances": [
                {
                    "actorId": "one",
                    "status": "live",
                    "connections": [{"id": "socket-one", "metadata": {"userId": "ada"}}]
                },
                {"actorId": "unsaved", "status": "dormant", "connections": []}
            ]
        }])
    );
    fixture
        .runtime
        .leases
        .unregister(&fixture.host, "session")
        .await?;
    let inventory: Value = fixture
        .get("/v1/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(inventory["actors"][0]["live"], 0);
    assert_eq!(inventory["actors"][0]["dormant"], 2);
    fixture
        .runtime
        .leases
        .register(&HostLeaseRequest {
            id: fixture.host.clone(),
            session_id: "replacement".into(),
            route: "http://localhost:7101".into(),
            duration_ms: 60_000,
        })
        .await?;
    let inventory: Value = fixture
        .get("/v1/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        inventory["actors"][0]["live"], 0,
        "a new host session must not revive old actors"
    );
    assert_eq!(
        fixture
            .get("/v1/observe/actors?namespace=bad%2Fnamespace")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn inventory_includes_unused_deployed_types_and_reports_missing_telemetry() -> Result<()> {
    let fixture = Fixture::start().await?;
    let contract = super::contracts::PublicActorContract::new(serde_json::from_str(
        include_str!("../../sdk/fixtures/public-contract.json"),
    )?)?;
    fixture
        .admin
        .register_deployment(
            &super::admin::HostLaunchSpec {
                namespace_id: "team.prod".into(),
                code_revision: "revision".into(),
                image_ref: "test-image".into(),
                working_directory: "/workspace".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
                socket_gateway_url: None,
            },
            Some(&contract),
        )
        .await?;
    fixture
        .store
        .claim_actor(
            &fixture.actor("unsaved"),
            None,
            &fixture.host,
            "north-america-east",
        )
        .await?;
    let inventory: Value = fixture
        .get("/v1/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    let rows = inventory["actors"].as_array().unwrap();
    let unused = rows
        .iter()
        .find(|row| row["actorType"] == "ChatRoom")
        .unwrap();
    assert_eq!(unused["live"], 0);
    assert_eq!(unused["dormant"], 0);
    let unsaved = rows
        .iter()
        .find(|row| row["actorType"] == "Room.with.dots")
        .unwrap();
    assert_eq!(unsaved["unknown"], 1);
    assert_eq!(unsaved["dormant"], 0);
    let other: Value = fixture
        .get("/v1/observe/actors?namespace=other")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(other["actors"], json!([]));
    Ok(())
}

struct Fixture {
    admin: AdminService,
    runtime: RuntimeFixture,
    store: Arc<RuntimeStorage>,
    host: HostId,
    issuer: ActorJwtIssuer,
    sockets: SocketRegistry,
    origin: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn start() -> Result<Self> {
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
        )?
        .with_default_namespace("team.prod")?;
        let sockets = SocketRegistry::default();
        let inspector =
            ActorInspector::new(store.clone(), store.clone(), store.clone(), sockets.clone());
        let routes = super::inspection::router(inspector, admin.clone());
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host.v2.team.prod:test");
        runtime
            .leases
            .register(&HostLeaseRequest {
                id: host.clone(),
                session_id: "session".into(),
                route: "http://localhost:7101".into(),
                duration_ms: 60_000,
            })
            .await?;
        Ok(Self {
            admin,
            runtime,
            store,
            host,
            issuer,
            sockets,
            origin,
            client: reqwest::Client::new(),
            server,
        })
    }

    fn actor(&self, id: &str) -> ActorKey {
        ActorKey {
            namespace_id: "team.prod".into(),
            actor_type: "Room.with.dots".into(),
            actor_id: id.into(),
        }
    }

    async fn connect(&self, actor: &ActorKey) {
        let connection = ActorSocketConnection {
            id: format!("socket-{}", actor.actor_id),
            metadata: json!({"userId": "ada"}),
            tags: vec![],
        };
        let (outbound, _) = tokio::sync::mpsc::unbounded_channel();
        assert!(
            self.sockets
                .insert(actor, connection.clone(), outbound, None)
                .await
        );
        self.sockets.activate(actor, &connection.id).await;
    }

    async fn save(&self, actor: &ActorKey, version: u64, state: Value) -> Result<String> {
        self.store
            .claim_actor(actor, None, &self.host, "north-america-east")
            .await?;
        let ticket = self
            .store
            .prepare_write("north-america-east", actor, version)
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
