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
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
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
    let response = fixture.get("/v1/actors/Room.with.dots/one").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let metadata: Value = response.json().await?;
    assert_eq!(metadata["homeRegion"], "north-america-east");
    assert!(metadata.get("state").is_none());
    let state: Value = fixture
        .get("/v1/actors/Room.with.dots/one?include=state")
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
        "/v1/actors/Room.with.dots/one/state",
        "/v1/actors/Room.with.dots/one/placement",
    ] {
        assert_eq!(fixture.get(path).await?.status(), StatusCode::NOT_FOUND);
    }
    assert_eq!(
        fixture
            .get("/v1/actors/Room.with.dots/one?include=unknown")
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
        .runtime
        .leases
        .unregister(&fixture.host, "session")
        .await?;
    let before = fixture.store.get(&actor.storage_key()).await?;

    let response = fixture.get("/v1/actors?limit=1").await?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    let page: Value = response.error_for_status()?.json().await?;
    assert_eq!(page["actors"][0]["actorType"], "Room.with.dots");
    assert_eq!(page["actors"][0]["actorId"], "one");
    assert_eq!(page["actors"][0]["stateVersion"], 1);
    assert_eq!(page["nextCursor"], Value::Null);

    let response = fixture
        .get("/v1/actors/Room.with.dots/one?include=state")
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
        )?
        .token;
    for path in ["/v1/actors", "/v1/actors/Room.with.dots/one?include=state"] {
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
            .get("/v1/actors/Room.with.dots/missing?include=state")
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
        .get("/v1/actors/Room.with.dots/empty?include=state")
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
        .get("/v1/actors/Room.with.dots/one?include=state")
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!response.text().await?.contains("uncommitted"));
    Ok(())
}

struct Fixture {
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
        let inspector = ActorInspector::new(store.clone(), store.clone());
        let routes = super::inspection::router(inspector, admin);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host.v3.test");
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
            actor_type: "Room.with.dots".into(),
            actor_id: id.into(),
        }
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
