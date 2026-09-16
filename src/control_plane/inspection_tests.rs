use std::{sync::Arc, time::Duration};

use anyhow::Result;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    actor::ActorKey,
    clock::SystemClock,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
    placement::{ObjectPlacementStore, StateCommitRequest},
    sqlite::SqliteStore,
    state_log::StateSnapshot,
    state_transport::{HttpStateTransport, StateTransport},
    storage_urls::{LocalStorage, StorageUrlSigner},
};

use super::{ActorJwtIssuer, admin::AdminService, inspection::ActorInspector};

#[tokio::test]
async fn inspection_reads_only_the_committed_snapshot_without_a_deployment_or_live_host()
-> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("one");
    let committed = fixture
        .save(&actor, 1, json!({"internal": {"password": "saved"}}))
        .await?;
    fixture
        .store
        .commit_state(&StateCommitRequest {
            object: actor.storage_key(),
            owner: fixture.host.clone(),
            session_id: "session".into(),
            owner_epoch: 1,
            expected_version: 0,
            state_object: committed,
            request_id: "request-1".into(),
        })
        .await?;
    fixture
        .save(&actor, 2, json!({"internal": "uncommitted"}))
        .await?;
    let before = fixture.store.get(&actor.storage_key()).await?;
    fixture
        .store
        .claim(
            &actor.storage_key(),
            before.as_ref(),
            &HostId::new("replacement"),
            "north-america-east",
        )
        .await?;
    fixture.store.unregister(&fixture.host, "session").await?;
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
        .claim(
            &actor.storage_key(),
            None,
            &fixture.host,
            "north-america-east",
        )
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
async fn inspection_reports_inconsistent_or_missing_snapshots_instead_of_returning_state()
-> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("one");
    fixture.commit(&actor, json!({"internal": "saved"})).await?;
    let placement = fixture.store.get(&actor.storage_key()).await?.unwrap();
    let file = fixture
        ._directory
        .path()
        .join("snapshots")
        .join(placement.state_object.unwrap());
    let wrong = StateSnapshot::new(
        2,
        1,
        "request-1".into(),
        json!({"internal": "uncommitted"}),
        Value::Null,
    )?;
    tokio::fs::write(&file, wrong.encode()?).await?;
    let response = fixture.get("/v1/actors/Room.with.dots/one/state").await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!response.text().await?.contains("uncommitted"));
    tokio::fs::remove_file(file).await?;
    assert_eq!(
        fixture
            .get("/v1/actors/Room.with.dots/one/state")
            .await?
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    Ok(())
}

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<SqliteStore>,
    storage: Arc<LocalStorage>,
    transport: Arc<HttpStateTransport>,
    host: HostId,
    issuer: ActorJwtIssuer,
    origin: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn start() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(SqliteStore::open(&directory.path().join("runtime.sqlite")).await?);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let storage = Arc::new(LocalStorage::new(
            directory.path().join("snapshots"),
            origin.clone(),
            Arc::new(SystemClock),
        )?);
        let transport = Arc::new(HttpStateTransport::new());
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let admin = AdminService::new("api-key".into(), store.clone(), issuer.clone())?
            .with_default_namespace("team.prod")?;
        let inspector = ActorInspector::new(store.clone(), storage.clone(), transport.clone());
        let routes = super::inspection::router(inspector, admin).merge(storage.clone().router());
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host");
        store
            .register(&HostLeaseRequest {
                id: host.clone(),
                session_id: "session".into(),
                route: "http://localhost:7101".into(),
                duration_ms: 60_000,
            })
            .await?;
        Ok(Self {
            _directory: directory,
            store,
            storage,
            transport,
            host,
            issuer,
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

    async fn save(&self, actor: &ActorKey, version: u64, state: Value) -> Result<String> {
        self.store
            .claim(&actor.storage_key(), None, &self.host, "north-america-east")
            .await?;
        let ticket = self
            .storage
            .write_ticket("north-america-east", actor, version)
            .await?;
        let snapshot =
            StateSnapshot::new(version, 1, format!("request-{version}"), state, Value::Null)?;
        self.transport
            .write(&ticket.url, snapshot.encode()?)
            .await?;
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
        let object = self.save(actor, 1, state).await?;
        self.store
            .commit_state(&StateCommitRequest {
                object: actor.storage_key(),
                owner: self.host.clone(),
                session_id: "session".into(),
                owner_epoch: 1,
                expected_version: 0,
                state_object: object,
                request_id: "request-1".into(),
            })
            .await?;
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
