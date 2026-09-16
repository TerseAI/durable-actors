use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};

use super::PublicActorContract;
use crate::{
    control_plane::admin::{
        AdminRegistry, HostLaunchSpec, LocalAdminRegistry, PostgresAdminRegistry,
    },
    postgres::PostgresDatabase,
    sqlite::SqliteStore,
};

#[test]
fn compiler_contract_is_preserved_and_hashes_ignore_json_object_key_order() -> Result<()> {
    let value: Value =
        serde_json::from_str(include_str!("../../sdk/fixtures/public-contract.json"))?;
    let contract = PublicActorContract::new(value.clone())?;
    assert_eq!(contract.document(), &value);
    let first = PublicActorContract::new(serde_json::from_str(r#"{"version":1,"actors":[]}"#)?)?;
    let second = PublicActorContract::new(serde_json::from_str(r#"{"actors":[],"version":1}"#)?)?;
    assert_eq!(first.hash(), second.hash());
    assert_ne!(contract.hash(), first.hash());
    Ok(())
}

#[test]
fn malformed_or_nonportable_contracts_are_rejected() -> Result<()> {
    let valid: Value =
        serde_json::from_str(include_str!("../../sdk/fixtures/public-contract.json"))?;
    for invalid in [
        json!({}),
        json!({"version":2,"actors":[]}),
        json!({"version":1,"actors":{}}),
    ] {
        assert!(PublicActorContract::new(invalid).is_err());
    }
    for (pointer, replacement) in [
        ("/actors/0/actorType", json!("different")),
        ("/actors/0/rpc/methods/0/name", json!("then")),
        ("/actors/0/rpc/methods/0/result/kind", json!("anything")),
        (
            "/actors/0/rpc/methods/1/parameters/0/type/$ref",
            json!("https://example.com/types.json"),
        ),
        (
            "/actors/0/rpc/methods/1/result/type/$ref",
            json!("#/definitions/Missing"),
        ),
        (
            "/actors/0/rpc/methods/1/parameters/0/optional",
            json!("yes"),
        ),
    ] {
        let mut malformed = valid.clone();
        *malformed.pointer_mut(pointer).expect(pointer) = replacement;
        assert!(PublicActorContract::new(malformed).is_err(), "{pointer}");
    }
    let mut duplicate = valid.clone();
    duplicate["actors"]
        .as_array_mut()
        .unwrap()
        .push(valid["actors"][0].clone());
    assert!(PublicActorContract::new(duplicate).is_err());
    let mut external = valid;
    external["actors"][0]["rpc"]["schema"]["definitions"]["External"] =
        json!({"$ref":"file:///private/example"});
    assert!(PublicActorContract::new(external).is_err());
    Ok(())
}

#[tokio::test]
async fn in_memory_contract_history_follows_deployment_revisions() -> Result<()> {
    registry_behavior(Arc::new(LocalAdminRegistry::default())).await
}

#[tokio::test]
async fn sqlite_contract_history_is_atomic_and_survives_reopening() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("runtime.sqlite");
    let store = Arc::new(SqliteStore::open(&path).await?);
    registry_behavior(store.clone()).await?;
    let contract = PublicActorContract::new(json!({"version":1,"actors":[]}))?;
    let deployment = spec("persisted");
    store
        .register_deployment(&deployment, Some(&contract))
        .await?;
    drop(store);
    let reopened = SqliteStore::open(&path).await?;
    assert_eq!(
        reopened
            .deployment_contract("persisted", None)
            .await?
            .unwrap()
            .contract,
        *contract.document()
    );
    let second = SqliteStore::open(&path).await?;
    let race = spec("independent-connections");
    let different = PublicActorContract::new(serde_json::from_str(include_str!(
        "../../sdk/fixtures/public-contract.json"
    ))?)?;
    let (left, right) = tokio::join!(
        reopened.register_deployment(&race, Some(&contract)),
        second.register_deployment(&race, Some(&different)),
    );
    assert_ne!(left.is_ok(), right.is_ok());
    Ok(())
}

#[tokio::test]
async fn postgres_contract_history_is_atomic_and_survives_reconnection() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_OBJECT_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let database = PostgresDatabase::connect(&url).await?;
    registry_behavior(Arc::new(PostgresAdminRegistry::from_database(database))).await?;
    let registry = PostgresAdminRegistry::from_database(PostgresDatabase::connect(&url).await?);
    let contract = PublicActorContract::new(json!({"version":1,"actors":[]}))?;
    let deployment = spec(&format!("persisted-{}", uuid::Uuid::new_v4()));
    registry
        .register_deployment(&deployment, Some(&contract))
        .await?;
    drop(registry);
    let reopened = PostgresAdminRegistry::from_database(PostgresDatabase::connect(&url).await?);
    assert_eq!(
        reopened
            .deployment_contract(&deployment.namespace_id, None)
            .await?
            .unwrap()
            .contract,
        *contract.document()
    );
    Ok(())
}

async fn registry_behavior(registry: Arc<dyn AdminRegistry>) -> Result<()> {
    let namespace = format!("contract-{}", uuid::Uuid::new_v4());
    let mut deployment = spec(&namespace);
    let empty = PublicActorContract::new(json!({"version":1,"actors":[]}))?;
    let full = PublicActorContract::new(serde_json::from_str(include_str!(
        "../../sdk/fixtures/public-contract.json"
    ))?)?;
    assert!(
        registry
            .deployment_contract(&namespace, None)
            .await?
            .is_none()
    );
    assert!(
        registry
            .register_deployment(&deployment, Some(&empty))
            .await?
    );
    assert!(
        !registry
            .register_deployment(&deployment, Some(&empty))
            .await?
    );
    let first = registry
        .deployment_contract(&namespace, None)
        .await?
        .unwrap();
    assert_eq!(first.code_revision, "revision-1");
    assert_eq!(first.contract_hash, empty.hash());
    assert_eq!(first.contract, *empty.document());
    let mut conflicting = deployment.clone();
    conflicting.image_ref = "rejected-image".into();
    assert!(
        registry
            .register_deployment(&conflicting, Some(&full))
            .await
            .is_err()
    );
    assert_eq!(
        registry.launch_spec(&namespace).await?,
        Some(deployment.clone())
    );
    assert!(!registry.register_deployment(&deployment, None).await?);
    assert_eq!(
        registry.deployment_contract(&namespace, None).await?,
        Some(first.clone())
    );
    deployment.code_revision = "revision-2".into();
    assert!(
        registry
            .register_deployment(&deployment, Some(&full))
            .await?
    );
    assert_eq!(
        registry
            .deployment_contract(&namespace, None)
            .await?
            .unwrap()
            .contract,
        *full.document()
    );
    assert_eq!(
        registry
            .deployment_contract(&namespace, Some("revision-1"))
            .await?,
        Some(first.clone())
    );
    assert!(
        registry
            .deployment_contract("another-namespace", Some("revision-1"))
            .await?
            .is_none()
    );
    deployment.code_revision = "revision-3".into();
    registry.register_deployment(&deployment, None).await?;
    assert!(
        registry
            .deployment_contract(&namespace, None)
            .await?
            .is_none()
    );
    deployment.code_revision = "revision-1".into();
    registry.register_deployment(&deployment, None).await?;
    assert_eq!(
        registry.deployment_contract(&namespace, None).await?,
        Some(first.clone())
    );
    registry.remove_deployment(&namespace).await?;
    assert!(
        registry
            .deployment_contract(&namespace, None)
            .await?
            .is_none()
    );
    assert_eq!(
        registry
            .deployment_contract(&namespace, Some("revision-1"))
            .await?,
        Some(first)
    );
    deployment.code_revision = "race".into();
    let (left, right) = tokio::join!(
        registry.register_deployment(&deployment, Some(&empty)),
        registry.register_deployment(&deployment, Some(&full))
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let winner = if left.is_ok() { empty } else { full };
    assert_eq!(
        registry
            .deployment_contract(&namespace, None)
            .await?
            .unwrap()
            .contract,
        *winner.document()
    );
    Ok(())
}

fn spec(namespace: &str) -> HostLaunchSpec {
    HostLaunchSpec {
        namespace_id: namespace.into(),
        code_revision: "revision-1".into(),
        image_ref: "image".into(),
        working_directory: "/app".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
        socket_gateway_url: None,
    }
}
