use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};

use super::PublicActorContract;
use crate::{
    control_plane::admin::{
        AdminRegistry, HostLaunchSpec, LocalAdminRegistry, PostgresAdminRegistry,
    },
    postgres::{PostgresDatabase, testing::with_postgres},
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

#[test]
fn actor_types_must_be_typescript_identifiers() -> Result<()> {
    let valid: Value =
        serde_json::from_str(include_str!("../../sdk/fixtures/public-contract.json"))?;
    for name in [
        "chat-room",
        "chat.room",
        "1ChatRoom",
        "123",
        "-",
        ".",
        "class",
        "interface",
        "constructor",
        "type",
        "async",
        "await",
        "using",
        "satisfies",
        "undefined",
        "defer",
    ] {
        let mut document = valid.clone();
        document["actors"][0]["actorType"] = json!(name);
        document["actors"][0]["socket"]["actorType"] = json!(name);
        let error = PublicActorContract::new(document).unwrap_err();
        assert!(
            error.to_string().contains("TypeScript identifier"),
            "{name}: {error}"
        );
    }
    for name in [
        "ChatRoom",
        "_",
        "_chat2",
        "chat_room",
        "Class",
        "class1",
        "asyncActor",
    ] {
        let mut document = valid.clone();
        document["actors"][0]["actorType"] = json!(name);
        document["actors"][0]["socket"]["actorType"] = json!(name);
        PublicActorContract::new(document)?;
    }
    Ok(())
}

#[test]
fn rest_parameters_require_array_schemas_without_tuple_items() -> Result<()> {
    let valid: Value =
        serde_json::from_str(include_str!("../../sdk/fixtures/public-contract.json"))?;
    for (schema, accepted) in [
        (json!(true), false),
        (json!(false), false),
        (json!({"type": "string"}), false),
        (json!({"items": {"type": "string"}}), false),
        (json!({"type": ["array", "null"]}), false),
        (json!({"type": "array", "items": []}), false),
        (
            json!({"type": "array", "items": [{"type": "string"}]}),
            false,
        ),
        (json!({"type": "array", "items": {"type": "string"}}), true),
        (json!({"type": "array", "items": true}), true),
        (json!({"type": "array"}), true),
    ] {
        let mut document = valid.clone();
        document["actors"][0]["rpc"]["schema"]["definitions"]["Method_sendMessage_Parameter_0"] =
            schema.clone();
        PublicActorContract::new(document.clone())?;
        document["actors"][0]["rpc"]["methods"][1]["parameters"][0]["rest"] = json!(true);
        let result = PublicActorContract::new(document);
        assert_eq!(result.is_ok(), accepted, "{schema}: {result:?}");
    }
    Ok(())
}

#[tokio::test]
async fn in_memory_contract_keeps_only_the_active_revision() -> Result<()> {
    registry_behavior(Arc::new(LocalAdminRegistry::default())).await
}

#[tokio::test]
async fn postgres_latest_contract_is_atomic_and_survives_reconnection() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        registry_behavior(Arc::new(PostgresAdminRegistry::from_database(database))).await?;
        let registry =
            PostgresAdminRegistry::from_database(PostgresDatabase::connect(&fixture.url).await?);
        let contract = PublicActorContract::new(json!({"version":1,"actors":[]}))?;
        let deployment = spec(&format!("persisted-{}", uuid::Uuid::new_v4()));
        registry
            .register_deployment(&deployment, Some(&contract))
            .await?;
        drop(registry);
        let reopened =
            PostgresAdminRegistry::from_database(PostgresDatabase::connect(&fixture.url).await?);
        assert_eq!(
            reopened
                .deployment_contract(&deployment.namespace_id, None)
                .await?
                .unwrap()
                .contract,
            *contract.document()
        );
        Ok(())
    })
    .await
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
        None
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
    assert_eq!(registry.deployment_contract(&namespace, None).await?, None);
    registry
        .register_deployment(&deployment, Some(&full))
        .await?;
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
        None
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
