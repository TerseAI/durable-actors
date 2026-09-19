use super::*;

#[test]
fn actor_identity_requires_only_type_and_id() {
    let key: ActorKey = serde_json::from_value(serde_json::json!({
        "actor_type": "Counter", "actor_id": "one"
    }))
    .expect("single deployment actor identity");
    key.validate().expect("valid identity");
    assert_eq!(key.storage_key().as_str(), "object.v3.Counter:one");
}

#[test]
fn maps_an_actor_to_one_safe_object_id() {
    let key = ActorKey {
        actor_type: "counter".into(),
        actor_id: "customer.123".into(),
    };

    key.validate().expect("actor key");
    assert_eq!(key.storage_key().as_str(), "object.v3.counter:customer.123");
}

#[test]
fn rejects_components_that_can_reshape_storage_paths() {
    let mut key = ActorKey {
        actor_type: "counter".into(),
        actor_id: "../other".into(),
    };
    assert!(key.validate().is_err());

    key.actor_id = "valid".into();
    key.actor_type = "counter/type".into();
    assert!(key.validate().is_err());
}
