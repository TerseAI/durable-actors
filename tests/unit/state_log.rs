use super::*;
use serde_json::json;

#[test]
fn accepts_state_larger_than_the_previous_one_mib_limit() {
    StateSnapshot::new(
        1,
        1,
        "request-1".into(),
        json!({"value": "x".repeat(2 * 1024 * 1024)}),
        Value::Null,
    )
    .expect("state within the supported limit");
}

#[test]
fn rejects_oversized_state() {
    let error = StateSnapshot::new(
        1,
        1,
        "request-1".into(),
        json!({"value": "x".repeat(MAX_ACTOR_STATE_BYTES)}),
        Value::Null,
    )
    .expect_err("oversized state");
    assert!(error.to_string().contains("actor state exceeds"));
}

#[test]
fn preserves_encoded_state_when_forwarding_a_snapshot() -> Result<()> {
    let bytes = br#"{"stateVersion":1,"ownerEpoch":2,"requestId":"request-1","state":{ "value": "\u0061", "nested": [1, true, null] },"result":null}"#;
    let snapshot = StateSnapshot::decode(bytes)?;
    assert_eq!(snapshot.encode()?, bytes);
    Ok(())
}

#[test]
fn rejects_invalid_snapshots_at_decode() {
    let valid = json!({
        "stateVersion": 1, "ownerEpoch": 1, "requestId": "request-1",
        "state": {}, "result": null,
    });
    for (field, value) in [
        ("stateVersion", json!(0)),
        ("ownerEpoch", json!(0)),
        ("requestId", json!("")),
        ("requestId", json!("x".repeat(256))),
        ("state", json!([])),
        ("state", Value::Null),
        ("state", json!("{}")),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    assert!(StateSnapshot::decode(br#"{"stateVersion":1,"ownerEpoch":1,"requestId":"r","state":{"value":},"result":null}"#).is_err());
}

#[test]
fn enforces_the_encoded_state_limit_at_creation_and_decode() -> Result<()> {
    let overhead = r#"{"value":""}"#.len();
    let at_limit = json!({"value": "x".repeat(MAX_ACTOR_STATE_BYTES - overhead)});
    let snapshot = StateSnapshot::new(1, 1, "r".into(), at_limit, Value::Null)?;
    StateSnapshot::decode(&snapshot.encode()?)?;

    let oversized = json!({
        "stateVersion": 1, "ownerEpoch": 1, "requestId": "r",
        "state": {"value": "x".repeat(MAX_ACTOR_STATE_BYTES - overhead + 1)},
        "result": null,
    });
    let error = StateSnapshot::decode(&serde_json::to_vec(&oversized)?).unwrap_err();
    assert!(error.to_string().contains("actor state exceeds"));
    Ok(())
}

#[test]
fn counts_escaped_bytes_toward_the_state_limit() {
    let state = json!({"value": "\u{0000}".repeat(MAX_ACTOR_STATE_BYTES / 6)});
    let error = StateSnapshot::new(1, 1, "r".into(), state, Value::Null).unwrap_err();
    assert!(error.to_string().contains("actor state exceeds"));
}
