use super::*;
use serde_json::json;

#[test]
fn forwards_sqlite_with_the_object_state_in_one_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database
        .execute_batch("CREATE TABLE entries (value TEXT); INSERT INTO entries VALUES ('saved')")?;
    use base64::Engine;
    let image = base64::engine::general_purpose::STANDARD.encode(std::fs::read(path)?);
    let document = serde_json::to_vec(&json!({
        "stateVersion": 2, "ownerEpoch": 1, "requestId": "both",
        "state": {"count": 3}, "sqlite": image, "result": null
    }))?;
    let snapshot = StateSnapshot::decode(&document)?;
    let forwarded: Value = serde_json::from_slice(&snapshot.encode()?)?;
    assert_eq!(forwarded["sqlite"], image);
    assert_eq!(forwarded["state"], json!({"count": 3}));
    Ok(())
}

#[test]
fn round_trips_large_object_and_sqlite_state() -> Result<()> {
    let state = json!({"value": "中".repeat(6 * 1024 * 1024)});
    let mut snapshot = StateSnapshot::new(1, 1, "large".into(), &state, Value::Null)?;
    snapshot.sqlite = Some("A".repeat(34 * 1024 * 1024));
    let restored = StateSnapshot::decode(&snapshot.encode()?)?;
    assert_eq!(serde_json::from_str::<Value>(restored.state.get())?, state);
    assert_eq!(restored.sqlite, snapshot.sqlite);
    Ok(())
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
