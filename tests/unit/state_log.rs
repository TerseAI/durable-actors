use super::*;
use serde_json::json;

fn sqlite_snapshot(payload: usize) -> Result<SqliteSnapshot> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE entries (value BLOB)",
    )?;
    database.execute("INSERT INTO entries VALUES (zeroblob(?))", [payload as i64])?;
    let mut capture = crate::ltx::SqliteCapture::new()?;
    let ltx = capture.capture(&crate::ltx::SqliteState {
        txid: 2,
        path: None,
        wal: Some(crate::ltx::SqliteWal {
            base_txid: 0,
            data: STANDARD.encode(std::fs::read(path.with_extension("sqlite-wal"))?),
        }),
    })?;
    Ok(SqliteSnapshot {
        object: "state/1".into(),
        txid: 2,
        parent: None,
        ltx: Some(STANDARD.encode(ltx)),
    })
}

#[test]
fn forwards_sqlite_with_the_object_state_in_one_snapshot() -> Result<()> {
    let mut snapshot = StateSnapshot::new(1, 1, "both".into(), json!({"count": 3}), Value::Null)?;
    snapshot.sqlite = Some(sqlite_snapshot(10)?);
    let restored = StateSnapshot::decode(&snapshot.encode()?)?;
    assert_eq!(
        serde_json::to_value(&restored.sqlite)?,
        serde_json::to_value(&snapshot.sqlite)?
    );
    assert_eq!(restored.state.get(), r#"{"count":3}"#);
    Ok(())
}

#[test]
fn round_trips_large_object_and_sqlite_state() -> Result<()> {
    let state = json!({"value": "中".repeat(6 * 1024 * 1024)});
    let mut snapshot = StateSnapshot::new(1, 1, "large".into(), &state, Value::Null)?;
    snapshot.sqlite = Some(sqlite_snapshot(25 * 1024 * 1024)?);
    let restored = StateSnapshot::decode(&snapshot.encode()?)?;
    assert_eq!(serde_json::from_str::<Value>(restored.state.get())?, state);
    assert_eq!(
        serde_json::to_value(&restored.sqlite)?,
        serde_json::to_value(&snapshot.sqlite)?
    );
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
