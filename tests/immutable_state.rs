use anyhow::Result;
use durable_actors::state_log::StateSnapshot;
use serde_json::json;

#[test]
fn immutable_snapshot_round_trips_state_and_result() -> Result<()> {
    let snapshot = StateSnapshot::new(7, 3, "request-7".into(), json!({ "count": 7 }), json!(7))?;

    let decoded = StateSnapshot::decode(&snapshot.encode()?)?;

    assert_eq!(decoded, snapshot);
    Ok(())
}
