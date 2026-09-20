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
