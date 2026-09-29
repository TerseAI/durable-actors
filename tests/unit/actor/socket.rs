use super::*;

#[test]
fn large_socket_state_and_payloads_remain_valid() -> Result<()> {
    let data = "x".repeat(33 * 1024 * 1024);
    validate_socket_effects(&[ActorSocketEffect::StateSnapshot {
        connection_id: "socket".into(),
        state: serde_json::json!({"data":data}),
        version: Some(1),
    }])?;
    validate_socket_message(&ActorSocketMessage::Text { data })?;
    Ok(())
}
