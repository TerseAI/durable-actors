use super::*;

#[test]
fn storage_access_refresh_roundtrips_through_the_control_plane_wire_format() -> Result<()> {
    let encoded = encode_command(ControlPlaneCommand::RefreshStorageAccess)?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded.command_json)?,
        serde_json::json!({"type":"refresh_storage_access"})
    );
    assert!(matches!(
        decode_command(encoded)?,
        ControlPlaneCommand::RefreshStorageAccess
    ));
    Ok(())
}

#[test]
fn alarm_update_roundtrips_through_the_control_plane_wire_format() -> Result<()> {
    let encoded = encode_command(ControlPlaneCommand::UpdateAlarm {
        state_version: 7,
        timestamp_ms: Some(1_000),
    })?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded.command_json)?,
        serde_json::json!({
            "type": "update_alarm",
            "state_version": 7,
            "timestamp_ms": 1_000
        })
    );
    assert!(matches!(
        decode_command(encoded)?,
        ControlPlaneCommand::UpdateAlarm {
            state_version: 7,
            timestamp_ms: Some(1_000)
        }
    ));
    Ok(())
}
