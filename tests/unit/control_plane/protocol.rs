use super::*;

#[test]
fn storage_access_refresh_roundtrips_through_the_control_plane_wire_format() -> Result<()> {
    let encoded = encode_command(ControlPlaneCommand::RefreshStorageAccess {
        read_only_ownership: true,
    })?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded.command_json)?,
        serde_json::json!({"type":"refresh_storage_access","read_only_ownership":true})
    );
    assert!(matches!(
        decode_command(encoded)?,
        ControlPlaneCommand::RefreshStorageAccess {
            read_only_ownership: true
        }
    ));
    Ok(())
}

#[test]
fn hosts_that_renew_in_the_bucket_refresh_without_the_read_only_flag() -> Result<()> {
    let legacy = ControlPlaneRequest {
        command_json: br#"{"type":"refresh_storage_access"}"#.to_vec(),
    };
    assert!(matches!(
        decode_command(legacy)?,
        ControlPlaneCommand::RefreshStorageAccess {
            read_only_ownership: false
        }
    ));
    Ok(())
}
