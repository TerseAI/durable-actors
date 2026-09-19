use super::*;
#[test]
fn runtime_commands_do_not_include_storage_operations() -> Result<()> {
    let encoded = encode_command(ControlPlaneCommand::RefreshStorageAccess)?;
    assert!(matches!(
        decode_command(encoded)?,
        ControlPlaneCommand::RefreshStorageAccess
    ));
    for command in [
        "register_lease",
        "prepare_state_write",
        "commit_state",
        "load_actor_state",
    ] {
        assert!(
            serde_json::from_value::<ControlPlaneCommand>(serde_json::json!({"type":command}))
                .is_err()
        );
    }
    Ok(())
}
