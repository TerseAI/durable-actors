use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::grpc::proto::{ControlPlaneReply, ControlPlaneRequest};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommand {
    RefreshStorageAccess,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommandReply {
    StorageAccess {
        token: Option<crate::bucket::access::StorageToken>,
        replacement_token: String,
    },
    Unit,
}

pub(crate) fn encode_command(command: ControlPlaneCommand) -> Result<ControlPlaneRequest> {
    Ok(ControlPlaneRequest {
        command_json: serde_json::to_vec(&command)?,
    })
}

pub(crate) fn decode_command(request: ControlPlaneRequest) -> Result<ControlPlaneCommand> {
    Ok(serde_json::from_slice(&request.command_json)?)
}

pub(crate) fn encode_reply(reply: ControlPlaneCommandReply) -> Result<ControlPlaneReply> {
    Ok(ControlPlaneReply {
        reply_json: serde_json::to_vec(&reply)?,
    })
}

pub(crate) fn decode_reply(reply: ControlPlaneReply) -> Result<ControlPlaneCommandReply> {
    Ok(serde_json::from_slice(&reply.reply_json)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn storage_credentials_are_the_only_runtime_control_plane_command() -> Result<()> {
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
}
