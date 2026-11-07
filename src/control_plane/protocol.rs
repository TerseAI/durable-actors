use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::grpc::proto::{ControlPlaneReply, ControlPlaneRequest};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommand {
    SocketOperation {
        actor: crate::actor::ActorKey,
        operation: super::socket_gateway::SocketOperation,
    },
    RefreshStorageAccess,
    Lifecycle {
        report: crate::actor_lifecycle::LifecycleReport,
    },
    GetAlarm,
    UpdateAlarm {
        timestamp_ms: Option<i64>,
    },
    InvokeActor {
        call: crate::actor::ActorCall,
    },
    LoadOwnership {
        key: String,
    },
    ReplaceOwnership {
        key: String,
        generation: Option<i64>,
        #[serde(with = "base64_bytes")]
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OwnershipRecord {
    pub generation: i64,
    #[serde(with = "base64_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommandReply {
    SocketOperation {
        reply: super::socket_gateway::SocketOperationReply,
    },
    StorageAccess {
        tokens: Option<crate::bucket::access::StorageTokens>,
        replacement_token: String,
    },
    Alarm {
        timestamp_ms: Option<i64>,
    },
    Invocation {
        outcome: serde_json::Value,
    },
    Ownership {
        record: Option<OwnershipRecord>,
    },
    OwnershipReplaced {
        replaced: bool,
    },
    Unit,
}

mod base64_bytes {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        STANDARD
            .decode(String::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
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
#[path = "../../tests/unit/control_plane/protocol.rs"]
mod tests;
