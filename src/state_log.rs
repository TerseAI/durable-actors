use crate::storage::SnapshotRef;
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{
    Value,
    value::{RawValue, to_raw_value},
};
use std::borrow::Borrow;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution: Option<StateAttribution>,
    pub state_version: u64,
    pub owner_epoch: u64,
    pub request_id: String,
    pub state: Box<RawValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite: Option<SqliteSnapshot>,
    pub result: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqliteSnapshot {
    pub object: String,
    pub txid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<SnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ltx: Option<String>,
}

impl SqliteSnapshot {
    pub(crate) fn segment(&self) -> Result<Option<Vec<u8>>> {
        self.ltx
            .as_ref()
            .map(|data| STANDARD.decode(data).context("decode SQLite LTX segment"))
            .transpose()
    }

    fn validate(&self, state_version: u64) -> Result<()> {
        ensure!(self.txid > 0, "SQLite transaction must be positive");
        if let Some(parent) = &self.parent {
            ensure!(
                parent.state_version < state_version,
                "SQLite dependency must precede its commit"
            );
        }
        match self.segment()? {
            Some(bytes) => {
                let (_, header) = litetx::Decoder::new(bytes.as_slice())?;
                ensure!(
                    header.max_txid.into_inner() == self.txid,
                    "SQLite LTX transaction mismatch"
                );
                ensure!(
                    header.pre_apply_checksum.is_some() == self.parent.is_some(),
                    "SQLite LTX dependency mismatch"
                );
            }
            None => ensure!(self.parent.is_some(), "SQLite state has no recovery source"),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateAttribution {
    pub operation: String,
    pub connection_id: Option<String>,
    pub committed_at_ms: u64,
    pub interleaved: bool,
}

impl StateSnapshot {
    pub fn new(
        state_version: u64,
        owner_epoch: u64,
        request_id: String,
        state: impl Borrow<Value>,
        result: Value,
    ) -> Result<Self> {
        let snapshot = Self {
            attribution: None,
            state_version,
            owner_epoch,
            request_id,
            state: to_raw_value(state.borrow())?,
            sqlite: None,
            result,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self =
            serde_json::from_slice(bytes).context("decode actor state snapshot")?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub(crate) fn validate_object(&self, object: &str) -> Result<()> {
        if let Some(sqlite) = &self.sqlite {
            if sqlite.ltx.is_some() {
                ensure!(
                    sqlite.object == object,
                    "SQLite segment belongs to another snapshot"
                );
            }
            if let Some(parent) = &sqlite.parent {
                ensure!(
                    crate::storage_paths::actor_from_snapshot(object)?
                        == crate::storage_paths::actor_from_snapshot(&parent.object)?,
                    "SQLite dependency belongs to another actor"
                );
                if sqlite.ltx.is_none() {
                    ensure!(sqlite.object == parent.object, "SQLite head mismatch");
                }
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.state_version > 0,
            "actor state version must be positive"
        );
        ensure!(self.owner_epoch > 0, "owner epoch must be positive");
        ensure!(
            !self.request_id.is_empty() && self.request_id.len() <= 255,
            "actor state request ID is invalid"
        );
        ensure!(
            self.state.get().starts_with('{'),
            "actor state must be a JSON object"
        );
        if let Some(sqlite) = &self.sqlite {
            sqlite.validate(self.state_version)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/state_log.rs"]
mod tests;
