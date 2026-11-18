use anyhow::{Result, ensure};
use aws_lc_rs::digest::{SHA256, digest};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::state_log::StateSnapshot;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStream {
    pub session: String,
    pub prefix: String,
    pub owner_epoch: u64,
    pub base_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub object: String,
    pub state_version: u64,
    pub request_id: String,
    pub digest: String,
}

#[derive(Clone, Debug)]
pub struct VerifiedSnapshot {
    pub reference: SnapshotRef,
    pub snapshot: StateSnapshot,
    pub bytes: Bytes,
}

impl VerifiedSnapshot {
    pub fn matching(reference: SnapshotRef, bytes: Bytes) -> Result<Self> {
        reference.verify(&bytes)?;
        let snapshot = StateSnapshot::decode(&bytes)?;
        snapshot.validate_object(&reference.object)?;
        ensure!(
            snapshot.state_version == reference.state_version
                && snapshot.request_id == reference.request_id,
            "snapshot identity does not match its reference"
        );
        Ok(Self {
            reference,
            snapshot,
            bytes,
        })
    }
    pub fn named(object: String, bytes: Bytes) -> Result<Self> {
        let snapshot = StateSnapshot::decode(&bytes)?;
        snapshot.validate_object(&object)?;
        ensure!(
            snapshot_position(&object) == Some((snapshot.owner_epoch, snapshot.state_version)),
            "snapshot identity does not match its object"
        );
        let reference = SnapshotRef::new(object, &snapshot, &bytes);
        Ok(Self {
            reference,
            snapshot,
            bytes,
        })
    }
}

impl StateStream {
    pub fn object(&self, version: u64) -> String {
        format!("{}{version}.json", self.prefix)
    }

    pub fn snapshot(&self, bytes: &Bytes) -> Result<SnapshotRef> {
        let snapshot = StateSnapshot::decode(bytes)?;
        snapshot.validate_object(&self.object(snapshot.state_version))?;
        ensure!(
            snapshot.owner_epoch == self.owner_epoch && snapshot.state_version > self.base_version,
            "snapshot belongs to another stream"
        );
        Ok(SnapshotRef::new(
            self.object(snapshot.state_version),
            &snapshot,
            bytes,
        ))
    }
}

impl SnapshotRef {
    pub fn new(object: String, snapshot: &StateSnapshot, bytes: &[u8]) -> Self {
        Self {
            object,
            state_version: snapshot.state_version,
            request_id: snapshot.request_id.clone(),
            digest: checksum(bytes),
        }
    }

    pub(crate) fn digest_of(bytes: &[u8]) -> String {
        checksum(bytes)
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        ensure!(checksum(bytes) == self.digest, "snapshot checksum mismatch");
        Ok(())
    }
}

fn checksum(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, bytes).as_ref())
}

pub(crate) fn snapshot_position(object: &str) -> Option<(u64, u64)> {
    let mut parts = object.rsplit('/');
    let version = parts.next()?.strip_suffix(".json")?.parse().ok()?;
    let epoch = u64::from_str_radix(parts.next()?, 16).ok()?;
    Some((epoch, version))
}
