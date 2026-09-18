use super::{ReplicaStream, SessionHead, SnapshotRef, StreamHead};
use crate::grpc::proto;
use anyhow::{Context, Result};

impl From<StreamHead> for proto::ReplicaStreamHead {
    fn from(head: StreamHead) -> Self {
        Self {
            stream: Some(head.stream.into()),
            latest: head.latest.map(Into::into),
        }
    }
}
impl TryFrom<proto::ReplicaStreamHead> for StreamHead {
    type Error = anyhow::Error;
    fn try_from(head: proto::ReplicaStreamHead) -> Result<Self> {
        Ok(Self {
            stream: head.stream.context("replica stream is required")?.into(),
            latest: head.latest.map(Into::into),
        })
    }
}
impl From<SessionHead> for proto::ReplicaSessionHead {
    fn from(head: SessionHead) -> Self {
        Self {
            session: head.session,
            initialized: head.initialized,
            sealed: head.sealed,
            streams: head.streams.into_iter().map(Into::into).collect(),
        }
    }
}
impl TryFrom<proto::ReplicaSessionHead> for SessionHead {
    type Error = anyhow::Error;
    fn try_from(head: proto::ReplicaSessionHead) -> Result<Self> {
        Ok(Self {
            session: head.session,
            initialized: head.initialized,
            sealed: head.sealed,
            streams: head
                .streams
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?,
        })
    }
}
impl From<ReplicaStream> for proto::ReplicaStreamIdentity {
    fn from(stream: ReplicaStream) -> Self {
        Self {
            session: stream.session,
            prefix: stream.prefix,
            owner_epoch: stream.owner_epoch,
            base_version: stream.base_version,
        }
    }
}
impl From<proto::ReplicaStreamIdentity> for ReplicaStream {
    fn from(stream: proto::ReplicaStreamIdentity) -> Self {
        Self {
            session: stream.session,
            prefix: stream.prefix,
            owner_epoch: stream.owner_epoch,
            base_version: stream.base_version,
        }
    }
}
impl From<SnapshotRef> for proto::ReplicaSnapshotRef {
    fn from(snapshot: SnapshotRef) -> Self {
        Self {
            object: snapshot.object,
            state_version: snapshot.state_version,
            request_id: snapshot.request_id,
            digest: snapshot.digest,
        }
    }
}
impl From<proto::ReplicaSnapshotRef> for SnapshotRef {
    fn from(snapshot: proto::ReplicaSnapshotRef) -> Self {
        Self {
            object: snapshot.object,
            state_version: snapshot.state_version,
            request_id: snapshot.request_id,
            digest: snapshot.digest,
        }
    }
}
