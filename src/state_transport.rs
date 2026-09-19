use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateWrite {
    Written,
    AlreadyExists,
    Replicated,
}

#[async_trait]
pub trait StateTransport: Send + Sync {
    async fn read(&self, signed_url: &str) -> Result<Bytes>;
    async fn write(&self, signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite>;
}

#[async_trait]
pub trait SnapshotWriter: Send + Sync {
    async fn write_snapshot(
        &self,
        plan: &crate::storage::WritePlan,
        bytes: Vec<u8>,
    ) -> Result<StateWrite>;
}

#[derive(Clone, Default)]
pub struct GrpcStateTransport;

impl GrpcStateTransport {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl StateTransport for GrpcStateTransport {
    async fn read(&self, signed_url: &str) -> Result<Bytes> {
        let (channel, token) = crate::grpc::transport::capability(signed_url)?;
        let response = storage_client(channel)
            .read(crate::grpc::transport::request(
                crate::grpc::proto::Empty {},
                &token,
            )?)
            .await?;
        Ok(Bytes::from(response.into_inner().data))
    }

    async fn write(&self, signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
        let (channel, token) = crate::grpc::transport::capability(signed_url)?;
        let response = storage_client(channel)
            .write(crate::grpc::transport::request(
                crate::grpc::proto::SnapshotData { data: bytes },
                &token,
            )?)
            .await?;
        Ok(if response.into_inner().already_exists {
            StateWrite::AlreadyExists
        } else {
            StateWrite::Written
        })
    }
}

fn storage_client(
    channel: tonic::transport::Channel,
) -> crate::grpc::proto::snapshot_service_client::SnapshotServiceClient<tonic::transport::Channel> {
    crate::grpc::proto::snapshot_service_client::SnapshotServiceClient::new(channel)
        .max_decoding_message_size(crate::grpc::transport::MAX_STORAGE_MESSAGE_BYTES)
        .max_encoding_message_size(crate::grpc::transport::MAX_STORAGE_MESSAGE_BYTES)
}

#[cfg(test)]
#[path = "../tests/unit/state_transport.rs"]
mod tests;
