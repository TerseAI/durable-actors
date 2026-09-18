use crate::{
    clock::{Clock, SystemClock},
    grpc::{
        proto::{Empty, replica_service_client::ReplicaServiceClient},
        transport::{MAX_STORAGE_MESSAGE_BYTES, capability, request},
    },
    replication::{
        ReplicaAccess, ReplicaGrant, ReplicaStream, ReplicaTarget, SessionHead, StreamHead,
    },
    state_transport::{GrpcStateTransport, StateTransport},
};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait ReplicaPeers: Send + Sync {
    async fn initialize(&self, peer: &ReplicaTarget, session: &str) -> Result<()>;
    async fn head(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead>;
    async fn seal(&self, peer: &ReplicaTarget, session: &str) -> Result<SessionHead>;
    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>>;
}

pub struct GrpcReplicaPeers {
    access: ReplicaAccess,
}

impl GrpcReplicaPeers {
    pub fn new(access: ReplicaAccess) -> Result<Self> {
        Ok(Self { access })
    }

    fn client(
        &self,
        peer: &ReplicaTarget,
        grant: ReplicaGrant,
    ) -> Result<(
        ReplicaServiceClient<tonic::transport::Channel>,
        tonic::Request<Empty>,
    )> {
        let address = self.access.url(
            &peer.url,
            &ReplicaGrant {
                host_id: peer.host_id.clone(),
                ..grant
            },
        )?;
        let (channel, token) = capability(&address)?;
        Ok((
            ReplicaServiceClient::new(channel)
                .max_decoding_message_size(MAX_STORAGE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_STORAGE_MESSAGE_BYTES),
            request(Empty {}, &token)?,
        ))
    }
}

#[async_trait]
impl ReplicaPeers for GrpcReplicaPeers {
    async fn initialize(&self, peer: &ReplicaTarget, session: &str) -> Result<()> {
        let (mut client, request) = self.client(
            peer,
            grant("INITIALIZE_SESSION", &peer.region, session, 60_000)?,
        )?;
        client.initialize(request).await?;
        Ok(())
    }

    async fn head(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        let (mut client, request) = self.client(
            peer,
            ReplicaGrant {
                stream: Some(stream.clone()),
                ..grant("HEAD", &peer.region, &stream.prefix, 60_000)?
            },
        )?;
        client.head(request).await?.into_inner().try_into()
    }

    async fn seal(&self, peer: &ReplicaTarget, session: &str) -> Result<SessionHead> {
        let (mut client, request) =
            self.client(peer, grant("SEAL_SESSION", &peer.region, session, 60_000)?)?;
        client.seal(request).await?.into_inner().try_into()
    }

    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        let address = self.access.url(
            &peer.url,
            &ReplicaGrant {
                host_id: peer.host_id.clone(),
                ..grant("GET", &peer.region, object, 60_000)?
            },
        )?;
        Ok(GrpcStateTransport::new().read(&address).await?.to_vec())
    }
}

pub(super) fn grant(
    operation: &str,
    region: &str,
    object: &str,
    duration_ms: u64,
) -> Result<ReplicaGrant> {
    Ok(ReplicaGrant {
        operation: operation.into(),
        region: region.into(),
        object: object.into(),
        host_id: String::new(),
        archive_url: String::new(),
        expires_at_ms: SystemClock.now_ms()?.saturating_add(duration_ms),
        stream: None,
    })
}
