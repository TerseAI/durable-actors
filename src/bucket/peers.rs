use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::{
    clock::{Clock, SystemClock},
    replication::{ReplicaAccess, ReplicaGrant, ReplicaStream, ReplicaTarget, StreamHead},
};

#[async_trait]
pub trait ReplicaPeers: Send + Sync {
    async fn initialize(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<()>;
    async fn head(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead>;
    async fn seal(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead>;
    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>>;
}

pub struct HttpReplicaPeers {
    access: ReplicaAccess,
    http: reqwest::Client,
}

impl HttpReplicaPeers {
    pub fn new(access: ReplicaAccess) -> Result<Self> {
        Ok(Self {
            access,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    fn url(
        &self,
        peer: &ReplicaTarget,
        stream: &ReplicaStream,
        operation: &str,
        resource: &str,
    ) -> Result<String> {
        self.access.url(
            &peer.url,
            resource,
            &ReplicaGrant {
                stream: Some(stream.clone()),
                host_id: peer.host_id.clone(),
                ..grant(operation, &peer.region, &stream.prefix, 60_000)?
            },
        )
    }
}

#[async_trait]
impl ReplicaPeers for HttpReplicaPeers {
    async fn initialize(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<()> {
        self.http
            .post(self.url(peer, stream, "INITIALIZE", "stream")?)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn head(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        Ok(self
            .http
            .get(self.url(peer, stream, "HEAD", "stream")?)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn seal(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        Ok(self
            .http
            .post(self.url(peer, stream, "SEAL", "seal")?)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        let url = self.access.url(
            &peer.url,
            "state",
            &ReplicaGrant {
                host_id: peer.host_id.clone(),
                ..grant("GET", &peer.region, object, 60_000)?
            },
        )?;
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?
            .to_vec())
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
