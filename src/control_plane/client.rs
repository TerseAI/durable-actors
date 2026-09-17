use std::{
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, Endpoint},
};

use crate::{
    actor::{
        ActorKey, ActorSocketConnection, ActorSocketEffect, ActorSocketPublisher, ActorSocketSource,
    },
    grpc::proto::actor_control_plane_service_client::ActorControlPlaneServiceClient,
    host::HostId,
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
    storage_urls::StateWriteTicket,
};

use super::{
    CONTROL_PLANE_REQUEST_TIMEOUT, MAX_CONTROL_PLANE_MESSAGE_BYTES,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_reply, encode_command},
};

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ControlPlaneClient {
    client: ActorControlPlaneServiceClient<Channel>,
    authorization: Arc<RwLock<MetadataValue<tonic::metadata::Ascii>>>,
    socket_gateway: String,
    http: reqwest::Client,
    lease_fence: Arc<Mutex<LeaseFence>>,
}

impl ControlPlaneClient {
    pub(crate) async fn load_actor_state(
        &self,
        actor: &ActorKey,
        host_id: &HostId,
        owner_epoch: u64,
    ) -> Result<(u64, String)> {
        match self
            .execute(ControlPlaneCommand::LoadActorState {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch,
            })
            .await?
        {
            ControlPlaneCommandReply::ActorState {
                state_version,
                state_read_url,
            } => Ok((state_version, state_read_url)),
            reply => anyhow::bail!("unexpected load-actor-state reply: {reply:?}"),
        }
    }

    pub async fn connect(endpoint: impl Into<String>, token: impl AsRef<str>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Endpoint::new(endpoint.clone())
            .context("parse actor control-plane endpoint")?
            .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
            .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
            .connect()
            .await
            .context("connect to actor control plane")?;
        Ok(Self {
            lease_fence: Arc::new(Mutex::new(LeaseFence::default())),
            socket_gateway: endpoint,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
                .build()?,
            client: ActorControlPlaneServiceClient::new(channel)
                .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES),
            authorization: Arc::new(RwLock::new(bearer_authorization(token.as_ref())?)),
        })
    }

    pub(crate) fn with_socket_gateway(mut self, endpoint: &str) -> Self {
        self.socket_gateway = endpoint.to_owned();
        self
    }

    pub(crate) fn ensure_live_lease(&self) -> Result<()> {
        self.lease_fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .check(Instant::now())
    }

    pub async fn prepare_state_write(
        &self,
        actor: &ActorKey,
        host_id: &HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<StateWriteTicket> {
        match self
            .execute(ControlPlaneCommand::PrepareStateWrite {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch,
                expected_version,
            })
            .await?
        {
            ControlPlaneCommandReply::StateWriteTicket { ticket } => Ok(ticket),
            reply => anyhow::bail!("unexpected prepare-state-write reply: {reply:?}"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_state(
        &self,
        actor: &ActorKey,
        host_id: &HostId,
        owner_epoch: u64,
        expected_version: u64,
        state_object: &str,
        request_id: &str,
    ) -> Result<(u64, Option<StateWriteTicket>)> {
        match self
            .execute(ControlPlaneCommand::CommitState {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch,
                expected_version,
                state_object: state_object.to_owned(),
                request_id: request_id.to_owned(),
            })
            .await?
        {
            ControlPlaneCommandReply::StateCommitted {
                state_version,
                next_write,
            } => Ok((state_version, next_write)),
            reply => anyhow::bail!("unexpected commit-state reply: {reply:?}"),
        }
    }
}

#[async_trait]
impl ActorSocketSource for ControlPlaneClient {
    async fn connections(&self, actor: &ActorKey) -> Result<Vec<ActorSocketConnection>> {
        let response = self
            .socket_request(reqwest::Method::GET, actor, "connections")?
            .send()
            .await
            .context("load actor connections")?
            .error_for_status()
            .context("socket gateway rejected connection lookup")?;
        response.json().await.context("decode actor connections")
    }
}

#[async_trait]
impl ActorSocketPublisher for ControlPlaneClient {
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        let response = self
            .socket_request(reqwest::Method::POST, actor, "socket-effects")?
            .json(&serde_json::json!({ "effects": effects }))
            .send()
            .await
            .context("publish actor socket output")?;
        ensure!(
            response.status().is_success(),
            "socket gateway rejected actor output with HTTP {}",
            response.status()
        );
        Ok(())
    }
}

#[async_trait]
impl HostLeaseRegistry for ControlPlaneClient {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        let started = Instant::now();
        self.lease_fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .begin(started)?;
        match self
            .execute(ControlPlaneCommand::RegisterLease {
                request: request.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Lease {
                lease,
                replacement_token,
            } => {
                ensure!(
                    lease.id == request.id && lease.session_id == request.session_id,
                    "lease reply has another identity"
                );
                self.lease_fence
                    .lock()
                    .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
                    .confirm(
                        started,
                        Duration::from_millis(request.duration_ms),
                        Instant::now(),
                    )?;
                if let Some(token) = replacement_token {
                    self.replace_token(&token)?;
                }
                Ok(lease)
            }
            reply => anyhow::bail!("unexpected register-lease reply: {reply:?}"),
        }
    }

    async fn unregister(&self, id: &HostId, _session_id: &str) -> Result<()> {
        self.lease_fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .fenced = true;
        match self
            .execute(ControlPlaneCommand::UnregisterLease {
                host_id: id.clone(),
            })
            .await?
        {
            ControlPlaneCommandReply::Unit => Ok(()),
            reply => anyhow::bail!("unexpected unregister-lease reply: {reply:?}"),
        }
    }
}

#[derive(Default)]
struct LeaseFence {
    deadline: Option<Instant>,
    fenced: bool,
}

impl LeaseFence {
    fn begin(&mut self, now: Instant) -> Result<()> {
        if self.deadline.is_some() {
            self.check(now)?;
        }
        ensure!(!self.fenced, "host session is permanently fenced");
        Ok(())
    }

    fn confirm(&mut self, started: Instant, duration: Duration, now: Instant) -> Result<()> {
        self.begin(now)?;
        let window = duration.saturating_sub(Duration::from_secs(5));
        let deadline = started + window;
        if deadline <= now {
            self.fenced = true;
        }
        ensure!(
            !self.fenced,
            "host lease response arrived after the safe lease window"
        );
        self.deadline = Some(deadline);
        Ok(())
    }

    fn check(&mut self, now: Instant) -> Result<()> {
        let deadline = self.deadline.context("host lease is not confirmed yet")?;
        if deadline <= now {
            self.fenced = true;
        }
        ensure!(!self.fenced, "host session has no confirmed live lease");
        Ok(())
    }
}

impl ControlPlaneClient {
    fn socket_request(
        &self,
        method: reqwest::Method,
        actor: &ActorKey,
        resource: &str,
    ) -> Result<reqwest::RequestBuilder> {
        actor.validate()?;
        let authorization = self
            .authorization
            .read()
            .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))?
            .to_str()?
            .to_owned();
        let url = format!(
            "{}/v1/namespaces/{}/actors/{}/{}/{resource}",
            self.socket_gateway.trim_end_matches('/'),
            actor.namespace_id,
            actor.actor_type,
            actor.actor_id
        );
        Ok(self
            .http
            .request(method, url)
            .header("authorization", authorization))
    }

    async fn execute(&self, command: ControlPlaneCommand) -> Result<ControlPlaneCommandReply> {
        let mut request = Request::new(encode_command(command)?);
        request.set_timeout(CONTROL_PLANE_REQUEST_TIMEOUT);
        request.metadata_mut().insert(
            "authorization",
            self.authorization
                .read()
                .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))?
                .clone(),
        );
        let reply = self
            .client
            .clone()
            .execute(request)
            .await
            .context("execute actor control-plane command")?
            .into_inner();
        decode_reply(reply).context("decode actor control-plane reply")
    }

    fn replace_token(&self, token: &str) -> Result<()> {
        *self
            .authorization
            .write()
            .map_err(|_| anyhow::anyhow!("actor authorization lock poisoned"))? =
            bearer_authorization(token)?;
        Ok(())
    }
}

fn bearer_authorization(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    ensure!(
        !token.is_empty() && token.trim() == token,
        "actor token is invalid"
    );
    format!("Bearer {token}")
        .parse()
        .context("actor token is not valid gRPC metadata")
}

#[cfg(test)]
mod lease_fence_tests {
    use super::*;

    #[test]
    fn an_early_request_does_not_prevent_initial_lease_confirmation() -> Result<()> {
        let now = Instant::now();
        let mut fence = LeaseFence::default();
        assert!(fence.check(now).is_err());
        fence.confirm(now, Duration::from_secs(30), now + Duration::from_secs(1))?;
        fence.check(now + Duration::from_secs(2))
    }

    #[test]
    fn slow_renewal_cannot_revive_an_expired_process() -> Result<()> {
        let start = Instant::now();
        let mut fence = LeaseFence::default();
        fence.confirm(
            start,
            Duration::from_secs(30),
            start + Duration::from_secs(1),
        )?;
        fence.check(start + Duration::from_secs(24))?;
        assert!(
            fence
                .confirm(
                    start + Duration::from_secs(20),
                    Duration::from_secs(30),
                    start + Duration::from_secs(26)
                )
                .is_err()
        );
        assert!(fence.begin(start + Duration::from_secs(27)).is_err());
        Ok(())
    }

    #[test]
    fn response_gate_expires_from_request_start_even_without_a_timer() -> Result<()> {
        let start = Instant::now();
        let mut fence = LeaseFence::default();
        fence.confirm(
            start,
            Duration::from_secs(30),
            start + Duration::from_secs(10),
        )?;
        assert!(fence.check(start + Duration::from_secs(25)).is_err());
        Ok(())
    }
}
