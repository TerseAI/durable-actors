use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, Endpoint},
};

use crate::{
    actor::ActorKey,
    grpc::proto::actor_control_plane_service_client::ActorControlPlaneServiceClient,
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
}

impl ControlPlaneClient {
    pub(crate) async fn report_traces(
        &self,
        traces: Vec<crate::request_traces::RequestTrace>,
        dropped: u64,
    ) -> Result<()> {
        self.execute(ControlPlaneCommand::RequestTraces { traces, dropped })
            .await?;
        Ok(())
    }

    pub(crate) async fn notify_inventory_changed(&self) -> Result<()> {
        match self.execute(ControlPlaneCommand::InventoryChanged).await? {
            ControlPlaneCommandReply::Unit => Ok(()),
            _ => anyhow::bail!("unexpected inventory notification response"),
        }
    }

    pub(crate) async fn notify_socket_message(
        &self,
        actor: ActorKey,
        event: crate::actor::ActorSocketEvent,
    ) -> Result<()> {
        self.execute(ControlPlaneCommand::SocketMessage { actor, event })
            .await?;
        Ok(())
    }

    pub(crate) fn token_expires_at_ms(&self) -> Result<u64> {
        use base64::Engine;
        let authorization = self
            .authorization
            .read()
            .map_err(|_| anyhow::anyhow!("authorization lock poisoned"))?;
        let payload = authorization
            .to_str()?
            .split('.')
            .nth(1)
            .context("host JWT has no payload")?;
        let claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload)?,
        )?;
        claims["exp"]
            .as_u64()
            .and_then(|value| value.checked_mul(1000))
            .context("host JWT has no expiration")
    }

    pub(crate) async fn refresh_storage_access(
        &self,
    ) -> Result<Option<crate::bucket::access::StorageToken>> {
        match self
            .execute(ControlPlaneCommand::RefreshStorageAccess)
            .await?
        {
            ControlPlaneCommandReply::StorageAccess {
                token,
                replacement_token,
            } => {
                self.replace_token(&replacement_token)?;
                Ok(token)
            }
            _ => anyhow::bail!("unexpected storage credentials response"),
        }
    }

    pub async fn connect(endpoint: impl Into<String>, token: impl AsRef<str>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Endpoint::new(endpoint.clone())
            .context("parse actor control-plane endpoint")?
            .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
            .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            client: ActorControlPlaneServiceClient::new(channel)
                .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES),
            authorization: Arc::new(RwLock::new(bearer_authorization(token.as_ref())?)),
        })
    }
}

#[derive(Default)]
pub(crate) struct LeaseFence {
    deadline: Option<Instant>,
    pub(crate) fenced: bool,
}

impl LeaseFence {
    pub(crate) fn begin(&mut self, now: Instant) -> Result<()> {
        if self.deadline.is_some() {
            self.check(now)?;
        }
        ensure!(!self.fenced, "host session is permanently fenced");
        Ok(())
    }

    pub(crate) fn confirm(
        &mut self,
        started: Instant,
        duration: Duration,
        now: Instant,
    ) -> Result<()> {
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

    pub(crate) fn check(&mut self, now: Instant) -> Result<()> {
        let deadline = self.deadline.context("host lease is not confirmed yet")?;
        if deadline <= now {
            self.fenced = true;
        }
        ensure!(!self.fenced, "host session has no confirmed live lease");
        Ok(())
    }
}

impl ControlPlaneClient {
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

#[cfg(test)]
mod refresh_tests {
    use super::*;
    use crate::grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
    };

    struct LocalCredentials;
    #[tonic::async_trait]
    impl ActorControlPlaneService for LocalCredentials {
        async fn execute(
            &self,
            _: Request<ControlPlaneRequest>,
        ) -> Result<tonic::Response<ControlPlaneReply>, tonic::Status> {
            Ok(tonic::Response::new(
                super::super::protocol::encode_reply(ControlPlaneCommandReply::StorageAccess {
                    token: None,
                    replacement_token: "renewed-local-token".into(),
                })
                .unwrap(),
            ))
        }
    }

    #[tokio::test]
    async fn local_hosts_refresh_socket_credentials_without_a_cloud_token() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = ControlPlaneClient::connect(
            format!("http://{}", listener.local_addr()?),
            "initial-token",
        )
        .await?;
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ActorControlPlaneServiceServer::new(LocalCredentials))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let result = client.refresh_storage_access().await;
        server.abort();
        result?;
        assert_eq!(
            client.authorization.read().unwrap().to_str()?,
            "Bearer renewed-local-token"
        );
        Ok(())
    }
}
