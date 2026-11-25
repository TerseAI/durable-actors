use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio_stream::wrappers::TcpListenerStream;

use super::{
    ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, ControlPlaneService,
    admin::{HostLaunchSpec, LocalAdminRegistry},
    service::HostProvisioner,
};
use crate::{
    actor::ActorKey, host_leases::HostLease, ownership::ActorClaim, ownership::Ownerships,
    payload::JsonText, sandbox::HostTermination,
};

/// Serves a control plane whose hosts renew, release and report through `ownerships`, and returns
/// its URL. It starts no hosts.
pub(crate) async fn serve_leases(
    ownerships: Arc<Ownerships>,
    issuer: &ActorJwtIssuer,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let service = ControlPlaneService::new(
        ownerships,
        ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(1800),
        )?,
        Arc::new(LocalAdminRegistry::default()),
        issuer.clone(),
        Arc::new(NoHosts),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service.into_internal_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Ok((url, server))
}

struct NoHosts;

#[async_trait]
impl HostProvisioner for NoHosts {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _: Option<&HostLaunchSpec>,
        _: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn ensure_actor_host(
        &self,
        _: &HostLaunchSpec,
        _: &str,
        _: &ActorKey,
        _: &ActorClaim,
        _: Option<&str>,
        _: Option<&crate::sandbox::assignment::InitialOperation>,
    ) -> Result<(HostLease, u64, Option<JsonText>)> {
        anyhow::bail!("this control plane starts no hosts")
    }

    async fn terminate_hosts(&self, _: &HostLaunchSpec, _: &[String]) -> Result<HostTermination> {
        anyhow::bail!("this control plane starts no hosts")
    }
}
