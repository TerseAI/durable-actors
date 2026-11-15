use super::{
    ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, ControlPlaneService,
    admin::{HostLaunchSpec, LocalAdminRegistry},
    service::HostProvisioner,
};
use crate::{
    actor::ActorKey,
    bucket::{FileBucket, RuntimeStorage, RuntimeStorageReader},
    clock::SystemClock,
    host_leases::HostLease,
    sandbox::HostTermination,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_stream::wrappers::TcpListenerStream;

/// Serves a control plane that applies hosts' lease renewals and releases to the file bucket at
/// `directory`, and returns its URL.
pub(crate) async fn serve_activations(
    directory: &Path,
    issuer: &ActorJwtIssuer,
    tasks: &mut JoinSet<()>,
) -> Result<String> {
    let bucket = Arc::new(FileBucket::new(directory.into())?);
    let service = ControlPlaneService::new(
        Arc::new(RuntimeStorage::new(bucket.clone(), Arc::new(SystemClock))?),
        ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(3600),
        )?,
        Arc::new(LocalAdminRegistry::default()),
        issuer.clone(),
        Arc::new(NoHosts),
    )
    .with_activation_authority(Arc::new(RuntimeStorageReader::new(
        bucket,
        Arc::new(SystemClock),
        Arc::new(crate::litestream::EmbeddedRestore),
    )?));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tasks.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service.into_internal_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Ok(url)
}

/// Fixture hosts are started by the test itself.
pub(crate) struct NoHosts;

#[async_trait]
impl HostProvisioner for NoHosts {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn ensure_actor_host(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
        _owner_hint: Option<&crate::bucket::OwnershipHint>,
        _: Option<&crate::sandbox::assignment::InitialOperation>,
    ) -> Result<(HostLease, u64, Option<serde_json::Value>)> {
        anyhow::bail!("fixture host must be active")
    }

    async fn terminate_hosts(
        &self,
        _spec: &HostLaunchSpec,
        _regions: &[String],
    ) -> Result<HostTermination> {
        anyhow::bail!("unused")
    }
}
