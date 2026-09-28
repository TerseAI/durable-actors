use super::*;
use crate::{
    bucket::{Bucket, BucketObject, FileBucket, GrpcReplicaPeers, RuntimeStorage},
    clock::SystemClock,
    host_leases::HostLeaseRequest,
    replication::{ReplicaAccess, ReplicaSet},
    sandbox::{ActorHostHandle, SocketCredentials, SocketCredentialsRequest},
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct CountingBucket {
    inner: FileBucket,
    reads: AtomicUsize,
    writes: AtomicUsize,
    read_started: tokio::sync::Semaphore,
}

#[async_trait]
impl Bucket for CountingBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read_started.add_permits(1);
        self.inner.get(key).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.compare_and_swap(key, generation, bytes).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
}

struct HostProvider {
    runtime: Arc<RuntimeStorage>,
    assigned: Mutex<Option<EnsureHostRequest>>,
}

#[async_trait]
impl SandboxProvider for HostProvider {
    fn supports_initial_invocation(&self) -> bool {
        true
    }
    async fn build_code(
        &self,
        _: &crate::sandbox::BuildCodeRequest,
    ) -> Result<crate::sandbox::BuiltActorCode> {
        anyhow::bail!("fixture does not build deployment images")
    }
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        *self.assigned.lock().unwrap() = Some(request.clone());
        let actor = request.actor.as_ref().unwrap();
        let request_lease = HostLeaseRequest {
            id: request.host_id.clone(),
            session_id: request.session_id.clone(),
            route: "https://host.test".into(),
            duration_ms: 60_000,
        };
        if actor.actor_id == "losing-claim" {
            let mut winner = request_lease.clone();
            winner.id = HostId::new(format!(
                "{}.winner",
                request.host_id.as_str().rsplit_once('.').unwrap().0
            ));
            winner.session_id = uuid::Uuid::new_v4().to_string();
            self.runtime
                .register_activation(
                    actor,
                    &winner,
                    &request.canonical_region,
                    request.actor_is_new,
                )
                .await?;
        }
        let activated = self
            .runtime
            .register_activation(
                actor,
                &request_lease,
                &request.canonical_region,
                request.actor_is_new,
            )
            .await
            .map_err(|error| error.context(crate::sandbox::InitialInvocationNotExecuted))?;
        Ok(ActorHostHandle {
            initial_outcome: None,
            host_id: request.host_id.clone(),
            route: request_lease.route,
            canonical_region: request.canonical_region.clone(),
            owner_epoch: activated.placement.owner_epoch,
            lease: Some(activated.placement.lease),
            provisioning: None,
        })
    }
    async fn socket_credentials(&self, _: &SocketCredentialsRequest) -> Result<SocketCredentials> {
        anyhow::bail!("unused")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unused")
    }
}

#[tokio::test]
async fn resolution_through_host_readiness_uses_two_bucket_operations() -> Result<()> {
    use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(CountingBucket {
        inner: FileBucket::new(directory.path().into())?,
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        read_started: tokio::sync::Semaphore::new(0),
    });
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let runtime = Arc::new(RuntimeStorage::new(
        bucket.clone(),
        Arc::new(ReplicaSet::default()),
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access.clone(),
        "http://control".into(),
        std::sync::Arc::new(crate::clock::SystemClock),
    )?);
    let provider = Arc::new(HostProvider {
        runtime: runtime.clone(),
        assigned: Mutex::new(None),
    });
    let issuer = super::tests::test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        std::time::Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            project_id: "default".into(),
            source: None,
            code_snapshot: None,
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let provisioner = Arc::new(SandboxHostProvisioner::new(
        provider.clone(),
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invocation".into(),
            host_idle_timeout_ms: 60_000,
        },
        issuer.clone(),
        Some("image".into()),
    ));
    let runtime_access = Arc::new(crate::bucket::access::RuntimeAccess::new(
        crate::bucket::access::BucketLocation::File {
            directory: directory.path().into(),
        },
        Arc::new(ReplicaSet::default()),
        access,
        runtime.clone(),
    )?);
    let registry = Arc::new(ConcurrentRegistry {
        inner: registry,
        bucket: bucket.clone(),
    });
    let service = ControlPlaneService::new(runtime, auth.clone(), registry, issuer, provisioner)
        .with_runtime_access(runtime_access);
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "new".into(),
    };
    let target = service
        .resolve_actor_route(&actor, None, None, None, None)
        .await?;
    assert_eq!(target.owner_epoch, 1);
    assert_eq!(bucket.reads.load(Ordering::SeqCst), 1);
    assert_eq!(bucket.writes.load(Ordering::SeqCst), 1);
    let assignment = provider.assigned.lock().unwrap().clone().unwrap();
    assert!(assignment.actor_is_new);
    let principal =
        auth.authenticate_authorization(&format!("Bearer {}", assignment.host_token))?;
    assert_eq!(principal.actor, actor);
    service.require_active_host(&principal).await?;
    assert_eq!(bucket.reads.load(Ordering::SeqCst), 2);
    let again = service
        .resolve_actor_route(&actor, None, None, None, None)
        .await?;
    assert_eq!(again.route, target.route);
    assert_eq!(
        bucket.reads.load(Ordering::SeqCst),
        3,
        "active routing reads the combined record once"
    );
    assert_eq!(bucket.writes.load(Ordering::SeqCst), 1);
    let ControlPlaneCommandReply::StorageAccess {
        replacement_token, ..
    } = service
        .execute_command(&principal, ControlPlaneCommand::RefreshStorageAccess)
        .await?
    else {
        panic!("missing replacement credential")
    };
    let renewed = auth.authenticate_authorization(&format!("Bearer {replacement_token}"))?;
    assert_eq!(
        renewed.actor, principal.actor,
        "credential refresh preserves actor scope"
    );
    let mut wrong_actor = principal.clone();
    wrong_actor.actor.actor_id = "other".into();
    assert!(service.require_active_host(&wrong_actor).await.is_err());
    let mut invalid = ActorHostHandle {
        initial_outcome: None,
        lease: Some(service.require_active_host(&principal).await?),
        host_id: assignment.host_id.clone(),
        route: target.route,
        canonical_region: assignment.canonical_region.clone(),
        owner_epoch: 1,
        provisioning: None,
    };
    assert!(ready_lease(&invalid, &assignment).is_ok());
    invalid.lease.as_mut().unwrap().session_id = "another-session".into();
    assert!(ready_lease(&invalid, &assignment).is_err());
    invalid.lease.as_mut().unwrap().session_id = assignment.session_id.clone();
    invalid.lease.as_mut().unwrap().expires_at_ms = 1;
    assert!(ready_lease(&invalid, &assignment).is_err());
    let initial = crate::actor::ActorInvocation {
        actor: ActorKey {
            actor_id: "missing-outcome".into(),
            ..actor
        },
        request_id: "first-write".into(),
        method: "increment".into(),
        args: vec![],
    };
    let error = service
        .resolve_actor_route(&initial.actor, None, None, None, Some(initial.clone()))
        .await
        .err()
        .expect("missing result must not trigger redispatch");
    assert!(error.is::<InitialInvocationUnknown>());
    let assigned = provider.assigned.lock().unwrap();
    assert_eq!(
        assigned
            .as_ref()
            .unwrap()
            .initial_invocation
            .as_ref()
            .unwrap()
            .invocation,
        initial
    );

    drop(assigned);
    let initial = crate::actor::ActorInvocation {
        actor: ActorKey {
            actor_id: "losing-claim".into(),
            ..initial.actor
        },
        ..initial
    };
    let winner = service
        .resolve_actor_route(&initial.actor, None, None, None, Some(initial.clone()))
        .await?;
    assert!(
        winner.initial_outcome.is_none(),
        "rejected request must be dispatched to the winning owner"
    );
    assert_eq!(winner.route, "https://host.test");
    Ok(())
}

struct ConcurrentRegistry {
    inner: Arc<super::super::admin::LocalAdminRegistry>,
    bucket: Arc<CountingBucket>,
}

#[async_trait]
impl AdminRegistry for ConcurrentRegistry {
    async fn launch_spec(&self, project: &str) -> Result<Option<HostLaunchSpec>> {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            self.bucket.read_started.acquire(),
        )
        .await
        .context("deployment lookup must overlap placement lookup")??
        .forget();
        self.inner.launch_spec(project).await
    }
    async fn register_deployment(
        &self,
        spec: &HostLaunchSpec,
        contract: Option<&super::super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        self.inner.register_deployment(spec, contract).await
    }
    async fn deployment_contract(
        &self,
        project: &str,
    ) -> Result<Option<super::super::contracts::PublishedContract>> {
        self.inner.deployment_contract(project).await
    }
    async fn launch_specs(&self) -> Result<Vec<HostLaunchSpec>> {
        self.inner.launch_specs().await
    }
    async fn remove_deployment(&self, project: &str) -> Result<()> {
        self.inner.remove_deployment(project).await
    }
}
