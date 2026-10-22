use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Cluster {
    creates: AtomicUsize,
    retires: AtomicUsize,
}
#[async_trait]
impl SandboxCluster for Cluster {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn allocate_spare(&self, _: &CreateSpareRequest) -> Result<SpareHandle> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        Ok(request()?.spare.unwrap())
    }
    async fn wait_ready(&self, _: &SpareHandle) -> Result<()> {
        Ok(())
    }
    async fn retire_spare(&self, _: &SpareHandle) -> Result<()> {
        self.retires.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn secrets(&self, _: &[String]) -> Result<HashMap<String, String>> {
        Ok(HashMap::from([(
            "CUSTOMER_SECRET".into(),
            "assigned-secret".into(),
        )]))
    }
}
struct Assign;
#[async_trait]
impl HostAssignment for Assign {
    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        assert_eq!(
            environment["DURABLE_ACTORS_ENTRYPOINT"],
            "/customer/actors.mjs"
        );
        assert!(environment["DURABLE_ACTORS_CODE_ARTIFACT"].starts_with("gcs:"));
        let secrets: HashMap<String, String> =
            serde_json::from_str(&environment["DURABLE_ACTORS_CUSTOMER_ENV"])?;
        assert_eq!(secrets["CUSTOMER_SECRET"], "assigned-secret");
        let handoff: ActivationHandoff =
            serde_json::from_str(&environment["DURABLE_ACTORS_ACTIVATION_HANDOFF"])?;
        let host = HostId::new(environment["DURABLE_ACTORS_HOST_ID"].clone());
        Ok(ActorHostHandle {
            lease: Some(handoff.lease().clone()),
            owner_epoch: handoff.owner_epoch(),
            host_id: host,
            route: spare.route.clone(),
            canonical_region: spare.canonical_region.clone(),
            provisioning: None,
        })
    }
}

#[tokio::test]
async fn prewarmed_assignment_uses_ready_host_without_creating_a_pod() -> Result<()> {
    let cluster = Arc::new(Cluster {
        creates: AtomicUsize::new(0),
        retires: AtomicUsize::new(0),
    });
    let provider = RemoteSandboxProvider {
        name: "gke",
        cluster: cluster.clone(),
        bootstrap: Arc::new(Bootstrap::new()?),
        assignment: Arc::new(Assign),
    };
    let request = request()?;
    let result = provider.ensure_host(&request).await?;
    assert_eq!(result.owner_epoch, 1);
    assert!(result.provisioning.unwrap().reused);
    assert_eq!(cluster.creates.load(Ordering::SeqCst), 0);
    Ok(())
}

fn request() -> Result<EnsureHostRequest> {
    let manifest = crate::bundle::ActorBundle {
        bucket: "code-bucket".into(),
        files: vec![crate::bundle::BundleFile {
            path: "actors.mjs".into(),
            object: "code/actors.mjs".into(),
            generation: 1,
            sha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
        }],
    };
    Ok(EnsureHostRequest {
        actor_is_new: true,
        owner_hint: None,
        actor: Some(crate::actor::ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        }),
        code_snapshot: Some(manifest.encode()?),
        spare: Some(SpareHandle {
            control_route: "http://10.0.0.1:7102".into(),
            control_token: "token".into(),
            name: "warm".into(),
            resource_id: "sandboxes/warm/uid".into(),
            route: "http://10.0.0.1:7101".into(),
            canonical_region: "north-america-west".into(),
        }),
        resources: ResourceLimits::default(),
        runtime_config: Some("{}".into()),
        host_config_key: "cfg.test".into(),
        canonical_region: "north-america-west".into(),
        host_id: HostId::new("host.v3.test.one"),
        session_id: uuid::Uuid::new_v4().to_string(),
        host_token: "host-jwt".into(),
        jwt_public_keys: "{}".into(),
        control_plane_url: "http://control:7100".into(),
        jwt_issuer: "issuer".into(),
        invocation_jwt_audience: "invoke".into(),
        image_ref: format!("registry/runtime@sha256:{}", "a".repeat(64)),
        working_directory: "/customer".into(),
        actor_entrypoint: Some("/customer/actors.mjs".into()),
        secret_refs: vec!["customer".into()],
        host_idle_timeout_ms: 10000,
    })
}

#[test]
fn python_assignment_uses_its_manifest_entrypoint() -> Result<()> {
    let mut request = request()?;
    let mut manifest = crate::bundle::ActorBundle::decode(request.code_snapshot.as_ref().unwrap())?;
    manifest.files[0].path = "actors.pyz".into();
    request.code_snapshot = Some(manifest.encode()?);
    request.actor_entrypoint = Some("actors.pyz".into());
    let environment =
        assignment_environment(&request, request.spare.as_ref().unwrap(), HashMap::new())?;
    assert_eq!(
        environment["DURABLE_ACTORS_ENTRYPOINT"],
        "/customer/actors.pyz"
    );
    Ok(())
}

struct ExpiredAssignment;
#[async_trait]
impl HostAssignment for ExpiredAssignment {
    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        let mut handle = Assign.assign(spare, environment).await?;
        handle.lease.as_mut().unwrap().expires_at_ms = 0;
        Ok(handle)
    }
}

#[tokio::test]
async fn assignment_rejects_a_host_whose_lease_expired_before_its_reply() -> Result<()> {
    let provider = RemoteSandboxProvider {
        name: "gke",
        cluster: Arc::new(Cluster {
            creates: AtomicUsize::new(0),
            retires: AtomicUsize::new(0),
        }),
        bootstrap: Arc::new(Bootstrap::new()?),
        assignment: Arc::new(ExpiredAssignment),
    };
    assert!(provider.ensure_host(&request()?).await.is_err());
    Ok(())
}

#[tokio::test]
async fn modal_assignment_failure_retires_the_claimed_spare() -> Result<()> {
    let cluster = Arc::new(Cluster {
        creates: AtomicUsize::new(0),
        retires: AtomicUsize::new(0),
    });
    let provider = RemoteSandboxProvider {
        name: "modal",
        cluster: cluster.clone(),
        bootstrap: Arc::new(Bootstrap::new()?),
        assignment: Arc::new(ExpiredAssignment),
    };
    assert!(provider.ensure_host(&request()?).await.is_err());
    assert_eq!(cluster.retires.load(Ordering::SeqCst), 1);
    assert_eq!(cluster.creates.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn modal_reuses_the_same_assignment_protocol_and_reports_its_provider() -> Result<()> {
    let provider = RemoteSandboxProvider {
        name: "modal",
        cluster: Arc::new(Cluster {
            creates: AtomicUsize::new(0),
            retires: AtomicUsize::new(0),
        }),
        bootstrap: Arc::new(Bootstrap::new()?),
        assignment: Arc::new(Assign),
    };
    let request = request()?;
    let result = provider.ensure_host(&request).await?;
    let provisioning = result.provisioning.unwrap();
    assert_eq!(provisioning.provider, "modal");
    assert!(provisioning.reused);
    assert_eq!(provisioning.resource_id, request.spare.unwrap().resource_id);
    Ok(())
}

struct Bootstrap {
    fixture: crate::bucket::testing::RuntimeFixture,
    releases: AtomicUsize,
    renewals: AtomicUsize,
    fail_credentials: bool,
    credentials: Option<Arc<Gate>>,
    claim: Option<Arc<Gate>>,
}

impl Bootstrap {
    fn new() -> Result<Self> {
        Ok(Self {
            fixture: crate::bucket::testing::RuntimeFixture::new()?,
            releases: AtomicUsize::new(0),
            renewals: AtomicUsize::new(0),
            fail_credentials: false,
            credentials: None,
            claim: None,
        })
    }
}

#[async_trait]
impl HostBootstrap for Bootstrap {
    async fn credentials(&self, _: &EnsureHostRequest) -> Result<String> {
        if let Some(gate) = &self.credentials {
            gate.wait().await;
        }
        ensure!(!self.fail_credentials, "credentials unavailable");
        Ok("{}".into())
    }
    async fn claim(&self, request: &EnsureHostRequest, route: &str) -> Result<ActivationHandoff> {
        if let Some(gate) = &self.claim {
            gate.wait().await;
        }
        self.fixture
            .runtime
            .prepare_activation(
                request.actor.as_ref().unwrap(),
                &lease_request(request, route),
                &request.canonical_region,
                true,
                None,
            )
            .await
    }
    async fn renew(&self, request: &EnsureHostRequest, route: &str) -> Result<ActivationHandoff> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        self.fixture
            .runtime
            .renew_prepared_activation(
                request.actor.as_ref().unwrap(),
                &lease_request(request, route),
            )
            .await
    }
    async fn release(&self, request: &EnsureHostRequest) -> Result<()> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.fixture
            .runtime
            .release_activation(
                request.actor.as_ref().unwrap(),
                &request.host_id,
                &request.session_id,
            )
            .await
    }
}

fn lease_request(request: &EnsureHostRequest, route: &str) -> crate::host_leases::HostLeaseRequest {
    crate::host_leases::HostLeaseRequest {
        id: request.host_id.clone(),
        session_id: request.session_id.clone(),
        route: route.into(),
        duration_ms: 30_000,
    }
}

struct Gate {
    entered: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Semaphore::new(0),
            resume: tokio::sync::Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        self.entered.add_permits(1);
        self.resume.acquire().await.unwrap().forget();
    }
    async fn reached(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), self.entered.acquire())
            .await??
            .forget();
        Ok(())
    }
    fn release(&self) {
        self.resume.add_permits(1);
    }
}

struct StartingCluster {
    cluster: Cluster,
    allocation: Option<Arc<Gate>>,
    readiness: Arc<Gate>,
    fail_ready: bool,
}
#[async_trait]
impl SandboxCluster for StartingCluster {
    async fn allocate_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        if let Some(gate) = &self.allocation {
            gate.wait().await;
        }
        self.cluster.allocate_spare(request).await
    }
    async fn wait_ready(&self, _: &SpareHandle) -> Result<()> {
        self.readiness.wait().await;
        ensure!(!self.fail_ready, "readiness failed");
        Ok(())
    }
    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        self.cluster.retire_spare(spare).await
    }
    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        self.cluster.stopped_spares(spares).await
    }
    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>> {
        self.cluster.secrets(names).await
    }
}

#[tokio::test]
async fn credentials_overlap_allocation_and_ownership_overlaps_readiness() -> Result<()> {
    let credentials = Gate::new();
    let allocation = Gate::new();
    let ownership = Gate::new();
    let readiness = Gate::new();
    let mut bootstrap = Bootstrap::new()?;
    bootstrap.credentials = Some(credentials.clone());
    bootstrap.claim = Some(ownership.clone());
    let provider = RemoteSandboxProvider {
        name: "modal",
        bootstrap: Arc::new(bootstrap),
        assignment: Arc::new(Assign),
        cluster: Arc::new(StartingCluster {
            cluster: Cluster::default(),
            allocation: Some(allocation.clone()),
            readiness: readiness.clone(),
            fail_ready: false,
        }),
    };
    let mut request = request()?;
    request.spare = None;
    let task = tokio::spawn(async move { provider.ensure_host(&request).await });
    credentials.reached().await?;
    allocation.reached().await?;
    allocation.release();
    ownership.reached().await?;
    readiness.reached().await?;
    assert!(!task.is_finished());
    credentials.release();
    ownership.release();
    readiness.release();
    assert!(!task.await??.provisioning.unwrap().reused);
    Ok(())
}

#[tokio::test]
async fn failure_before_assignment_releases_the_claim_and_retires_the_sandbox() -> Result<()> {
    use crate::placement::ObjectPlacementStore;
    let mut bootstrap = Bootstrap::new()?;
    bootstrap.fail_credentials = true;
    let bootstrap = Arc::new(bootstrap);
    let cluster = Arc::new(Cluster::default());
    let provider = RemoteSandboxProvider {
        name: "modal",
        bootstrap: bootstrap.clone(),
        cluster: cluster.clone(),
        assignment: Arc::new(Assign),
    };
    let request = request()?;
    assert!(provider.ensure_host(&request).await.is_err());
    assert_eq!(bootstrap.releases.load(Ordering::SeqCst), 1);
    assert_eq!(cluster.retires.load(Ordering::SeqCst), 1);
    let owner = bootstrap
        .fixture
        .runtime
        .get_owner(&request.actor.unwrap().storage_key())
        .await?
        .unwrap();
    assert_eq!(owner.lease.expires_at_ms, 0);
    Ok(())
}

#[tokio::test]
async fn ambiguous_assignment_does_not_release_a_possible_writer() -> Result<()> {
    use crate::placement::ObjectPlacementStore;
    let bootstrap = Arc::new(Bootstrap::new()?);
    let cluster = Arc::new(Cluster::default());
    let provider = RemoteSandboxProvider {
        name: "gke",
        bootstrap: bootstrap.clone(),
        cluster: cluster.clone(),
        assignment: Arc::new(ExpiredAssignment),
    };
    let request = request()?;
    assert!(provider.ensure_host(&request).await.is_err());
    assert_eq!(bootstrap.releases.load(Ordering::SeqCst), 0);
    assert_eq!(cluster.retires.load(Ordering::SeqCst), 1);
    let owner = bootstrap
        .fixture
        .runtime
        .get_owner(&request.actor.unwrap().storage_key())
        .await?
        .unwrap();
    assert!(owner.lease.expires_at_ms > now_ms()?);
    Ok(())
}

#[tokio::test]
async fn cancelled_startup_retires_the_sandbox_and_releases_unassigned_ownership() -> Result<()> {
    use crate::placement::ObjectPlacementStore;
    let readiness = Gate::new();
    let bootstrap = Arc::new(Bootstrap::new()?);
    let cluster = Arc::new(StartingCluster {
        cluster: Cluster::default(),
        allocation: None,
        readiness: readiness.clone(),
        fail_ready: false,
    });
    let provider = RemoteSandboxProvider {
        name: "modal",
        bootstrap: bootstrap.clone(),
        cluster: cluster.clone(),
        assignment: Arc::new(Assign),
    };
    let mut request = request()?;
    request.spare = None;
    let actor = request.actor.as_ref().unwrap().clone();
    let task = tokio::spawn(async move { provider.ensure_host(&request).await });
    readiness.reached().await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while bootstrap
            .fixture
            .runtime
            .get_owner(&actor.storage_key())
            .await?
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        anyhow::Ok(())
    })
    .await??;
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if bootstrap
                .fixture
                .runtime
                .get_owner(&actor.storage_key())
                .await?
                .is_some_and(|owner| owner.lease.expires_at_ms == 0)
            {
                return anyhow::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(cluster.cluster.retires.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_slow_sandbox_keeps_its_prepared_ownership_alive() -> Result<()> {
    use crate::placement::ObjectPlacementStore;
    let readiness = Gate::new();
    let bootstrap = Arc::new(Bootstrap::new()?);
    let provider = RemoteSandboxProvider {
        name: "gke",
        bootstrap: bootstrap.clone(),
        assignment: Arc::new(Assign),
        cluster: Arc::new(StartingCluster {
            cluster: Cluster::default(),
            allocation: None,
            readiness: readiness.clone(),
            fail_ready: false,
        }),
    };
    let mut request = request()?;
    request.spare = None;
    let actor = request.actor.as_ref().unwrap().clone();
    let task = tokio::spawn(async move { provider.ensure_host(&request).await });
    readiness.reached().await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while bootstrap
            .fixture
            .runtime
            .get_owner(&actor.storage_key())
            .await?
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        anyhow::Ok(())
    })
    .await??;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(3), async {
        while bootstrap.renewals.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    readiness.release();
    task.await??;
    assert_eq!(bootstrap.releases.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn a_failed_readiness_check_releases_the_prepared_activation() -> Result<()> {
    let readiness = Gate::new();
    let bootstrap = Arc::new(Bootstrap::new()?);
    let cluster = Arc::new(StartingCluster {
        cluster: Cluster::default(),
        allocation: None,
        readiness: readiness.clone(),
        fail_ready: true,
    });
    let provider = RemoteSandboxProvider {
        name: "gke",
        bootstrap: bootstrap.clone(),
        cluster: cluster.clone(),
        assignment: Arc::new(Assign),
    };
    let mut request = request()?;
    request.spare = None;
    readiness.release();
    assert!(provider.ensure_host(&request).await.is_err());
    assert_eq!(bootstrap.releases.load(Ordering::SeqCst), 1);
    assert_eq!(cluster.cluster.retires.load(Ordering::SeqCst), 1);
    Ok(())
}
