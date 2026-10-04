use super::*;
use std::sync::Mutex;

#[test]
fn actor_names_fit_substrate_dns_labels_and_keep_sessions_distinct() -> Result<()> {
    let first = actor_name(&HostId::new("host.v3.cfg.test.first"))?;
    let second = actor_name(&HostId::new("host.v3.cfg.test.second"))?;
    assert!(first.len() <= 63, "actor name is {} bytes", first.len());
    assert!(
        first
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    );
    assert_ne!(first, second);
    assert_eq!(
        first.rsplit_once('-').unwrap().0,
        second.rsplit_once('-').unwrap().0
    );
    Ok(())
}

#[test]
fn template_sizes_share_capacity_without_sharing_customer_identity() -> Result<()> {
    let mut request = request()?;
    let config = config();
    let small = template::build(&config, &runtime_template(&request), &request.resources)?;
    request.resources = ResourceLimits {
        cpu_millis: 750,
        memory_mib: 768,
    };
    let large = template::build(&config, &runtime_template(&request), &request.resources)?;
    assert_ne!(
        small.metadata.as_ref().unwrap().name,
        large.metadata.as_ref().unwrap().name
    );
    assert_eq!(small.worker_selector, large.worker_selector);
    assert_eq!(large.resources.unwrap().limits[0].quantity, "750m");
    let environment = &small.containers[0].env;
    assert!(
        environment
            .iter()
            .any(|v| v.name == "DURABLE_ACTORS_SANDBOX_IDENTITY_FILE")
    );
    assert!(
        environment
            .iter()
            .all(|v| !v.value.contains(&request.host_token))
    );
    Ok(())
}

#[tokio::test]
async fn failed_assignment_deletes_the_allocated_actor() -> Result<()> {
    let api = Arc::new(Api::new(Vec::new()));
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        assignment: Arc::new(FailingAssignment),
        config: config(),
        issuer: issuer()?,
    };
    assert!(provider.ensure_host(&request()?).await.is_err());
    assert_eq!(
        *api.0.lock().unwrap(),
        ["template", "create", "egress", "resume", "delete"]
    );
    Ok(())
}

struct Api(
    Mutex<Vec<&'static str>>,
    Vec<proto::Actor>,
    Mutex<HashMap<String, proto::Tag>>,
    Mutex<Vec<proto::Actor>>,
    bool,
);
impl Api {
    fn new(actors: Vec<proto::Actor>) -> Self {
        Self(
            Mutex::new(Vec::new()),
            actors,
            Mutex::new(HashMap::new()),
            Mutex::new(Vec::new()),
            false,
        )
    }
}
struct TestCodeSource;
#[async_trait]
impl code::CodeSource for TestCodeSource {
    async fn open(&self, _: &str, _: &crate::artifacts::ArtifactFile) -> Result<code::CodeStream> {
        Ok(stream::once(async { Ok(bytes::Bytes::from_static(b"test-code")) }).boxed())
    }
}
#[async_trait]
impl SubstrateApi for Api {
    async fn template(&self, mut template: proto::ActorTemplate) -> Result<proto::ActorTemplate> {
        self.0.lock().unwrap().push("template");
        template.metadata.as_mut().unwrap().uid =
            format!("uid-{}", template.metadata.as_ref().unwrap().name);
        template.status = Some(proto::ActorTemplateStatus {
            golden_snapshot_status: Some(proto::GoldenSnapshotStatus {
                golden_tag: Some(reference("staging", "golden")),
                ..Default::default()
            }),
        });
        Ok(template)
    }
    async fn tag(&self, target: proto::ObjectRef) -> Result<Option<proto::Tag>> {
        self.0.lock().unwrap().push("tag");
        Ok(self.2.lock().unwrap().get(&target.name).cloned())
    }
    async fn suspend(&self, _: proto::ObjectRef) -> Result<()> {
        self.0.lock().unwrap().push("suspend");
        Ok(())
    }
    async fn create_tag(&self, mut tag: proto::Tag) -> Result<()> {
        self.0.lock().unwrap().push("create_tag");
        ensure!(!self.4, "snapshot failed");
        let source = self
            .3
            .lock()
            .unwrap()
            .iter()
            .find(|a| a.metadata.as_ref().unwrap().name == tag.source_actor.as_ref().unwrap().name)
            .cloned()
            .unwrap();
        tag.status = Some(proto::TagStatus {
            snapshot: Some(Default::default()),
            actor_template_uid: format!("uid-{}", source.actor_template.unwrap().name),
            ..Default::default()
        });
        self.2
            .lock()
            .unwrap()
            .insert(tag.metadata.as_ref().unwrap().name.clone(), tag);
        Ok(())
    }
    async fn create(&self, mut actor: proto::Actor) -> Result<proto::Actor> {
        self.0.lock().unwrap().push("create");
        actor.metadata.as_mut().unwrap().uid = "restored-uid".into();
        self.3.lock().unwrap().push(actor.clone());
        Ok(actor)
    }
    async fn egress(&self, _: proto::ObjectRef, _: Vec<proto::EgressRule>) -> Result<()> {
        self.0.lock().unwrap().push("egress");
        Ok(())
    }
    async fn resume(&self, _: proto::ObjectRef) -> Result<()> {
        self.0.lock().unwrap().push("resume");
        Ok(())
    }
    async fn delete(&self, _: proto::Actor, _: Option<i64>) -> Result<()> {
        self.0.lock().unwrap().push("delete");
        Ok(())
    }
    async fn actors(&self, _: &str) -> Result<Vec<proto::Actor>> {
        Ok(self.1.clone())
    }
    async fn secrets(&self, _: &[String]) -> Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }
}
struct FailingAssignment;
#[async_trait]
impl HostAssignment for FailingAssignment {
    async fn prepare_code(
        &self,
        _: &str,
        token: &str,
        artifact: &crate::artifacts::ArtifactFile,
        mut chunks: code::CodeStream,
    ) -> Result<()> {
        assert!(!token.is_empty());
        assert_eq!(artifact.path, "actors.mjs");
        assert_eq!(
            chunks.next().await.unwrap()?,
            bytes::Bytes::from_static(b"test-code")
        );
        assert!(chunks.next().await.is_none());
        Ok(())
    }

    async fn alive(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn assign(
        &self,
        _: &str,
        _: &str,
        _: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        anyhow::bail!("assignment failed")
    }
}

#[tokio::test]
async fn exited_runtime_releases_capacity_after_repeated_failed_probes() -> Result<()> {
    let actor = proto::Actor {
        metadata: Some(proto::ResourceMetadata {
            atespace: "staging".into(),
            name: "h-exited".into(),
            uid: "exited-uid".into(),
            version: 3,
            ..Default::default()
        }),
        status: Some(proto::ActorStatus {
            state: proto::ActorState::Running.into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let api = Arc::new(Api::new(vec![actor]));
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        assignment: Arc::new(FailingAssignment),
        config: config(),
        issuer: issuer()?,
    };
    let mut failures = HashMap::new();
    provider.reap(&mut failures).await?;
    provider.reap(&mut failures).await?;
    assert!(api.0.lock().unwrap().is_empty());
    provider.reap(&mut failures).await?;
    assert_eq!(*api.0.lock().unwrap(), ["delete"]);
    Ok(())
}

#[tokio::test]
async fn warm_probe_uses_the_actor_router_and_accepts_only_success() -> Result<()> {
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::get,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    let healthy = Arc::new(AtomicBool::new(true));
    let status = healthy.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let route = format!("http://{}/substrate/staging/h-test", listener.local_addr()?);
    let app = Router::new().route(
        "/warmz",
        get(move |headers: HeaderMap| {
            assert_eq!(headers["ate-target-actor"], "staging/h-test");
            let status = if status.load(Ordering::SeqCst) {
                StatusCode::OK
            } else {
                StatusCode::BAD_GATEWAY
            };
            async move { status }
        }),
    );
    let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, app).await
    }));
    let assignment = HttpAssignment(reqwest::Client::new());
    assert!(assignment.alive(&route).await?);
    healthy.store(false, Ordering::SeqCst);
    assert!(!assignment.alive(&route).await?);
    healthy.store(true, Ordering::SeqCst);
    assert!(assignment.alive(&route).await?);
    drop(task);
    Ok(())
}
fn config() -> SubstrateConfig {
    SubstrateConfig {
        code_snapshots: false,
        endpoint: "https://api.ate-system.svc".into(),
        router: "http://router".into(),
        token_file: "/run/substrate/token".into(),
        trust_bundle: "/run/substrate/trust-bundle.pem".into(),
        atespace: "staging".into(),
        worker_labels: std::collections::BTreeMap::from([("workload".into(), "terse".into())]),
        regions: vec!["north-america-west".into()],
        snapshot_location: "gs://snapshots/runtime/".into(),
        sandbox_config: "gvisor-default".into(),
        secrets_namespace: "staging".into(),
        egress_cidrs: vec!["10.108.8.182/32".into()],
    }
}
fn issuer() -> Result<crate::control_plane::ActorJwtIssuer> {
    use base64::Engine;
    let key = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(
        &aws_lc_rs::rand::SystemRandom::new(),
    )?;
    crate::control_plane::ActorJwtIssuer::from_base64_pkcs8(
        &base64::engine::general_purpose::STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(1800),
    )
}
fn request() -> Result<EnsureHostRequest> {
    let manifest = crate::artifacts::ArtifactManifest {
        bucket: "code-bucket".into(),
        files: vec![crate::artifacts::ArtifactFile {
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
        image_ref: "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        working_directory: "/customer".into(),
        actor_entrypoint: Some("/customer/actors.mjs".into()),
        secret_refs: vec!["customer".into()],
        host_idle_timeout_ms: 10000,
    })
}

#[test]
fn python_assignment_uses_its_manifest_entrypoint() -> Result<()> {
    let mut request = request()?;
    let mut manifest =
        crate::artifacts::ArtifactManifest::decode(request.code_snapshot.as_ref().unwrap())?;
    manifest.files[0].path = "actors.pyz".into();
    request.code_snapshot = Some(manifest.encode()?);
    request.actor_entrypoint = Some("actors.pyz".into());
    let environment = assignment::environment(
        &request,
        "http://router/substrate/staging/host",
        HashMap::new(),
    )?;
    assert_eq!(
        environment["DURABLE_ACTORS_ENTRYPOINT"],
        "/customer/actors.pyz"
    );
    Ok(())
}

#[tokio::test]
async fn deployment_prepares_golden_snapshots_before_actors_are_created() -> Result<()> {
    let api = Arc::new(Api::new(Vec::new()));
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        assignment: Arc::new(FailingAssignment),
        config: config(),
        issuer: issuer()?,
    };
    let host = request()?;
    provider
        .prepare_runtime(&RuntimeTemplateRequest {
            code_snapshot: host.code_snapshot,
            image_ref: host.image_ref,
            canonical_region: host.canonical_region,
            resources: vec![
                ResourceLimits {
                    cpu_millis: 500,
                    memory_mib: 512,
                },
                ResourceLimits {
                    cpu_millis: 1500,
                    memory_mib: 2048,
                },
            ],
            jwt_public_keys: host.jwt_public_keys,
            jwt_issuer: host.jwt_issuer,
        })
        .await?;
    assert_eq!(*api.0.lock().unwrap(), ["template", "template"]);
    Ok(())
}

#[tokio::test]
async fn deployment_prepares_code_without_assigning_customer_runtime() -> Result<()> {
    let api = Arc::new(Api::new(Vec::new()));
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        assignment: Arc::new(FailingAssignment),
        config: SubstrateConfig {
            code_snapshots: true,
            ..config()
        },
        issuer: issuer()?,
    };
    provider
        .prepare_runtime(&runtime_template(&request()?))
        .await?;
    assert_eq!(
        *api.0.lock().unwrap(),
        [
            "template",
            "tag",
            "create",
            "resume",
            "suspend",
            "create_tag",
            "delete"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn code_snapshots_are_reused_and_changed_artifacts_get_new_snapshots() -> Result<()> {
    let api = Arc::new(Api::new(Vec::new()));
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        assignment: Arc::new(FailingAssignment),
        config: SubstrateConfig {
            code_snapshots: true,
            ..config()
        },
        issuer: issuer()?,
    };
    let mut host = request()?;
    provider.prepare_runtime(&runtime_template(&host)).await?;
    provider.prepare_runtime(&runtime_template(&host)).await?;
    assert_eq!(api.3.lock().unwrap().len(), 1);
    let actor = provider
        .allocate(&host, &mut ProvisioningTimings::default())
        .await?;
    let first = actor.source_tag.unwrap();
    assert!(first.name.starts_with("code-"));
    assert!(api.2.lock().unwrap().contains_key(&first.name));
    let mut manifest =
        crate::artifacts::ArtifactManifest::decode(host.code_snapshot.as_ref().unwrap())?;
    manifest.files[0].generation += 1;
    host.code_snapshot = Some(manifest.encode()?);
    assert!(
        provider
            .allocate(&host, &mut ProvisioningTimings::default())
            .await
            .is_err()
    );
    provider.prepare_runtime(&runtime_template(&host)).await?;
    let changed = provider
        .allocate(&host, &mut ProvisioningTimings::default())
        .await?;
    assert_ne!(changed.source_tag.unwrap().name, first.name);
    Ok(())
}

#[tokio::test]
async fn failed_code_snapshot_releases_preparation_capacity() -> Result<()> {
    let mut api = Api::new(Vec::new());
    api.4 = true;
    let api = Arc::new(api);
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        assignment: Arc::new(FailingAssignment),
        config: SubstrateConfig {
            code_snapshots: true,
            ..config()
        },
        issuer: issuer()?,
    };
    assert!(
        provider
            .prepare_runtime(&runtime_template(&request()?))
            .await
            .is_err()
    );
    assert_eq!(api.0.lock().unwrap().last(), Some(&"delete"));
    Ok(())
}
