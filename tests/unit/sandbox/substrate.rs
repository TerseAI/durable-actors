use super::*;
use std::sync::Mutex;

#[tokio::test]
async fn deployment_prepares_customer_code_for_every_resource_shape() -> Result<()> {
    let api = Arc::new(Api::default());
    let provider = SubstrateProvider {
        api: api.clone(),
        code: Arc::new(TestCodeSource),
        bootstrap: Arc::new(TestBootstrap::new()?),
        assignment: Arc::new(ReadyAssignment),
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
    let prepared = api.1.lock().unwrap();
    assert_eq!(prepared.len(), 2);
    assert_ne!(prepared[0].actor_template, prepared[1].actor_template);
    assert_eq!(api.0.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn ensure_host_restores_and_assigns_a_prepared_actor() -> Result<()> {
    let provider = SubstrateProvider {
        api: Arc::new(Api::prepared()?),
        code: Arc::new(TestCodeSource),
        bootstrap: Arc::new(TestBootstrap::new()?),
        assignment: Arc::new(ReadyAssignment),
        config: config(),
        issuer: issuer()?,
    };
    let handle = provider.ensure_host(&request()?).await?;
    assert_eq!(handle.owner_epoch, 1);
    Ok(())
}

#[derive(Default)]
struct Api(Mutex<HashMap<String, proto::Tag>>, Mutex<Vec<proto::Actor>>);
impl Api {
    fn prepared() -> Result<Self> {
        let api = Self::default();
        let request = request()?;
        let mut template =
            template::build(&config(), &runtime_template(&request), &request.resources)?;
        let meta = template.metadata.as_mut().unwrap();
        meta.uid = format!("uid-{}", meta.name);
        let uid = meta.uid.clone();
        let target = code::code_tag(&template, request.code_snapshot.as_ref().unwrap())?;
        api.0.lock().unwrap().insert(
            target.name,
            proto::Tag {
                status: Some(proto::TagStatus {
                    snapshot: Some(Default::default()),
                    actor_template_uid: uid,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        Ok(api)
    }
}
struct TestCodeSource;
#[async_trait]
impl code::CodeSource for TestCodeSource {
    async fn open(&self, _: &str, _: &crate::bundle::BundleFile) -> Result<code::CodeStream> {
        Ok(stream::once(async { Ok(bytes::Bytes::from_static(b"test-code")) }).boxed())
    }
}
#[async_trait]
impl SubstrateApi for Api {
    async fn workers(&self) -> Result<Vec<proto::Worker>> {
        Ok(Vec::new())
    }
    async fn template(&self, mut template: proto::ActorTemplate) -> Result<proto::ActorTemplate> {
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
        Ok(self.0.lock().unwrap().get(&target.name).cloned())
    }
    async fn suspend(&self, _: proto::ObjectRef) -> Result<()> {
        Ok(())
    }
    async fn create_tag(&self, mut tag: proto::Tag) -> Result<()> {
        let source = self
            .1
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
        self.0
            .lock()
            .unwrap()
            .insert(tag.metadata.as_ref().unwrap().name.clone(), tag);
        Ok(())
    }
    async fn create(&self, mut actor: proto::Actor) -> Result<proto::Actor> {
        actor.metadata.as_mut().unwrap().uid = "restored-uid".into();
        self.1.lock().unwrap().push(actor.clone());
        Ok(actor)
    }
    async fn egress(&self, _: proto::ObjectRef, _: Vec<proto::EgressRule>) -> Result<()> {
        Ok(())
    }
    async fn resume(&self, _: proto::ObjectRef) -> Result<()> {
        Ok(())
    }
    async fn delete(&self, _: proto::Actor, _: Option<i64>) -> Result<()> {
        Ok(())
    }
    async fn actors(&self, _: &str) -> Result<Vec<proto::Actor>> {
        Ok(Vec::new())
    }
    async fn secrets(&self, _: &[String]) -> Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }
}
fn config() -> SubstrateConfig {
    SubstrateConfig {
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

struct TestBootstrap {
    _directory: tempfile::TempDir,
    storage: crate::bucket::RuntimeStorage,
}
impl TestBootstrap {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let storage = crate::bucket::RuntimeStorage::new(
            Arc::new(crate::bucket::FileBucket::new(directory.path().into())?),
            Arc::new(SystemClock),
        )?;
        Ok(Self {
            _directory: directory,
            storage,
        })
    }
}
#[async_trait]
impl HostBootstrap for TestBootstrap {
    async fn credentials(&self, _: &EnsureHostRequest) -> Result<String> {
        Ok("{}".into())
    }
    async fn claim(
        &self,
        request: &EnsureHostRequest,
        route: &str,
    ) -> Result<crate::bucket::ActivationHandoff> {
        self.storage
            .prepare_activation(
                request.actor.as_ref().unwrap(),
                &crate::host_leases::HostLeaseRequest {
                    id: request.host_id.clone(),
                    session_id: request.session_id.clone(),
                    route: route.into(),
                    duration_ms: 30_000,
                },
                &request.canonical_region,
                true,
                None,
            )
            .await
    }
    async fn release(&self, request: &EnsureHostRequest) -> Result<()> {
        self.storage
            .release_activation(
                request.actor.as_ref().unwrap(),
                &request.host_id,
                &request.session_id,
            )
            .await
    }
}

struct ReadyAssignment;
#[async_trait]
impl HostAssignment for ReadyAssignment {
    async fn alive(&self, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn prepare_code(
        &self,
        _: &str,
        _: &str,
        _: &crate::bundle::BundleFile,
        _: code::CodeStream,
    ) -> Result<()> {
        Ok(())
    }
    async fn assign(
        &self,
        route: &str,
        _: &str,
        env: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        assert_eq!(env["DURABLE_ACTORS_RUNTIME_CONFIG"], "{}");
        let handoff: serde_json::Value =
            serde_json::from_str(&env["DURABLE_ACTORS_ACTIVATION_HANDOFF"])?;
        Ok(ActorHostHandle {
            host_id: HostId::new(env["DURABLE_ACTORS_HOST_ID"].clone()),
            route: route.into(),
            canonical_region: env["DURABLE_ACTORS_REGION"].clone(),
            owner_epoch: handoff["record"]["epoch"].as_u64().unwrap(),
            lease: Some(serde_json::from_value(handoff["record"]["lease"].clone())?),
            provisioning: None,
        })
    }
}
