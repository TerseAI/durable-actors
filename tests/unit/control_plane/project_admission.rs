use super::*;
use crate::{
    clock::Clock,
    control_plane::project_limits::{
        ActorAdmission, AdmissionRejected, MemoryReservations, ProjectLimitStore, ProjectLimits,
    },
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[tokio::test]
async fn project_limits_block_http_websocket_and_alarm_provisioning() -> Result<()> {
    for (max_active_actors, code, retry_after) in [
        (1, "actor_quota_exceeded", None),
        (100, "actor_start_rate_limited", Some("1")),
    ] {
        let limits = Arc::new(TestLimits {
            limits: ProjectLimits {
                max_active_actors,
                starts_per_second: 1,
                start_burst: 1,
            },
            unavailable: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let admission = Arc::new(ActorAdmission::new(
            limits.clone(),
            Arc::new(MemoryReservations::new(
                Arc::new(FixedClock),
                Duration::from_secs(86_460),
            )),
        ));
        admission
            .reserve(&actor("admitted"), &HostId::new("host-one"), "one")
            .await?;
        let provider = Arc::new(CountingProvider(AtomicUsize::new(0)));
        let (service, admin) = service(admission, provider.clone()).await?;
        let routes = super::super::super::public_api::router(service.clone(), admin);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let client = reqwest::Client::new();
        assert_http_rejections(&client, &origin, code, retry_after).await?;
        assert_socket_rejection(&client, &origin).await?;
        let alarm = super::super::super::alarms::ClaimedAlarm {
            actor: actor("new"),
            revision: 1,
        };
        let error = super::super::super::invocation::invoke_alarm(&service, &client, &alarm)
            .await
            .unwrap_err();
        assert!(error.is::<AdmissionRejected>());
        assert_eq!(provider.0.load(Ordering::SeqCst), 0);
        assert_eq!(limits.calls.load(Ordering::SeqCst), 5);
        limits.unavailable.store(true, Ordering::SeqCst);
        let response = client
            .post(format!(
                "{origin}/v1/projects/default/actors/Counter/new/find-actor"
            ))
            .bearer_auth("api-key")
            .json(&json!({}))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(provider.0.load(Ordering::SeqCst), 0);
        server.abort();
    }
    Ok(())
}

async fn assert_http_rejections(
    client: &reqwest::Client,
    origin: &str,
    code: &str,
    retry_after: Option<&str>,
) -> Result<()> {
    for (suffix, body) in [
        ("find-actor", json!({})),
        (
            "invoke",
            json!({"requestId":"attempt", "method":"load", "args":[]}),
        ),
    ] {
        let url = format!("{origin}/v1/projects/default/actors/Counter/new/{suffix}");
        assert_eq!(
            client.post(&url).json(&body).send().await?.status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        let response = client
            .post(url)
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .map(|value| value.to_str().unwrap()),
            retry_after
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.json::<Value>().await?["error"]["code"], code);
    }
    Ok(())
}

async fn assert_socket_rejection(client: &reqwest::Client, origin: &str) -> Result<()> {
    let grant: Value = client
        .post(format!(
            "{origin}/v1/projects/default/actors/Counter/new/find-websocket"
        ))
        .bearer_auth("api-key")
        .json(&json!({"metadata":null}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut socket_url = reqwest::Url::parse(grant["websocketUrl"].as_str().unwrap())?;
    let address = reqwest::Url::parse(origin)?;
    socket_url.set_scheme("ws").unwrap();
    socket_url.set_host(address.host_str())?;
    socket_url.set_port(address.port()).unwrap();
    let (mut socket, _) = tokio_tungstenite::connect_async(socket_url.as_str()).await?;
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await?
        .unwrap()?;
    assert!(
        matches!(message, tokio_tungstenite::tungstenite::Message::Close(Some(frame)) if u16::from(frame.code) == 4400)
    );
    Ok(())
}

async fn service(
    admission: Arc<ActorAdmission>,
    provider: Arc<CountingProvider>,
) -> Result<(ControlPlaneService, AdminService)> {
    let issuer = test_issuer()?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            organization_id: None,
            image_policy: Default::default(),
            project_id: "default".into(),
            sandboxes: serde_json::from_value(
                json!({"Counter":{"regions":["north-america-east", "north-america-west"]}}),
            )?,
            source: None,
            code_snapshot: None,
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let verifier = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let provisioner = SandboxHostProvisioner::new(
        provider,
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control.example".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invoke".into(),
            host_idle_timeout_ms: 10_000,
        },
        issuer.clone(),
        None,
    )
    .with_actor_admission(admission.clone());
    let mut service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        verifier,
        registry,
        issuer,
        Arc::new(provisioner),
    );
    service.lifecycle = service.lifecycle.with_observer(admission);
    Ok((with_socket_gateway(service).await?, admin))
}

fn actor(id: &str) -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: id.into(),
    }
}

struct TestLimits {
    limits: ProjectLimits,
    unavailable: AtomicBool,
    calls: AtomicUsize,
}
#[async_trait]
impl ProjectLimitStore for TestLimits {
    async fn get(&self, _: &str) -> Result<ProjectLimits> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ensure!(
            !self.unavailable.load(Ordering::SeqCst),
            "limit storage unavailable"
        );
        Ok(self.limits)
    }
}

struct FixedClock;
impl Clock for FixedClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1000)
    }
}

struct CountingProvider(AtomicUsize);
#[async_trait]
impl SandboxProvider for CountingProvider {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<crate::sandbox::ActorHostHandle> {
        self.0.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("unexpected provisioning")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unused")
    }
}
