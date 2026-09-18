use std::{collections::HashMap, env, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tracing::info;

use crate::{
    bucket::{BucketHostLeases, GcsBucket, GrpcReplicaPeers, RuntimeStorage},
    clock::SystemClock,
    host_leases::HostLeaseStore,
    postgres::PostgresDatabase,
    sandbox::{CommandSandboxProvider, HostSandboxRuntimeConfig},
};

use super::{ActorJwtVerifier, ControlPlaneService};

const DEFAULT_JWT_ISSUER: &str = "durable-object-control-plane";
const DEFAULT_AUTHORITY_AUDIENCE: &str = "durable-object-authority";
const DEFAULT_INVOCATION_AUDIENCE: &str = "durable-object-invoke";
const DEFAULT_JWT_TTL_SECONDS: u64 = 86_400;
const DEFAULT_ACTOR_IDLE_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 300_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub struct ControlPlaneProcessConfig {
    pub bind: SocketAddr,
    pub jwt_signing_key: String,
    pub jwt_key_id: String,
    pub jwt_issuer: String,
    pub authority_audience: String,
    pub invocation_audience: String,
    pub jwt_max_lifetime: Duration,
    pub api_key: String,
    pub storage: ControlPlaneStorageConfig,
    pub sandbox_provider: SandboxProviderConfig,
    pub socket_event_sink: Option<SocketEventSinkConfig>,
    pub region: Option<String>,
}

pub struct ControlPlaneStorageConfig {
    pub postgres_url: String,
    pub bucket: String,
    pub replica_regions: Vec<String>,
}

pub struct SandboxProviderConfig {
    pub provider_name: String,
    pub command: String,
    pub environment: HashMap<String, String>,
    pub runtime: HostSandboxRuntimeConfig,
}

pub struct SocketEventSinkConfig {
    pub url: String,
}

impl ControlPlaneProcessConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }
}

pub async fn serve_control_plane(
    config: ControlPlaneProcessConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let bind = config.bind;
    let routes = control_plane_routes(config).await?;
    info!(bind = %bind, "durable-object control plane is ready");
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .context("bind durable-object control plane")?;
    serve_routes(listener, routes, shutdown).await
}

async fn serve_routes(
    listener: tokio::net::TcpListener,
    routes: tonic::service::Routes,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    axum::serve(listener, routes.into_axum_router())
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve durable-object control plane")
}

async fn control_plane_routes(config: ControlPlaneProcessConfig) -> Result<tonic::service::Routes> {
    let issuer = super::ActorJwtIssuer::from_base64_pkcs8(
        &config.jwt_signing_key,
        config.jwt_key_id,
        config.jwt_issuer.clone(),
        config.authority_audience.clone(),
        config.invocation_audience,
        config.jwt_max_lifetime,
    )?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        config.jwt_issuer,
        config.authority_audience,
        super::ActorTokenPurpose::ControlPlane,
        config.jwt_max_lifetime,
    )?;
    let database = PostgresDatabase::lazy(&config.storage.postgres_url)?;
    let authority = Arc::new(GcsBucket::new(&config.storage.bucket).await?);
    let leases: Arc<dyn HostLeaseStore> = Arc::new(BucketHostLeases::new(
        authority.clone(),
        Arc::new(SystemClock),
    ));
    let registry = Arc::new(super::PostgresAdminRegistry::from_database(database));
    let (fleet, access) = super::replication::fleet(
        registry.clone(),
        &config.sandbox_provider,
        &config.jwt_signing_key,
        config.storage.replica_regions,
    )?;
    let runtime_access = Arc::new(crate::bucket::access::RuntimeAccess::new(
        crate::bucket::access::BucketLocation::Gcs {
            bucket: config.storage.bucket.clone(),
        },
        fleet.clone(),
        access.clone(),
    )?);
    let storage = Arc::new(RuntimeStorage::new(
        authority,
        leases.clone(),
        fleet,
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access,
        config.sandbox_provider.runtime.control_plane_url.clone(),
    )?);
    let placements = storage.clone();
    let provisioner = sandbox_provisioner(
        config.sandbox_provider,
        &issuer,
        &leases,
        runtime_access.clone(),
    )?;
    let socket_events = config
        .socket_event_sink
        .map(|sink| {
            super::event_sink::HttpSocketMessageEventSink::new(sink.url, config.api_key.clone())
        })
        .transpose()?
        .map(|sink| Arc::new(sink) as Arc<dyn super::event_sink::SocketMessageEventSink>);
    let mut service = ControlPlaneService::new(
        leases,
        placements.clone(),
        auth,
        registry.clone(),
        issuer.clone(),
        provisioner,
    )
    .with_runtime_access(runtime_access)
    .with_socket_event_sink(socket_events);
    service.region = config.region;
    let admin = super::admin::AdminService::new(config.api_key, registry, issuer)?;
    let inspector = super::inspection::ActorInspector::new(
        placements,
        storage.clone(),
        storage.clone(),
        service.changes.clone(),
    );
    let public_api = super::public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::router(inspector, admin))
        .merge(storage.router());
    let internal_api = service.into_internal_service();
    Ok(tonic::service::Routes::from(public_api).add_service(internal_api))
}

fn sandbox_provisioner(
    config: SandboxProviderConfig,
    issuer: &super::ActorJwtIssuer,
    leases: &Arc<dyn HostLeaseStore>,
    access: Arc<crate::bucket::access::RuntimeAccess>,
) -> Result<Arc<dyn super::service::HostProvisioner>> {
    let provider = Arc::new(CommandSandboxProvider::new(
        config.provider_name,
        config.command,
        config.environment,
    )?);
    Ok(Arc::new(
        super::service::SandboxHostProvisioner::new(
            provider,
            config.runtime,
            issuer.clone(),
            leases.clone(),
        )
        .with_runtime_access(access),
    ))
}

impl ControlPlaneProcessConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = get("DURABLE_OBJECT_CONTROL_PLANE_BIND")
            .unwrap_or_else(|| "127.0.0.1:7100".into())
            .parse()
            .context("DURABLE_OBJECT_CONTROL_PLANE_BIND must be a socket address")?;
        let jwt_signing_key = required(&mut get, "DURABLE_OBJECT_JWT_SIGNING_KEY")?;
        let jwt_key_id = get("DURABLE_OBJECT_JWT_KEY_ID").unwrap_or_else(|| "primary".into());
        let jwt_issuer =
            get("DURABLE_OBJECT_JWT_ISSUER").unwrap_or_else(|| DEFAULT_JWT_ISSUER.into());
        let authority_audience = get("DURABLE_OBJECT_AUTHORITY_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_AUTHORITY_AUDIENCE.into());
        let invocation_audience = get("DURABLE_OBJECT_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_INVOCATION_AUDIENCE.into());
        let jwt_max_lifetime = Duration::from_secs(
            get("DURABLE_OBJECT_JWT_MAX_TTL_SECONDS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_OBJECT_JWT_MAX_TTL_SECONDS must be an integer")?
                .unwrap_or(DEFAULT_JWT_TTL_SECONDS),
        );
        ensure!(
            !jwt_max_lifetime.is_zero(),
            "DURABLE_OBJECT_JWT_MAX_TTL_SECONDS must be positive"
        );
        let api_key = required(&mut get, "DURABLE_OBJECT_API_KEY")?;
        ensure!(
            api_key.trim() == api_key,
            "DURABLE_OBJECT_API_KEY has surrounding whitespace"
        );
        let bucket = required(&mut get, "DURABLE_OBJECT_BUCKET")?;
        crate::storage::validate_bucket(&bucket)?;
        let replica_regions = crate::replication::replica_regions(&mut get)?;
        let region = get("DURABLE_OBJECT_REGION");
        if let Some(region) = &region {
            crate::placement::validate_region(region)?;
        }
        let storage = ControlPlaneStorageConfig {
            replica_regions,
            postgres_url: required(&mut get, "DURABLE_OBJECT_POSTGRES_URL")?,
            bucket,
        };
        let sandbox_provider =
            sandbox_provider_config(&mut get, &jwt_issuer, &invocation_audience)?;
        let socket_event_sink = socket_event_sink_config(&mut get)?;
        Ok(Self {
            bind,
            jwt_signing_key,
            jwt_key_id,
            jwt_issuer,
            authority_audience,
            invocation_audience,
            jwt_max_lifetime,
            api_key,
            storage,
            sandbox_provider,
            socket_event_sink,
            region,
        })
    }
}

fn socket_event_sink_config(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<SocketEventSinkConfig>> {
    get("DURABLE_OBJECT_SOCKET_EVENT_URL")
        .map(|url| {
            Ok(SocketEventSinkConfig {
                url: validated_http_url(&url, "DURABLE_OBJECT_SOCKET_EVENT_URL")?,
            })
        })
        .transpose()
}

fn sandbox_provider_config(
    get: &mut impl FnMut(&str) -> Option<String>,
    jwt_issuer: &str,
    invocation_audience: &str,
) -> Result<SandboxProviderConfig> {
    let provider_name = required(get, "DURABLE_OBJECT_SANDBOX_PROVIDER")?;
    ensure!(
        provider_name == "modal",
        "unsupported sandbox provider {provider_name:?}"
    );
    let mut environment = HashMap::from([
        (
            "MODAL_TOKEN_ID".into(),
            provider_credential(get, "MODAL_TOKEN_ID")?,
        ),
        (
            "MODAL_TOKEN_SECRET".into(),
            provider_credential(get, "MODAL_TOKEN_SECRET")?,
        ),
    ]);
    if let Some(value) = get("DURABLE_OBJECT_MODAL_MUTABLE_NETWORK") {
        let enabled: bool = value
            .parse()
            .context("DURABLE_OBJECT_MODAL_MUTABLE_NETWORK must be true or false")?;
        environment.insert(
            "DURABLE_OBJECT_MODAL_MUTABLE_NETWORK".into(),
            enabled.to_string(),
        );
    }
    let control_plane_url = validated_http_url(
        &required(get, "DURABLE_OBJECT_CONTROL_PLANE_URL")?,
        "DURABLE_OBJECT_CONTROL_PLANE_URL",
    )?;
    Ok(SandboxProviderConfig {
        provider_name,
        command: get("DURABLE_OBJECT_SANDBOX_COMMAND")
            .unwrap_or_else(|| "little-actors-modal-go".into()),
        environment,
        runtime: HostSandboxRuntimeConfig {
            control_plane_url,
            jwt_issuer: jwt_issuer.into(),
            invocation_jwt_audience: invocation_audience.into(),
            actor_idle_timeout_seconds: actor_idle_timeout_seconds(get)?,
            host_idle_timeout_ms: idle_timeout(
                get,
                "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS",
                DEFAULT_HOST_IDLE_TIMEOUT_MS,
                MAX_IDLE_TIMEOUT_MS,
            )?,
        },
    })
}

fn provider_credential(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = required(get, name)?;
    ensure!(value.trim() == value, "{name} has surrounding whitespace");
    Ok(value)
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn validated_http_url(value: &str, name: &str) -> Result<String> {
    let url = reqwest::Url::parse(value).with_context(|| format!("{name} must be a URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "{name} must be HTTP or HTTPS"
    );
    Ok(url.to_string())
}

pub(super) fn actor_idle_timeout_seconds(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<u64> {
    idle_timeout(
        get,
        "DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_SECONDS",
        DEFAULT_ACTOR_IDLE_TIMEOUT_SECONDS,
        86_400,
    )
}

fn idle_timeout(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
    maximum: u64,
) -> Result<u64> {
    let value = get(name)
        .map(|value| value.parse())
        .transpose()
        .with_context(|| format!("{name} must be an integer"))?
        .unwrap_or(default);
    ensure!(
        (1..=maximum).contains(&value),
        "{name} must be an integer between 1 and {maximum}"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use axum::{Router, extract::WebSocketUpgrade, response::Response, routing::get};
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::oneshot;
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    use super::*;

    #[test]
    fn actor_idle_timeout_uses_bounded_seconds() -> Result<()> {
        assert_eq!(actor_idle_timeout_seconds(&mut |_| None)?, 60);
        for value in ["1", "10", "86400"] {
            let parsed = actor_idle_timeout_seconds(&mut |name| {
                assert_eq!(name, "DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_SECONDS");
                Some(value.into())
            })?;
            assert_eq!(parsed, value.parse::<u64>()?);
        }
        for value in ["0", "-1", "1.5", "86401", "not-a-number"] {
            assert!(actor_idle_timeout_seconds(&mut |_| Some(value.into())).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn server_carries_websocket_upgrades() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let routes =
            tonic::service::Routes::from(Router::new().route("/socket", get(echo_websocket)));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_routes(listener, routes, async {
            let _ = shutdown_rx.await;
        }));

        let (mut socket, _) = connect_async(format!("ws://{address}/socket")).await?;
        socket.send(Message::Text("hello".into())).await?;
        assert_eq!(
            socket.next().await.transpose()?,
            Some(Message::Text("hello".into()))
        );
        socket.close(None).await?;
        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn parses_the_minimal_storage_configuration() -> Result<()> {
        let values = HashMap::from([
            ("DURABLE_OBJECT_JWT_SIGNING_KEY", "c2lnbmluZw=="),
            ("DURABLE_OBJECT_API_KEY", "api-key"),
            ("DURABLE_OBJECT_BUCKET", "actor-state-test"),
            ("DURABLE_OBJECT_SANDBOX_PROVIDER", "modal"),
            (
                "DURABLE_OBJECT_CONTROL_PLANE_URL",
                "https://objects.example.com",
            ),
            ("MODAL_TOKEN_ID", "modal-token-id"),
            ("MODAL_TOKEN_SECRET", "modal-token-secret"),
            (
                "DURABLE_OBJECT_POSTGRES_URL",
                "postgresql://localhost/actors",
            ),
        ]);
        let config = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        })?;
        assert_eq!(config.storage.bucket, "actor-state-test");
        assert_eq!(config.jwt_max_lifetime, Duration::from_secs(86_400));
        Ok(())
    }

    #[test]
    fn mutable_modal_network_requires_explicit_boolean_configuration() -> Result<()> {
        let mut values = HashMap::from([
            ("DURABLE_OBJECT_SANDBOX_PROVIDER", "modal"),
            (
                "DURABLE_OBJECT_CONTROL_PLANE_URL",
                "https://control.example",
            ),
            ("MODAL_TOKEN_ID", "id"),
            ("MODAL_TOKEN_SECRET", "secret"),
        ]);
        let configure = |values: &HashMap<&str, &str>| {
            sandbox_provider_config(
                &mut |name| values.get(name).map(|v| (*v).into()),
                "issuer",
                "audience",
            )
        };
        assert!(
            !configure(&values)?
                .environment
                .contains_key("DURABLE_OBJECT_MODAL_MUTABLE_NETWORK")
        );
        values.insert("DURABLE_OBJECT_MODAL_MUTABLE_NETWORK", "true");
        assert_eq!(
            configure(&values)?.environment["DURABLE_OBJECT_MODAL_MUTABLE_NETWORK"],
            "true"
        );
        values.insert("DURABLE_OBJECT_MODAL_MUTABLE_NETWORK", "yes");
        assert!(configure(&values).is_err());
        Ok(())
    }

    #[test]
    fn configures_socket_events_without_a_separate_key() -> Result<()> {
        let mut complete = HashMap::from([(
            "DURABLE_OBJECT_SOCKET_EVENT_URL",
            "https://api.example.com/events",
        )]);
        let sink =
            socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
                .context("socket event sink was not configured")?;
        assert_eq!(sink.url, "https://api.example.com/events");
        complete.remove("DURABLE_OBJECT_SOCKET_EVENT_URL");
        assert!(
            socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
                .is_none()
        );
        Ok(())
    }

    async fn echo_websocket(upgrade: WebSocketUpgrade) -> Response {
        upgrade.on_upgrade(async |mut socket| {
            if let Some(Ok(message)) = socket.recv().await {
                let _ = socket.send(message).await;
            }
        })
    }
}
