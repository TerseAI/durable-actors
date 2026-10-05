use std::{env, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tracing::{info, warn};

use crate::{
    bucket::{GcsBucket, RuntimeStorageReader},
    postgres::PostgresDatabase,
    request_traces::{TraceStore, persistence::postgres::PostgresTracePersistence},
    sandbox::{
        HostSandboxRuntimeConfig,
        substrate::{SubstrateConfig, SubstrateProvider},
    },
};

use super::{ActorJwtVerifier, ControlPlaneService};

const DEFAULT_JWT_ISSUER: &str = "durable-actors-control-plane";
const DEFAULT_AUTHORITY_AUDIENCE: &str = "durable-actors-authority";
const DEFAULT_INVOCATION_AUDIENCE: &str = "durable-actors-invoke";
const DEFAULT_JWT_TTL_SECONDS: u64 = 86_400;

pub struct ControlPlaneProcessConfig {
    pub bind: SocketAddr,
    pub gateway_route: String,
    pub gateway_accept_connections: bool,
    pub max_socket_connections: usize,
    pub jwt_signing_key: String,
    pub jwt_key_id: String,
    pub jwt_issuer: String,
    pub authority_audience: String,
    pub invocation_audience: String,
    pub jwt_max_lifetime: Duration,
    pub api_key: Option<String>,
    pub storage: ControlPlaneStorageConfig,
    pub sandbox_provider: SandboxProviderConfig,
    pub socket_event_sink: Option<SocketEventSinkConfig>,
    pub region: Option<String>,
}

pub struct ControlPlaneStorageConfig {
    pub postgres_url: String,
    pub token_issuer: String,
    pub trace_retention: Duration,
    pub bucket: String,
    pub persistence: crate::bucket::PersistenceConfig,
    pub artifact_bucket: String,
}

pub struct SandboxProviderConfig {
    pub metrics_bind: SocketAddr,
    pub runtime_image: String,
    pub(crate) substrate: SubstrateConfig,
    pub public_origin: String,
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
    warn_if_authentication_disabled(bind, config.api_key.as_deref());
    let stop = tokio_util::sync::CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let routes = control_plane_routes(config, stop).await?;
    info!(bind = %bind, "durable-actors control plane is ready");
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .context("bind durable-actors control plane")?;
    serve_routes(listener, routes, shutdown).await
}

fn warn_if_authentication_disabled(bind: SocketAddr, secret: Option<&str>) {
    if secret.is_none() && !bind.ip().is_loopback() {
        warn!(
            %bind,
            "Authentication is disabled. Anyone who can reach this server can access its API. Set DURABLE_ACTORS_SECRET to enable authentication."
        );
    }
}

async fn serve_routes(
    listener: tokio::net::TcpListener,
    routes: tonic::service::Routes,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    axum::serve(listener, routes.into_axum_router())
        .with_graceful_shutdown(shutdown)
        .await
        .context("serve durable-actors control plane")
}

async fn control_plane_routes(
    config: ControlPlaneProcessConfig,
    stop: tokio_util::sync::CancellationToken,
) -> Result<tonic::service::Routes> {
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
    let trace_persistence = Arc::new(PostgresTracePersistence::new(
        database.clone(),
        config.storage.trace_retention,
    ));
    let changes = crate::postgres::notifications::ChangeFeed::postgres(
        database.clone(),
        &config.storage.postgres_url,
        stop.clone(),
    )
    .await?;
    let mut traces = TraceStore::open(trace_persistence.clone()).await?;
    traces.changes = changes.clone();
    trace_persistence.start_retention(stop.clone());
    let authority = Arc::new(GcsBucket::new(&config.storage.bucket).await?);
    let registry = Arc::new(super::PostgresAdminRegistry::from_database(
        database.clone(),
    ));
    let snapshots = Arc::new(crate::bucket::RapidSnapshots::gcs(
        &config.storage.persistence,
        authority.clients(),
        stop.clone(),
        None,
    )?);
    crate::bucket::RapidSnapshots::validate_gcs(&config.storage.persistence, authority.clients())
        .await?;
    let storage = Arc::new(
        RuntimeStorageReader::new(
            authority,
            Arc::new(crate::clock::SystemClock),
            Arc::new(crate::litestream::EmbeddedRestore),
        )?
        .with_persistence(config.storage.persistence, snapshots)?,
    );
    let runtime_access = Arc::new(
        crate::bucket::access::RuntimeAccess::new(
            crate::bucket::access::BucketLocation::Gcs {
                artifact_bucket: config.storage.artifact_bucket.clone(),
                bucket: config.storage.bucket.clone(),
            },
            storage.persistence.clone(),
        )?
        .with_token_cache(
            database.clone(),
            config.storage.token_issuer.clone(),
            stop.clone(),
        ),
    );
    let placements = storage.clone();
    let socket_gateway = super::socket_gateway::SocketGateway::start(
        config.gateway_route.clone(),
        Arc::new(super::socket_directory::PostgresSocketDirectory::new(
            database.clone(),
        )),
        config.max_socket_connections,
        config.gateway_accept_connections,
        stop.child_token(),
    )
    .await?;
    let gateway = super::gateway::Gateway::new(
        &issuer,
        config.sandbox_provider.public_origin.clone(),
        socket_gateway,
    )?;
    let provisioner = sandbox_provisioner(
        config.sandbox_provider,
        &issuer,
        runtime_access.clone(),
        storage.clone(),
        stop,
    )
    .await?;
    let socket_events = config
        .socket_event_sink
        .map(|sink| {
            super::event_sink::HttpSocketMessageEventSink::new(sink.url, config.api_key.clone())
        })
        .transpose()?
        .map(|sink| Arc::new(sink) as Arc<dyn super::event_sink::SocketMessageEventSink>);
    let mut service = ControlPlaneService::new(
        placements.clone(),
        auth,
        registry.clone(),
        issuer.clone(),
        provisioner,
    )
    .with_runtime_access(runtime_access)
    .with_traces(traces)
    .with_socket_event_sink(socket_events);
    service.changes = changes;
    service.gateway = Some(gateway);
    service.region = config.region;
    let inventory = Arc::new(super::socket_inventory::GatewayInventoryReader::new(
        storage.clone(),
        service.gateway.as_ref().unwrap().connections.clone(),
        config.api_key.clone(),
    ));
    let admin = super::admin::AdminService::new(config.api_key, registry, issuer)?;
    let inspector =
        super::inspection::ActorInspector::new(inventory, storage.clone(), service.changes.clone())
            .with_traces(service.traces.clone());
    let public_api = super::public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::router(inspector, admin));
    let internal_api = service.into_internal_service();
    Ok(tonic::service::Routes::from(public_api).add_service(internal_api))
}

async fn sandbox_provisioner(
    config: SandboxProviderConfig,
    issuer: &super::ActorJwtIssuer,
    access: Arc<crate::bucket::access::RuntimeAccess>,
    storage: Arc<RuntimeStorageReader>,
    stop: tokio_util::sync::CancellationToken,
) -> Result<Arc<dyn super::service::HostProvisioner>> {
    let provider = Arc::new(
        SubstrateProvider::new(
            config.substrate,
            issuer.clone(),
            &config.runtime.control_plane_url,
            Arc::new(super::bootstrap::RuntimeBootstrap::new(
                access.clone(),
                storage,
            )),
        )
        .await?,
    );
    provider
        .start_metrics(config.metrics_bind, stop.clone())
        .await?;
    provider.start(stop);
    Ok(Arc::new(
        super::service::SandboxHostProvisioner::new(
            provider,
            config.runtime,
            issuer.clone(),
            Some(config.runtime_image),
        )
        .with_runtime_access(access),
    ))
}

impl ControlPlaneProcessConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind = get("DURABLE_ACTORS_CONTROL_PLANE_BIND")
            .unwrap_or_else(|| "127.0.0.1:7100".into())
            .parse()
            .context("DURABLE_ACTORS_CONTROL_PLANE_BIND must be a socket address")?;
        let jwt_signing_key = required(&mut get, "DURABLE_ACTORS_JWT_SIGNING_KEY")?;
        let jwt_key_id = get("DURABLE_ACTORS_JWT_KEY_ID").unwrap_or_else(|| "primary".into());
        let jwt_issuer =
            get("DURABLE_ACTORS_JWT_ISSUER").unwrap_or_else(|| DEFAULT_JWT_ISSUER.into());
        let authority_audience = get("DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_AUTHORITY_AUDIENCE.into());
        let invocation_audience = get("DURABLE_ACTORS_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| DEFAULT_INVOCATION_AUDIENCE.into());
        let jwt_max_lifetime = Duration::from_secs(
            get("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_ACTORS_JWT_MAX_TTL_SECONDS must be an integer")?
                .unwrap_or(DEFAULT_JWT_TTL_SECONDS),
        );
        ensure!(
            !jwt_max_lifetime.is_zero(),
            "DURABLE_ACTORS_JWT_MAX_TTL_SECONDS must be positive"
        );
        let api_key = get("DURABLE_ACTORS_SECRET");
        if let Some(api_key) = &api_key {
            ensure!(
                !api_key.is_empty(),
                "DURABLE_ACTORS_SECRET must not be empty"
            );
            ensure!(
                api_key.trim() == api_key,
                "DURABLE_ACTORS_SECRET has surrounding whitespace"
            );
        }
        let bucket = required(&mut get, "DURABLE_ACTORS_BUCKET")?;
        crate::storage::validate_bucket(&bucket)?;
        let artifact_bucket = required(&mut get, "DURABLE_ACTORS_ARTIFACT_BUCKET")?;
        crate::storage::validate_bucket(&artifact_bucket)?;
        let archive_bucket = required(&mut get, "DURABLE_ACTORS_ARCHIVE_BUCKET")?;
        let buckets = serde_json::from_str(&required(&mut get, "DURABLE_ACTORS_RAPID_BUCKETS")?)?;
        let persistence = crate::bucket::PersistenceConfig::Rapid {
            archive_bucket,
            buckets,
            archive_batch: crate::bucket::ArchiveBatchConfig {
                bytes: get("DURABLE_ACTORS_ARCHIVE_BATCH_BYTES")
                    .map(|value| value.parse())
                    .transpose()
                    .context("DURABLE_ACTORS_ARCHIVE_BATCH_BYTES must be an integer")?
                    .unwrap_or(16 * 1024 * 1024),
                interval_ms: get("DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS")
                    .map(|value| value.parse())
                    .transpose()
                    .context("DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS must be an integer")?
                    .unwrap_or(10_000),
            },
        };
        persistence.validate()?;
        let region = get("DURABLE_ACTORS_REGION");
        if let Some(region) = &region {
            crate::placement::validate_region(region)?;
        }
        let storage = ControlPlaneStorageConfig {
            persistence,
            artifact_bucket,
            token_issuer: required(&mut get, "DURABLE_ACTORS_GOOGLE_SERVICE_ACCOUNT")?,
            postgres_url: required(&mut get, "DURABLE_ACTORS_POSTGRES_URL")?,
            trace_retention: trace_retention(&mut get)?,
            bucket,
        };
        let sandbox_provider =
            sandbox_provider_config(&mut get, &jwt_issuer, &invocation_audience)?;
        let region = region.or_else(|| sandbox_provider.substrate.regions.first().cloned());
        ensure!(
            region
                .as_ref()
                .is_some_and(|region| sandbox_provider.substrate.regions.contains(region)),
            "default region has no configured Substrate workers"
        );
        let socket_event_sink = socket_event_sink_config(&mut get)?;
        let gateway_route = validated_http_url(
            &required(&mut get, "DURABLE_ACTORS_GATEWAY_ROUTE")?,
            "DURABLE_ACTORS_GATEWAY_ROUTE",
        )?;
        Ok(Self {
            bind,
            gateway_route,
            max_socket_connections: crate::sockets::max_connections(&mut get)?,
            gateway_accept_connections: get("DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS")
                .map(|value| value.parse())
                .transpose()
                .context("DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS must be boolean")?
                .unwrap_or(true),
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

fn trace_retention(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Duration> {
    let days: u64 = get("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS")
        .map(|value| value.parse())
        .transpose()
        .context("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS must be an integer")?
        .unwrap_or(30);
    ensure!(
        (1..=3650).contains(&days),
        "DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS must be between 1 and 3650"
    );
    Ok(Duration::from_secs(days * 86400))
}

fn socket_event_sink_config(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<SocketEventSinkConfig>> {
    get("DURABLE_ACTORS_SOCKET_EVENT_URL")
        .map(|url| {
            Ok(SocketEventSinkConfig {
                url: validated_http_url(&url, "DURABLE_ACTORS_SOCKET_EVENT_URL")?,
            })
        })
        .transpose()
}

fn sandbox_provider_config(
    get: &mut impl FnMut(&str) -> Option<String>,
    jwt_issuer: &str,
    invocation_audience: &str,
) -> Result<SandboxProviderConfig> {
    let public_origin = validated_http_url(
        &required(get, "DURABLE_ACTORS_PUBLIC_URL")?,
        "DURABLE_ACTORS_PUBLIC_URL",
    )?;
    let public_url = reqwest::Url::parse(&public_origin)?;
    ensure!(
        public_url.scheme() == "https"
            && public_url.path() == "/"
            && public_url.query().is_none()
            && public_url.fragment().is_none()
            && public_url.username().is_empty()
            && public_url.password().is_none(),
        "public gateway must be an HTTPS origin"
    );
    let control_plane_url = validated_http_url(
        &required(get, "DURABLE_ACTORS_CONTROL_PLANE_URL")?,
        "DURABLE_ACTORS_CONTROL_PLANE_URL",
    )?;
    let regions: Vec<String> =
        serde_json::from_str(&required(get, "DURABLE_ACTORS_SUBSTRATE_REGIONS")?)?;
    ensure!(
        !regions.is_empty(),
        "at least one Substrate region is required"
    );
    for region in &regions {
        crate::placement::validate_region(region)?;
    }
    let atespace = required(get, "DURABLE_ACTORS_SUBSTRATE_ATESPACE")?;
    ensure!(
        terse_substrate::valid_resource_name(&atespace),
        "invalid Substrate atespace"
    );
    let endpoint = validated_http_url(
        &required(get, "DURABLE_ACTORS_SUBSTRATE_ENDPOINT")?,
        "Substrate endpoint",
    )?;
    ensure!(
        endpoint.starts_with("https://"),
        "Substrate API requires TLS"
    );
    let router = validated_http_url(
        &required(get, "DURABLE_ACTORS_SUBSTRATE_ROUTER")?,
        "Substrate router",
    )?;
    super::gateway::backend_origin(&router)?;
    let snapshot_location = required(get, "DURABLE_ACTORS_SUBSTRATE_SNAPSHOTS")?;
    ensure!(
        snapshot_location.starts_with("gs://") && snapshot_location.ends_with('/'),
        "Substrate snapshots require a GCS prefix ending in /"
    );
    Ok(SandboxProviderConfig {
        metrics_bind: get("DURABLE_ACTORS_METRICS_BIND")
            .unwrap_or_else(|| "127.0.0.1:9090".into())
            .parse()
            .context("DURABLE_ACTORS_METRICS_BIND must be a socket address")?,
        runtime_image: {
            let image = required(get, "DURABLE_ACTORS_RUNTIME_IMAGE")?;
            crate::sandbox::substrate::validate_image(&image)?;
            image
        },
        public_origin,
        substrate: SubstrateConfig {
            endpoint,
            router,
            atespace,
            regions,
            snapshot_location,
            token_file: required(get, "DURABLE_ACTORS_SUBSTRATE_TOKEN_FILE")?,
            trust_bundle: required(get, "DURABLE_ACTORS_SUBSTRATE_TRUST_BUNDLE")?,
            worker_labels: serde_json::from_str(&required(
                get,
                "DURABLE_ACTORS_SUBSTRATE_WORKER_LABELS",
            )?)?,
            sandbox_config: required(get, "DURABLE_ACTORS_SUBSTRATE_SANDBOX_CONFIG")?,
            secrets_namespace: required(get, "DURABLE_ACTORS_SECRETS_NAMESPACE")?,
            egress_cidrs: serde_json::from_str(&required(
                get,
                "DURABLE_ACTORS_SUBSTRATE_EGRESS_CIDRS",
            )?)?,
        },
        runtime: HostSandboxRuntimeConfig {
            control_plane_url,
            jwt_issuer: jwt_issuer.into(),
            invocation_jwt_audience: invocation_audience.into(),
            host_idle_timeout_ms: crate::host::host_idle_timeout_ms(get)?,
        },
    })
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

#[cfg(test)]
#[path = "../../tests/unit/control_plane/process.rs"]
mod tests;
