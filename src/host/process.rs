use std::{
    env,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tokio::{net::TcpListener, process::Command};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    actor::{ActorExecutorConnection, ActorExecutorListener},
    clock::SystemClock,
    control_plane::{ActorJwtVerifier, ActorTokenPurpose, ControlPlaneClient},
    grpc::ActorHostGrpcService,
    host_leases::MAX_HOST_LEASE_DURATION_MS,
    state_transport::GrpcStateTransport,
};

use super::{ActorHost, HostEndpoint, HostLeaseMaintainer, LeaseRenewalTask};

const HOST_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HOST_ACTOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 300_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub struct ActorHostConfig {
    runtime_config: crate::bucket::access::HostStorageConfig,
    pub control_plane_url: String,
    pub host_token: String,
    pub jwt_public_keys: String,

    pub host_id: super::HostId,
    pub session_id: String,
    pub executor_socket: PathBuf,
    pub host_bind: SocketAddr,
    pub host_route: Option<String>,
    pub public_route_file: Option<PathBuf>,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub socket_jwt_audience: String,
    pub jwt_max_lifetime: Duration,
    pub lease_duration: Duration,
    pub renew_every: Duration,
    pub host_idle_timeout: Duration,
    metadata: Option<HostMetadataFile>,
    startup_started_at: Instant,
    configuration_loaded_at_ms: f64,
}

struct HostMetadataFile {
    path: PathBuf,
    canonical_region: String,
}

impl ActorHostConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }
}

pub async fn serve_actor_host<F>(config: ActorHostConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut timings = HostStartupTimings::new(&config);
    let prepared = match prepare_actor_host(&config, &mut timings).await {
        Ok(prepared) => prepared,
        Err(error) => {
            log_startup(&config, &timings, "failed", Some(&error));
            return Err(error);
        }
    };
    let PreparedActorHost {
        invocation_auth,
        listener,
        route,
        executor_connection,
        mut javascript,
        host,
        lease,
        renewal,
        sockets,
        control_plane,
        archive,
    } = prepared;
    let mut lease_lost = renewal.lease_lost();
    let mut activity = host.activity();
    let mut socket_activity = sockets.registry.activity();

    let service = ActorHostGrpcService::new(
        host.clone(),
        config.session_id.clone(),
        invocation_auth,
        sockets.clone(),
    )
    .into_service();
    if let Err(error) = executor_connection
        .mark_ready(Some(sockets.clone()), Some(sockets.clone()))
        .await
    {
        log_startup(&config, &timings, "failed", Some(&error));
        return Err(error);
    }
    timings.executor_notified_at_ms = Some(timings.elapsed_ms());
    log_startup(&config, &timings, "ready", None);
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let socket_stop = CancellationToken::new();
    let socket_routes =
        crate::sockets::browser::router(crate::sockets::browser::SocketServerState {
            registry: sockets.registry.clone(),
            verifier: crate::control_plane::socket_ticket::SocketTicketVerifier::new(
                &config.jwt_public_keys,
                config.jwt_issuer.clone(),
                config.socket_jwt_audience.clone(),
            )?,
            dispatcher: Arc::new(
                super::sockets::HostSocketDispatcher::new(
                    host.clone(),
                    sockets,
                    config.session_id.clone(),
                )
                .with_events(control_plane, socket_stop.clone()),
            ),
            stop: socket_stop.clone(),
        });
    let routes = tonic::service::Routes::from(socket_routes).add_service(service);
    let mut server = Box::pin(async move {
        axum::serve(listener, routes.into_axum_router())
            .with_graceful_shutdown(async move { server_stop.cancelled().await })
            .await
            .context("serve actor host endpoints")
    });
    let mut executor_task = Box::pin(executor_connection.run(stop.clone()));
    tokio::pin!(shutdown);

    info!(host_id = %config.host_id, route, "durable-object host is ready");
    let stop_result = wait_for_host_stop(
        server.as_mut(),
        executor_task.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease_lost,
        (&mut activity, &mut socket_activity),
        config.host_idle_timeout,
    )
    .await;
    socket_stop.cancel();
    stop_host_tasks(&host, &stop, server, executor_task).await;
    archive.cancel();
    drop(javascript);
    let renewal_result = renewal.shutdown().await;
    let unregister_result = lease.unregister().await;
    info!(host_id = %config.host_id, "durable-object host stopped");
    stop_result?;
    renewal_result?;
    unregister_result
}

impl ActorHostConfig {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let startup_started_at = Instant::now();
        let control_plane_url = required(&mut get, "DURABLE_OBJECT_CONTROL_PLANE_URL")?;
        let host_token = required(&mut get, "DURABLE_OBJECT_HOST_TOKEN")?;
        let jwt_public_keys = required(&mut get, "DURABLE_OBJECT_JWT_PUBLIC_KEYS")?;
        let host_id = super::HostId::new(required(&mut get, "DURABLE_OBJECT_HOST_ID")?);
        ensure!(
            host_id.as_str().starts_with("host.v3."),
            "DURABLE_OBJECT_HOST_ID is invalid"
        );
        let session_id = required(&mut get, "DURABLE_OBJECT_SESSION_ID")?;
        uuid::Uuid::parse_str(&session_id).context("DURABLE_OBJECT_SESSION_ID must be a UUID")?;
        let executor_socket = get("DURABLE_OBJECT_EXECUTOR_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/durable-object-executor.sock"));
        let host_route = get("DURABLE_OBJECT_HOST_ROUTE");
        if let Some(route) = &host_route {
            tonic::transport::Endpoint::from_shared(route.clone())
                .context("DURABLE_OBJECT_HOST_ROUTE must be a valid HTTP or HTTPS URI")?;
        }
        let public_route_file = get("DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE").map(PathBuf::from);
        let metadata = HostMetadataFile::from_lookup(&mut get)?;
        ensure!(
            public_route_file.is_none() || host_route.is_none(),
            "DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE cannot be combined with other host route settings"
        );
        let host_bind = get("DURABLE_OBJECT_HOST_BIND")
            .unwrap_or_else(|| {
                if host_route.is_some() || public_route_file.is_some() {
                    "0.0.0.0:7101"
                } else {
                    "127.0.0.1:0"
                }
                .into()
            })
            .parse()
            .context("DURABLE_OBJECT_HOST_BIND must be a socket address")?;
        let jwt_issuer = get("DURABLE_OBJECT_JWT_ISSUER")
            .unwrap_or_else(|| "durable-object-control-plane".into());
        let invocation_jwt_audience = get("DURABLE_OBJECT_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| "durable-object-invoke".into());
        let jwt_max_lifetime =
            duration_seconds(&mut get, "DURABLE_OBJECT_JWT_MAX_TTL_SECONDS", 86_400)?;
        let lease_duration = duration_ms(&mut get, "DURABLE_OBJECT_LEASE_MS", 30_000)?;
        let renew_every = duration_ms(&mut get, "DURABLE_OBJECT_RENEW_MS", 10_000)?;
        let host_idle_timeout = duration_ms(
            &mut get,
            "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS",
            DEFAULT_HOST_IDLE_TIMEOUT_MS,
        )?;
        ensure!(
            host_idle_timeout.as_millis() <= u128::from(MAX_IDLE_TIMEOUT_MS),
            "DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS is too large"
        );
        ensure!(
            lease_duration.as_millis() <= u128::from(MAX_HOST_LEASE_DURATION_MS),
            "DURABLE_OBJECT_LEASE_MS is too large"
        );
        ensure!(
            renew_every < lease_duration,
            "DURABLE_OBJECT_RENEW_MS must be shorter than DURABLE_OBJECT_LEASE_MS"
        );
        let runtime_config = serde_json::from_str(
            &get("DURABLE_OBJECT_RUNTIME_CONFIG").context("host storage configuration missing")?,
        )
        .context("parse host runtime configuration")?;
        Ok(Self {
            runtime_config,
            control_plane_url,
            host_token,
            jwt_public_keys,
            host_id,
            session_id,
            executor_socket,
            host_bind,
            host_route,
            public_route_file,
            jwt_issuer,
            invocation_jwt_audience,
            socket_jwt_audience: get("DURABLE_OBJECT_SOCKET_JWT_AUDIENCE")
                .unwrap_or_else(|| "durable-object-authority:websocket".into()),
            jwt_max_lifetime,
            lease_duration,
            renew_every,
            host_idle_timeout,
            metadata,
            configuration_loaded_at_ms: startup_started_at.elapsed().as_secs_f64() * 1_000.0,
            startup_started_at,
        })
    }
}

impl HostMetadataFile {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Option<Self>> {
        let Some(path) = get("DURABLE_OBJECT_HOST_METADATA_FILE") else {
            return Ok(None);
        };
        ensure!(
            !path.is_empty(),
            "DURABLE_OBJECT_HOST_METADATA_FILE must not be empty"
        );
        let canonical_region = required(get, "DURABLE_OBJECT_REGION")?;
        crate::placement::validate_region(&canonical_region)?;
        Ok(Some(Self {
            path: path.into(),
            canonical_region,
        }))
    }
}

struct PreparedActorHost {
    archive: CancellationToken,
    sockets: Arc<super::sockets::HostSockets>,
    control_plane: Arc<ControlPlaneClient>,
    invocation_auth: ActorJwtVerifier,
    listener: TcpListener,
    route: String,
    executor_connection: ActorExecutorConnection,
    javascript: tokio::process::Child,
    host: Arc<ActorHost>,
    lease: Arc<HostLeaseMaintainer>,
    renewal: LeaseRenewalTask,
}

async fn prepare_actor_host(
    config: &ActorHostConfig,
    timings: &mut HostStartupTimings,
) -> Result<PreparedActorHost> {
    let invocation_auth = invocation_auth(config)?;
    timings.authentication_ready_at_ms = Some(timings.elapsed_ms());
    let started_at = timings.started_at;
    let (control_plane, (listener, route, endpoint), (executor_connection, javascript)) =
        connect_host_dependencies(
            timed_connection(
                started_at,
                &mut timings.control_plane_client_ready_at_ms,
                ControlPlaneClient::connect(&config.control_plane_url, &config.host_token),
            ),
            bind_host(
                config,
                started_at,
                &mut timings.listener_bound_at_ms,
                &mut timings.route_resolved_at_ms,
            ),
            timed_connection(
                started_at,
                &mut timings.executor_attached_at_ms,
                connect_executor(
                    &config.executor_socket,
                    started_at,
                    &mut timings.javascript_spawned_at_ms,
                ),
            ),
        )
        .await?;
    let control_plane = Arc::new(control_plane);
    let local = Arc::new(
        crate::replication::FileReplicaStore::open(
            std::env::temp_dir()
                .join(format!("little-actors-{}", config.host_id))
                .join("snapshots"),
            crate::replication::DEFAULT_SPOOL_BYTES,
        )
        .await?,
    );
    let archive = crate::replication::start_archiver(local.clone());
    let storage = Arc::new(
        super::storage::HostStorage::new(
            config.runtime_config.clone(),
            config.host_id.clone(),
            config.session_id.clone(),
            config.control_plane_url.clone(),
            control_plane.clone(),
            archive.child_token(),
        )
        .await?,
    );
    let sockets = Arc::new(super::sockets::HostSockets::new(storage.clone()));
    let host = Arc::new(ActorHost::new(
        endpoint.clone(),
        executor_connection.executor(),
        storage.clone(),
        Arc::new(crate::replication::ReplicatedStateTransport::new(
            storage.runtime.clone(),
            Arc::new(GrpcStateTransport::new()),
            local,
        )),
        sockets.clone(),
    ));
    let lease = Arc::new(HostLeaseMaintainer::new(
        endpoint,
        config.session_id.clone(),
        storage,
        Arc::new(SystemClock),
        config.lease_duration,
        config.renew_every,
    )?);
    let renewal = lease.clone().start().await?;
    timings.lease_registered_at_ms = Some(timings.elapsed_ms());
    Ok(PreparedActorHost {
        archive,
        sockets,
        control_plane,
        invocation_auth,
        listener,
        route,
        executor_connection,
        javascript,
        host,
        lease,
        renewal,
    })
}

fn invocation_auth(config: &ActorHostConfig) -> Result<ActorJwtVerifier> {
    ActorJwtVerifier::for_scope(
        &config.jwt_public_keys,
        config.jwt_issuer.clone(),
        config.invocation_jwt_audience.clone(),
        ActorTokenPurpose::Invocation,
        config.jwt_max_lifetime,
    )
}

async fn connect_host_dependencies<C, L, E>(
    control_plane: impl Future<Output = Result<C>>,
    listener: impl Future<Output = Result<L>>,
    executor: impl Future<Output = Result<E>>,
) -> Result<(C, L, E)> {
    tokio::try_join!(control_plane, listener, executor)
}

async fn timed_connection<T>(
    started_at: Instant,
    milestone: &mut Option<f64>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    let result = operation.await?;
    *milestone = Some(started_at.elapsed().as_secs_f64() * 1_000.0);
    Ok(result)
}

async fn bind_host(
    config: &ActorHostConfig,
    started_at: Instant,
    listener_bound_at_ms: &mut Option<f64>,
    route_resolved_at_ms: &mut Option<f64>,
) -> Result<(TcpListener, String, HostEndpoint)> {
    let listener = TcpListener::bind(config.host_bind)
        .await
        .with_context(|| format!("bind actor host at {}", config.host_bind))?;
    *listener_bound_at_ms = Some(started_at.elapsed().as_secs_f64() * 1_000.0);
    let route = advertised_route(config, listener.local_addr()?).await?;
    write_host_metadata(config, &route).await?;
    *route_resolved_at_ms = Some(started_at.elapsed().as_secs_f64() * 1_000.0);
    let endpoint = HostEndpoint {
        id: config.host_id.clone(),
        route: route.clone(),
    };
    Ok((listener, route, endpoint))
}

async fn write_host_metadata(config: &ActorHostConfig, route: &str) -> Result<()> {
    let Some(metadata) = &config.metadata else {
        return Ok(());
    };
    let document = serde_json::to_vec(&serde_json::json!({
        "hostId": config.host_id,
        "sessionId": config.session_id,
        "route": route,
        "canonicalRegion": metadata.canonical_region,
    }))?;
    let temporary = metadata
        .path
        .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    tokio::fs::write(&temporary, document)
        .await
        .context("write actor host metadata")?;
    tokio::fs::rename(&temporary, &metadata.path)
        .await
        .context("publish actor host metadata")
}

struct HostStartupTimings {
    started_at: Instant,
    configuration_loaded_at_ms: f64,
    authentication_ready_at_ms: Option<f64>,
    control_plane_client_ready_at_ms: Option<f64>,
    listener_bound_at_ms: Option<f64>,
    route_resolved_at_ms: Option<f64>,
    executor_attached_at_ms: Option<f64>,
    javascript_spawned_at_ms: Option<f64>,
    lease_registered_at_ms: Option<f64>,
    executor_notified_at_ms: Option<f64>,
}

impl HostStartupTimings {
    fn new(config: &ActorHostConfig) -> Self {
        Self {
            started_at: config.startup_started_at,
            configuration_loaded_at_ms: config.configuration_loaded_at_ms,
            authentication_ready_at_ms: None,
            control_plane_client_ready_at_ms: None,
            listener_bound_at_ms: None,
            route_resolved_at_ms: None,
            executor_attached_at_ms: None,
            javascript_spawned_at_ms: None,
            lease_registered_at_ms: None,
            executor_notified_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64() * 1_000.0
    }
}

fn log_startup(
    config: &ActorHostConfig,
    timings: &HostStartupTimings,
    outcome: &str,
    error: Option<&anyhow::Error>,
) {
    info!(
        event = "actor_host_startup",

        host_id = %config.host_id,
        started_at_ms = 0,
        configuration_loaded_at_ms = timings.configuration_loaded_at_ms,
        authentication_ready_at_ms = timings.authentication_ready_at_ms,
        control_plane_client_ready_at_ms = timings.control_plane_client_ready_at_ms,
        listener_bound_at_ms = timings.listener_bound_at_ms,
        route_resolved_at_ms = timings.route_resolved_at_ms,
        executor_attached_at_ms = timings.executor_attached_at_ms,
        javascript_spawned_at_ms = timings.javascript_spawned_at_ms,
        lease_registered_at_ms = timings.lease_registered_at_ms,
        executor_notified_at_ms = timings.executor_notified_at_ms,
        completed_at_ms = timings.elapsed_ms(),
        outcome,
        error = error.map(|error| format!("{error:#}")),
        "actor host startup completed"
    );
}

async fn advertised_route(config: &ActorHostConfig, bound: SocketAddr) -> Result<String> {
    if let Some(route) = &config.host_route {
        return Ok(route.clone());
    }
    if let Some(path) = &config.public_route_file {
        return tokio::time::timeout(Duration::from_secs(60), read_public_route(path))
            .await
            .context("public host route was not published within 60 seconds")?;
    }
    Ok(format!("http://{bound}"))
}

async fn read_public_route(path: &std::path::Path) -> Result<String> {
    loop {
        match tokio::fs::read_to_string(path).await {
            Ok(route) if !route.trim().is_empty() => {
                let route = route.trim().to_owned();
                let endpoint = tonic::transport::Endpoint::from_shared(route.clone())
                    .context("public host route file must contain a valid HTTPS URI")?;
                ensure!(
                    endpoint.uri().scheme_str() == Some("https") && endpoint.uri().host().is_some(),
                    "public host route must use HTTPS"
                );
                return Ok(route);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("read public host route"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn connect_executor(
    socket: &std::path::Path,
    started_at: Instant,
    javascript_spawned_at_ms: &mut Option<f64>,
) -> Result<(ActorExecutorConnection, tokio::process::Child)> {
    let listener = ActorExecutorListener::bind(socket).await?;
    let javascript = spawn_javascript_process()?;
    *javascript_spawned_at_ms = Some(started_at.elapsed().as_secs_f64() * 1_000.0);
    Ok((listener.accept().await?, javascript))
}

async fn wait_for_host_stop<ServerFuture, ExecutorFuture, ShutdownFuture>(
    mut server: std::pin::Pin<&mut ServerFuture>,
    mut executor: std::pin::Pin<&mut ExecutorFuture>,
    javascript: &mut tokio::process::Child,
    mut shutdown: std::pin::Pin<&mut ShutdownFuture>,
    lease_lost: &mut tokio::sync::watch::Receiver<bool>,
    activity: (
        &mut tokio::sync::watch::Receiver<usize>,
        &mut tokio::sync::watch::Receiver<usize>,
    ),
    idle_timeout: Duration,
) -> Result<()>
where
    ServerFuture: Future<Output = Result<()>> + ?Sized,
    ExecutorFuture: Future<Output = Result<()>> + ?Sized,
    ShutdownFuture: Future<Output = ()> + ?Sized,
{
    let (activity, socket_activity) = activity;
    let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;
    loop {
        tokio::select! {
            result = server.as_mut() => break result.context("serve actor host network endpoints"),
            result = executor.as_mut() => break result.context("run JavaScript actor executor"),
            result = javascript.wait() => break Err(anyhow::anyhow!("JavaScript actor executor exited with {}", result?)),
            () = shutdown.as_mut() => break Ok(()),
            changed = lease_lost.changed() => {
                if changed.is_err() || *lease_lost.borrow() {
                    break Err(anyhow::anyhow!("host lease expired; host self-fenced"));
                }
            }
            changed = socket_activity.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("socket activity tracker stopped")); }
                if *socket_activity.borrow() == 0 { idle_deadline = tokio::time::Instant::now() + idle_timeout; }
            }
            changed = activity.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("actor activity tracker stopped")); }
                if *activity.borrow() == 0 {
                    idle_deadline = tokio::time::Instant::now() + idle_timeout;
                }
            }
            () = tokio::time::sleep_until(idle_deadline), if *activity.borrow() == 0 && *socket_activity.borrow() == 0 => break Ok(()),
        }
    }
}

async fn stop_host_tasks(
    host: &ActorHost,
    stop: &CancellationToken,
    server: impl Future,
    executor: impl Future,
) {
    if let Err(error) = host.drain(HOST_ACTOR_DRAIN_TIMEOUT).await {
        error!(error = %format!("{error:#}"), "actor invocations did not drain cleanly");
    }
    stop.cancel();
    let _ = tokio::time::timeout(HOST_TASK_SHUTDOWN_TIMEOUT, async {
        let _ = tokio::join!(server, executor);
    })
    .await;
}

fn spawn_javascript_process() -> Result<tokio::process::Child> {
    Command::new("node")
        .args([
            "--eval",
            "import(process.env.DURABLE_OBJECT_SDK_HOST ?? \"little-actors/host\").then(module => module.runDurableObjectHost())",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("start JavaScript actor executor")
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

fn duration_ms(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of milliseconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_millis(value))
}

fn duration_seconds(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of seconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_secs(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn host_needs_no_local_state_directory() -> Result<()> {
        let values = values();
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
        assert_eq!(
            config.executor_socket,
            PathBuf::from("/tmp/durable-object-executor.sock")
        );
        assert_eq!(config.host_idle_timeout, Duration::from_secs(300));
        assert_eq!(config.jwt_max_lifetime, Duration::from_secs(86_400));
        Ok(())
    }

    #[tokio::test]
    async fn host_publishes_complete_metadata_before_dependencies_are_ready() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("host.json");
        let mut values = values();
        values.insert("DURABLE_OBJECT_HOST_BIND".into(), "127.0.0.1:0".into());
        values.insert(
            "DURABLE_OBJECT_HOST_METADATA_FILE".into(),
            path.display().to_string(),
        );
        values.insert("DURABLE_OBJECT_REGION".into(), "north-america-east".into());
        values.insert(
            "DURABLE_OBJECT_HOST_ROUTE".into(),
            "https://host.example.com".into(),
        );
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
        let (_, route, _) = bind_host(&config, Instant::now(), &mut None, &mut None).await?;
        let metadata: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
        assert_eq!(
            metadata,
            serde_json::json!({
                    "hostId": config.host_id,
            "sessionId": config.session_id,
                    "route": route,
                    "canonicalRegion": "north-america-east",
                })
        );
        let handle: crate::sandbox::ActorHostHandle = serde_json::from_value(metadata)?;
        assert_eq!(handle.host_id, config.host_id);
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn metadata_publication_failure_prevents_host_readiness() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut values = values();
        values.insert(
            "DURABLE_OBJECT_HOST_METADATA_FILE".into(),
            directory
                .path()
                .join("missing/host.json")
                .display()
                .to_string(),
        );
        values.insert("DURABLE_OBJECT_REGION".into(), "north-america-east".into());
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
        assert!(
            bind_host(&config, Instant::now(), &mut None, &mut None)
                .await
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn host_metadata_requires_a_valid_region() {
        for region in [None, Some(""), Some("bad/region")] {
            let mut values = values();
            values.insert(
                "DURABLE_OBJECT_HOST_METADATA_FILE".into(),
                "/tmp/host.json".into(),
            );
            if let Some(region) = region {
                values.insert("DURABLE_OBJECT_REGION".into(), region.into());
            }
            assert!(ActorHostConfig::from_lookup(|name| values.get(name).cloned()).is_err());
        }
    }

    #[tokio::test]
    async fn host_connections_start_without_waiting_for_each_other() -> Result<()> {
        let barrier = tokio::sync::Barrier::new(3);
        let connect = || async {
            barrier.wait().await;
            Ok(())
        };
        tokio::time::timeout(
            Duration::from_millis(100),
            connect_host_dependencies(connect(), connect(), connect()),
        )
        .await
        .context("host dependencies ran sequentially")??;
        Ok(())
    }

    #[tokio::test]
    async fn public_route_can_arrive_after_the_host_process_starts() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("route");
        let mut values = values();
        values.insert(
            "DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE".into(),
            path.display().to_string(),
        );
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
        assert_eq!(config.host_bind, "0.0.0.0:7101".parse()?);
        let publish = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tokio::fs::write(path, "https://host.example.com").await?;
            anyhow::Ok(())
        };
        let (route, ()) = tokio::try_join!(advertised_route(&config, config.host_bind), publish)?;
        assert_eq!(route, "https://host.example.com");
        Ok(())
    }

    #[test]
    fn public_route_file_cannot_be_combined_with_other_route_settings() {
        {
            let conflict = "DURABLE_OBJECT_HOST_ROUTE";
            let mut values = values();
            values.insert(
                "DURABLE_OBJECT_HOST_PUBLIC_ROUTE_FILE".into(),
                "/tmp/input-route".into(),
            );
            values.insert(conflict.into(), "https://host.example.com".into());
            assert!(
                ActorHostConfig::from_lookup(|name| values.get(name).cloned()).is_err(),
                "{conflict}"
            );
        }
    }

    #[test]
    fn startup_timings_begin_with_only_configuration_loaded() {
        let values = values();
        let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
        let timings = HostStartupTimings::new(&config);

        assert!(timings.configuration_loaded_at_ms <= timings.elapsed_ms());
        assert!(timings.control_plane_client_ready_at_ms.is_none());
        assert!(timings.javascript_spawned_at_ms.is_none());
        assert!(timings.executor_notified_at_ms.is_none());
    }

    #[tokio::test]
    async fn open_sockets_prevent_idle_host_shutdown() -> Result<()> {
        let mut server = Box::pin(std::future::pending::<Result<()>>());
        let mut executor = Box::pin(std::future::pending::<Result<()>>());
        let mut shutdown = Box::pin(std::future::pending::<()>());
        let mut javascript = tokio::process::Command::new("sleep")
            .arg("60")
            .kill_on_drop(true)
            .spawn()?;
        let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
        let (_activity_sender, mut activity) = tokio::sync::watch::channel(0);
        let (sockets, mut socket_activity) = tokio::sync::watch::channel(1);
        let mut stopped = Box::pin(wait_for_host_stop(
            server.as_mut(),
            executor.as_mut(),
            &mut javascript,
            shutdown.as_mut(),
            &mut lease,
            (&mut activity, &mut socket_activity),
            Duration::from_millis(10),
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stopped.as_mut())
                .await
                .is_err()
        );
        sockets.send_replace(0);
        tokio::time::timeout(Duration::from_secs(1), stopped.as_mut()).await??;
        drop(stopped);
        javascript.kill().await?;
        Ok(())
    }

    fn values() -> HashMap<String, String> {
        HashMap::from([
            (
                "DURABLE_OBJECT_RUNTIME_CONFIG".into(),
                serde_json::json!({
                    "bucket": {"type":"file", "directory":"/tmp/actor-test-bucket"},
                    "region":"north-america-east", "replicaSecret":"secret", "replicas":[],
                    "token":null
                })
                .to_string(),
            ),
            (
                "DURABLE_OBJECT_CONTROL_PLANE_URL".into(),
                "http://127.0.0.1:7100".into(),
            ),
            ("DURABLE_OBJECT_HOST_TOKEN".into(), "host-jwt".into()),
            ("DURABLE_OBJECT_JWT_PUBLIC_KEYS".into(), "{}".into()),
            (
                "DURABLE_OBJECT_HOST_ID".into(),
                "host.v3.revision-1.host-1".into(),
            ),
            (
                "DURABLE_OBJECT_SESSION_ID".into(),
                "00000000-0000-4000-8000-000000000001".into(),
            ),
        ])
    }
}
