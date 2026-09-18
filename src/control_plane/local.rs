use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    os::fd::FromRawFd,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anstyle::{AnsiColor, Style};
use anyhow::{Context, Result, ensure};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use axum::{Router, extract::Request, response::Response};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, ValueEnum};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{Span, info, info_span};

use crate::{
    bucket::{
        Bucket, BucketHostLeases, FileBucket, GcsBucket, HttpReplicaPeers, RuntimeStorage,
        access::{BucketLocation, RuntimeAccess},
    },
    clock::SystemClock,
    host_leases::HostLeaseStore,
    sandbox::{HostSandboxRuntimeConfig, LocalSandboxProvider},
};

use super::{
    ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, ControlPlaneService,
    admin::{AdminRegistry, AdminService, HostLaunchSpec, LocalAdminRegistry},
    contracts::PublicActorContract,
    public_api,
    service::SandboxHostProvisioner,
};

#[derive(Args)]
pub struct DevOptions {
    #[arg(long, env = "DURABLE_OBJECT_API_KEY")]
    pub api_key: Option<String>,
    #[arg(long, env = "DURABLE_OBJECT_PROJECT", default_value = ".")]
    pub project: PathBuf,
    #[arg(long, env = "DURABLE_OBJECT_PORT", default_value_t = 7100)]
    pub port: u16,
    #[arg(long, env = "DURABLE_OBJECT_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    #[arg(
        long,
        env = "DURABLE_OBJECT_ENTRYPOINT",
        default_value = "src/durable-objects.ts"
    )]
    pub entrypoint: String,
    #[arg(
        long,
        env = "DURABLE_OBJECT_STORAGE",
        value_enum,
        default_value = "local"
    )]
    pub storage: DevStorage,
    #[arg(long, hide = true, value_parser = clap::value_parser!(i32).range(3..))]
    pub ready_fd: Option<i32>,
    #[arg(long, hide = true)]
    pub sdk_host: Option<PathBuf>,
    #[arg(long, hide = true)]
    pub contract: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum DevStorage {
    Local,
    Gcs,
}

pub async fn serve_local(
    options: DevOptions,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let project = options
        .project
        .canonicalize()
        .context("find actor project directory")?;
    ensure!(
        project.join(&options.entrypoint).is_file(),
        "actor file {} is missing; create it before starting the demo",
        options.entrypoint
    );
    let directory = options
        .data_dir
        .clone()
        .unwrap_or_else(|| project.join(".little-actors"));
    let _lock = prepare_directory(&directory)?;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, options.port))
        .await
        .context("bind local runtime; use --port to select another port")?;
    let origin = format!("http://{}", listener.local_addr()?);
    let generated_api_key = options.api_key.is_none();
    let api_key = options
        .api_key
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let storage = local_storage(&options, &directory, &origin).await?;
    let provider = Arc::new(LocalSandboxProvider::new(
        std::env::current_exe()?,
        project.clone(),
        storage.leases.clone(),
        options.sdk_host.clone(),
    ));
    let routes = local_routes(
        &options,
        &project,
        &origin,
        &storage,
        provider.clone(),
        &api_key,
    )
    .await?;
    let server = LocalServer::start(listener, routes, provider);
    let ready = notify_launcher(&origin, &api_key, &storage.region, options.ready_fd);
    if ready.is_ok() {
        if generated_api_key && options.ready_fd.is_none() {
            println!(
                "Set this in the terminal running your application backend:\nexport DURABLE_OBJECT_API_KEY={api_key}"
            );
        }
        anstream::println!(
            "{}",
            styled_local_ready_message(
                &origin,
                &directory,
                matches!(options.storage, DevStorage::Local)
            )
        );
    }
    server.run_until(shutdown, ready).await
}

struct LocalServer {
    provider: Arc<LocalSandboxProvider>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl LocalServer {
    fn start(
        listener: TcpListener,
        routes: tonic::service::Routes,
        provider: Arc<LocalSandboxProvider>,
    ) -> Self {
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, logged_routes(routes))
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
        });
        Self {
            provider,
            stop,
            server,
        }
    }

    async fn run_until(
        mut self,
        shutdown: impl Future<Output = ()>,
        ready: Result<()>,
    ) -> Result<()> {
        let result = match ready {
            Ok(()) => {
                tokio::select! {
                    _ = shutdown => Ok(()),
                    result = &mut self.server => result.context("local server task failed").and_then(|result| result.context("local server failed")),
                }
            }
            Err(error) => Err(error),
        };
        // Hosts unregister their leases through this server while draining.
        self.provider.shutdown().await;
        self.stop.cancel();
        if !self.server.is_finished()
            && tokio::time::timeout(Duration::from_secs(5), &mut self.server)
                .await
                .is_err()
        {
            self.server.abort();
        }
        result
    }
}

fn logged_routes(routes: tonic::service::Routes) -> Router {
    routes.into_axum_router().layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request| {
                info_span!(
                    target: "little_actors::dev",
                    "control_plane_request",
                    method = %request.method(),
                    path = %request.uri().path(),
                )
            })
            .on_response(|response: &Response, latency: Duration, span: &Span| {
                info!(
                    target: "little_actors::dev",
                    parent: span,
                    status = response.status().as_u16(),
                    latency_ms = %format_args!("{:.1}", latency.as_secs_f64() * 1_000.0),
                    "request completed"
                );
            })
            .on_failure(()),
    )
}

fn prepare_directory(directory: &Path) -> Result<File> {
    std::fs::create_dir_all(directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("runtime.lock"))?;
    lock.try_lock()
        .context("another local runtime is already using this data directory")?;
    std::fs::write(directory.join(".gitignore"), "*\n")?;
    Ok(lock)
}

struct LocalState {
    runtime: Arc<RuntimeStorage>,
    access: Arc<RuntimeAccess>,
    leases: Arc<dyn HostLeaseStore>,
    region: String,
}

async fn local_storage(options: &DevOptions, directory: &Path, origin: &str) -> Result<LocalState> {
    let location = match options.storage {
        DevStorage::Local => BucketLocation::File {
            directory: directory.canonicalize()?.join("objects"),
        },
        DevStorage::Gcs => BucketLocation::Gcs {
            bucket: std::env::var("DURABLE_OBJECT_BUCKET")
                .context("--storage gcs requires DURABLE_OBJECT_BUCKET")?,
        },
    };
    let bucket: Arc<dyn Bucket> = match &location {
        BucketLocation::File { directory } => Arc::new(FileBucket::new(directory.clone())?),
        BucketLocation::Gcs { bucket } => Arc::new(GcsBucket::new(bucket).await?),
    };
    let leases = Arc::new(BucketHostLeases::new(bucket.clone(), Arc::new(SystemClock)));
    let access = crate::replication::ReplicaAccess::new(
        &uuid::Uuid::new_v4().to_string(),
        Arc::new(SystemClock),
    );
    let fleet = Arc::new(crate::replication::ReplicaSet::default());
    let bootstrap = Arc::new(RuntimeAccess::new(location, fleet.clone(), access.clone())?);
    let runtime = Arc::new(RuntimeStorage::new(
        bucket,
        leases.clone(),
        fleet,
        Arc::new(HttpReplicaPeers::new(access.clone())?),
        access,
        origin.into(),
    )?);
    Ok(LocalState {
        runtime,
        access: bootstrap,
        leases,
        region: "north-america-east".into(),
    })
}

async fn local_routes(
    options: &DevOptions,
    project: &Path,
    origin: &str,
    storage: &LocalState,
    provider: Arc<LocalSandboxProvider>,
    api_key: &str,
) -> Result<tonic::service::Routes> {
    let contract = options.contract.as_deref().map(read_contract).transpose()?;
    let issuer = local_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "durable-object-control-plane",
        "durable-object-authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(86_400),
    )?;
    let spec = HostLaunchSpec {
        namespace_id: "local".into(),
        code_revision: uuid::Uuid::new_v4().to_string(),
        image_ref: "local".into(),
        working_directory: project.display().to_string(),
        actor_entrypoint: Some(options.entrypoint.clone()),
        secret_refs: vec![],
        socket_gateway_url: None,
    };
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_deployment(&spec, contract.as_ref())
        .await?;
    let runtime = HostSandboxRuntimeConfig {
        control_plane_url: origin.to_owned(),
        jwt_issuer: "durable-object-control-plane".into(),
        invocation_jwt_audience: "durable-object-invoke".into(),
        actor_idle_timeout_ms: 60_000,
        host_idle_timeout_ms: 300_000,
    };
    let provisioner = Arc::new(
        SandboxHostProvisioner::new(provider, runtime, issuer.clone(), storage.leases.clone())
            .with_runtime_access(storage.access.clone()),
    );
    let service = ControlPlaneService::new(
        storage.leases.clone(),
        storage.runtime.clone(),
        auth,
        registry.clone(),
        issuer.clone(),
        provisioner,
    )
    .with_runtime_access(storage.access.clone());
    let admin = AdminService::new(api_key.to_owned(), registry, issuer)?
        .with_default_namespace("local")?
        .with_socket_origin(origin)?;
    let inspector = super::inspection::ActorInspector::new(
        storage.runtime.clone(),
        storage.runtime.clone(),
        storage.runtime.clone(),
        service.sockets.clone(),
    );
    let public = public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::router(inspector, admin))
        .merge(storage.runtime.clone().router());
    Ok(tonic::service::Routes::from(public).add_service(service.into_internal_service()))
}

fn read_contract(path: &Path) -> Result<PublicActorContract> {
    let bytes = std::fs::read(path).context("read local actor contract")?;
    PublicActorContract::new(serde_json::from_slice(&bytes).context("parse local actor contract")?)
}

fn local_issuer() -> Result<ActorJwtIssuer> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("generate local signing key"))?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(key.as_ref()),
        "local",
        "durable-object-control-plane",
        "durable-object-authority",
        "durable-object-invoke",
        Duration::from_secs(86_400),
    )
}

fn notify_launcher(origin: &str, api_key: &str, region: &str, ready_fd: Option<i32>) -> Result<()> {
    if let Some(fd) = ready_fd {
        // The launcher transfers ownership of this inherited readiness descriptor.
        let mut ready = unsafe { File::from_raw_fd(fd) };
        let connection = serde_json::json!({ "pid": std::process::id(), "controlPlaneUrl": origin, "namespaceId": "local", "apiKey": api_key, "storageRegion": region });
        serde_json::to_writer(&mut ready, &connection)?;
        ready.flush()?;
    }
    Ok(())
}

fn styled_local_ready_message(origin: &str, directory: &Path, local_storage: bool) -> String {
    let styles = LocalReadyStyles {
        title: Style::new().bold().fg_color(Some(AnsiColor::Cyan.into())),
        context: Style::new().fg_color(Some(AnsiColor::BrightBlack.into())),
        ready: Style::new().bold().fg_color(Some(AnsiColor::Green.into())),
        label: Style::new().bold(),
        command: Style::new().fg_color(Some(AnsiColor::Cyan.into())),
    };
    format_local_ready_message(origin, directory, local_storage, styles)
}

#[cfg(test)]
fn local_ready_message(origin: &str, directory: &Path) -> String {
    format_local_ready_message(origin, directory, true, LocalReadyStyles::default())
}

fn format_local_ready_message(
    origin: &str,
    directory: &Path,
    local_storage: bool,
    styles: LocalReadyStyles,
) -> String {
    let LocalReadyStyles {
        title,
        context,
        ready,
        label,
        command,
    } = styles;
    let note = if local_storage {
        "\n\n  State persists between restarts. Delete the state directory to start fresh."
    } else {
        ""
    };
    format!(
        "{title}little actors{title:#} {context}/ local{context:#}\n\n  {ready}Ready{ready:#}  {origin}\n  {label}State{label:#}  {}\n  {label}Next{label:#}   {command}npx little-actors generate --url {origin}{command:#}{note}",
        directory.display(),
    )
}

#[derive(Clone, Copy, Default)]
struct LocalReadyStyles {
    title: Style,
    context: Style,
    ready: Style,
    label: Style,
    command: Style,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_message_has_clear_hierarchy_and_next_step() {
        let message = local_ready_message(
            "http://127.0.0.1:7100",
            Path::new("/projects/chat/.little-actors"),
        );

        assert_eq!(
            message,
            "little actors / local\n\n  Ready  http://127.0.0.1:7100\n  State  /projects/chat/.little-actors\n  Next   npx little-actors generate --url http://127.0.0.1:7100\n\n  State persists between restarts. Delete the state directory to start fresh."
        );
    }

    #[tokio::test]
    async fn relative_state_directories_are_absolute_in_host_configuration() -> Result<()> {
        let cwd = std::env::current_dir()?;
        let directory = tempfile::tempdir_in(&cwd)?;
        let relative = directory.path().strip_prefix(&cwd)?;
        let options = DevOptions {
            api_key: Some("test-key".into()),
            contract: None,
            project: cwd.clone(),
            port: 0,
            data_dir: Some(relative.into()),
            entrypoint: "actors.ts".into(),
            storage: DevStorage::Local,
            ready_fd: None,
            sdk_host: None,
        };
        let state = local_storage(&options, relative, "http://localhost:7100").await?;
        let config: crate::bucket::access::HostStorageConfig =
            serde_json::from_str(&state.access.bootstrap("local", &state.region).await?)?;
        let BucketLocation::File {
            directory: configured,
        } = config.bucket
        else {
            panic!("expected file bucket")
        };
        assert_eq!(configured, directory.path().canonicalize()?.join("objects"));
        Ok(())
    }
}
