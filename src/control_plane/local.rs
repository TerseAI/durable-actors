use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    os::fd::FromRawFd,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use axum::Router;
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, ValueEnum};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{
    clock::SystemClock,
    sandbox::{HostSandboxRuntimeConfig, LocalSandboxProvider},
    sqlite::SqliteStore,
    storage_urls::{GcsStorageUrlSigner, LocalStorage, StorageUrlSigner},
};

use super::{
    ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, ControlPlaneService,
    admin::{AdminRegistry, AdminService, HostLaunchSpec},
    public_api,
    service::SandboxHostProvisioner,
};

#[derive(Args)]
pub struct DevOptions {
    #[arg(long, default_value = ".")]
    pub project: PathBuf,
    #[arg(long, default_value_t = 7100)]
    pub port: u16,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long, default_value = "src/durable-objects.ts")]
    pub entrypoint: String,
    #[arg(long, value_enum, default_value = "local")]
    pub storage: DevStorage,
    #[arg(long, hide = true, value_parser = clap::value_parser!(i32).range(3..))]
    pub ready_fd: Option<i32>,
    #[arg(long, hide = true)]
    pub sdk_host: Option<PathBuf>,
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
    let database = Arc::new(SqliteStore::open(&directory.join("runtime.sqlite")).await?);
    database.reset_local_leases().await?;
    let storage = local_storage(&options, &directory, &origin).await?;
    let provider = Arc::new(LocalSandboxProvider::new(
        std::env::current_exe()?,
        project.clone(),
        database.clone(),
        options.sdk_host.clone(),
    ));
    let api_key = uuid::Uuid::new_v4().simple().to_string();
    let routes = local_routes(
        &options,
        &project,
        &origin,
        database,
        &storage,
        provider.clone(),
        &api_key,
    )
    .await?;
    let server = LocalServer::start(listener, routes, provider);
    let ready = publish_connection(
        &directory,
        &origin,
        &api_key,
        &storage.region,
        options.ready_fd,
    );
    if ready.is_ok() {
        println!(
            "Local actors ready at {origin}\nState: {}\nGenerate a browser SDK: npx little-actors generate\nRestart this command after changing actor code.",
            directory.display()
        );
        if matches!(options.storage, DevStorage::Local) {
            println!(
                "Local storage is for development; losing this directory loses your actor state."
            );
        }
    }
    let result = server.run_until(shutdown, ready).await;
    let _ = std::fs::remove_file(directory.join("runtime.json"));
    result
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
            axum::serve(listener, routes.into_axum_router())
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
    signer: Arc<dyn StorageUrlSigner>,
    routes: Router,
    region: String,
}

async fn local_storage(options: &DevOptions, directory: &Path, origin: &str) -> Result<LocalState> {
    let (state, identity) = match options.storage {
        DevStorage::Local => {
            let storage = Arc::new(LocalStorage::new(
                directory.to_owned(),
                origin.to_owned(),
                Arc::new(SystemClock),
            )?);
            (
                LocalState {
                    region: storage.regions()[0].clone(),
                    routes: storage.clone().router(),
                    signer: storage,
                },
                serde_json::json!({ "storage": "local" }),
            )
        }
        DevStorage::Gcs => {
            let buckets: HashMap<String, String> = serde_json::from_str(
                &std::env::var("DURABLE_OBJECT_STANDARD_BUCKETS")
                    .context("--storage gcs requires DURABLE_OBJECT_STANDARD_BUCKETS")?,
            )?;
            let identity = serde_json::json!({ "storage": "gcs", "buckets": buckets });
            let storage = Arc::new(GcsStorageUrlSigner::from_adc(buckets).await?);
            (
                LocalState {
                    region: storage.regions()[0].clone(),
                    routes: Router::new(),
                    signer: storage,
                },
                identity,
            )
        }
    };
    let path = directory.join("storage.json");
    if path.exists() {
        let previous: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        ensure!(
            previous == identity,
            "storage configuration changed; select a separate --data-dir to avoid losing access to saved state"
        );
    } else {
        write_private_json(&path, &identity)?;
    }
    Ok(state)
}

async fn local_routes(
    options: &DevOptions,
    project: &Path,
    origin: &str,
    database: Arc<SqliteStore>,
    storage: &LocalState,
    provider: Arc<LocalSandboxProvider>,
    api_key: &str,
) -> Result<tonic::service::Routes> {
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
        code_revision: "local".into(),
        image_ref: "local".into(),
        working_directory: project.display().to_string(),
        actor_entrypoint: Some(options.entrypoint.clone()),
        secret_refs: vec![],
        socket_gateway_url: None,
    };
    database
        .ensure_namespace_and_register_deployment(&spec)
        .await?;
    let runtime = HostSandboxRuntimeConfig {
        control_plane_url: origin.to_owned(),
        jwt_issuer: "durable-object-control-plane".into(),
        invocation_jwt_audience: "durable-object-invoke".into(),
        actor_idle_timeout_ms: 60_000,
        host_idle_timeout_ms: 300_000,
    };
    let provisioner = Arc::new(SandboxHostProvisioner::new(
        provider,
        runtime,
        issuer.clone(),
        database.clone(),
    ));
    let service = ControlPlaneService::new(
        database.clone(),
        database.clone(),
        storage.signer.clone(),
        auth,
        database.clone(),
        issuer.clone(),
        provisioner,
    );
    let admin = AdminService::new(api_key.to_owned(), database.clone(), issuer)?
        .with_default_namespace("local")?
        .with_socket_origin(origin)?;
    let inspector = super::inspection::ActorInspector::new(
        database,
        storage.signer.clone(),
        Arc::new(crate::state_transport::HttpStateTransport::new()),
    );
    let public = public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::router(inspector, admin))
        .merge(storage.routes.clone());
    Ok(tonic::service::Routes::from(public).add_service(service.into_internal_service()))
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

fn publish_connection(
    directory: &Path,
    origin: &str,
    api_key: &str,
    region: &str,
    ready_fd: Option<i32>,
) -> Result<()> {
    let connection = serde_json::json!({ "pid": std::process::id(), "controlPlaneUrl": origin, "namespaceId": "local", "apiKey": api_key, "storageRegion": region });
    write_private_json(&directory.join("runtime.json"), &connection)?;
    if let Some(fd) = ready_fd {
        // The launcher transfers ownership of this inherited readiness descriptor.
        let mut ready = unsafe { File::from_raw_fd(fd) };
        serde_json::to_writer(&mut ready, &connection)?;
        ready.flush()?;
    }
    Ok(())
}

fn write_private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut temporary =
        tempfile::NamedTempFile::new_in(path.parent().context("file has no parent")?)?;
    temporary.write_all(&serde_json::to_vec_pretty(value)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}
