use std::{future::Future, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::net::TcpListener;

use super::process::{
    ActorHostConfig, executor_runtime, serve_assigned_host, spawn_executor_process,
};
use crate::actor::{ActorExecutorListener, WarmExecutor};
use crate::bucket::WarmGcs;
use crate::litestream::{Litestream, Replicator};

pub(super) struct WarmHost {
    pub readiness: Option<tokio::sync::oneshot::Sender<super::process::HostReadiness>>,
    pub listener: super::server::HostServer,
    pub executor: WarmExecutor,
    pub javascript: tokio::process::Child,
    pub entrypoint: String,
    pub storage: WarmGcs,
    pub replication: Arc<dyn Replicator>,
}

pub async fn serve_warm(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    super::protect_runtime_credentials()?;
    let socket = std::env::var("DURABLE_ACTORS_EXECUTOR_SOCKET")
        .unwrap_or_else(|_| "/tmp/durable-actors-executor.sock".into());
    let authorization = assignment_authorization(|name| std::env::var(name).ok())?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readiness = ready.clone();
    let routes = super::assignment::router(authorization, send, "/customer".into()).route(
        "/warmz",
        axum::routing::get(move || {
            let ready = readiness.load(std::sync::atomic::Ordering::Acquire);
            async move {
                if ready {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    );
    let bind = std::env::var("DURABLE_ACTORS_HOST_BIND").unwrap_or_else(|_| "0.0.0.0:80".into());
    let mut listener = super::server::HostServer::new(TcpListener::bind(bind).await?, routes);
    let ipc = ActorExecutorListener::bind(&socket).await?;
    let configured_runtime = std::env::var("DURABLE_ACTORS_EXECUTOR_RUNTIME").ok();
    let runtime = executor_runtime(None, configured_runtime.as_deref())?;
    let mut javascript = spawn_executor_process(true, &socket, runtime)?;
    let (executor, storage) = tokio::try_join!(
        async { tokio::time::timeout(Duration::from_secs(30), ipc.accept_warm()).await? },
        WarmGcs::new(),
    )?;
    ready.store(true, std::sync::atomic::Ordering::Release);
    tokio::pin!(shutdown);
    let assigned = tokio::select! {
        value = receive => value?,
        result = listener.stopped() => anyhow::bail!("warm assignment server stopped: {result:?}"),
        status = javascript.wait() => anyhow::bail!("generic actor executor exited: {}", status?),
        () = &mut shutdown => return Ok(()),
    };
    let environment = assigned.environment;
    let config = ActorHostConfig::from_lookup(|name| environment.get(name).cloned())?;
    ensure!(
        config.actor.is_some(),
        "runtime assignment must name exactly one actor"
    );
    let entrypoint = environment
        .get("DURABLE_ACTORS_ENTRYPOINT")
        .context("missing customer entrypoint")?
        .clone();
    ensure!(
        entrypoint.starts_with("/customer/")
            && (entrypoint.ends_with(".mjs") || entrypoint.ends_with(".pyz"))
            && !Path::new(&entrypoint)
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "customer entrypoint must be a compiled module under /customer"
    );
    ensure!(
        crate::artifacts::ActorRuntime::from_entrypoint(&entrypoint)? == runtime,
        "actor artifact does not match the prewarmed executor runtime"
    );
    let warm = WarmHost {
        readiness: Some(assigned.ready),
        listener,
        executor,
        javascript,
        entrypoint,
        storage,
        replication: Arc::new(Litestream::default()),
    };
    serve_assigned_host(config, Some(warm), shutdown).await
}

fn assignment_authorization(
    get: impl Fn(&str) -> Option<String>,
) -> Result<crate::control_plane::assignment::AssignmentVerifier> {
    let keys =
        get("DURABLE_ACTORS_ASSIGNMENT_PUBLIC_KEYS").context("assignment public keys missing")?;
    let path =
        get("DURABLE_ACTORS_SANDBOX_IDENTITY_FILE").context("sandbox identity file missing")?;
    crate::control_plane::assignment::AssignmentVerifier::new(
        &keys,
        &get("DURABLE_ACTORS_JWT_ISSUER").context("assignment issuer missing")?,
        move || {
            let uid = std::fs::read_to_string(&path)?;
            let uid = uid.trim();
            ensure!(!uid.is_empty(), "sandbox identity file is empty");
            Ok(uid.to_owned())
        },
    )
}

#[cfg(test)]
#[path = "../../tests/unit/host/warm.rs"]
mod tests;
