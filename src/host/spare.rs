use std::{future::Future, path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::net::TcpListener;

use super::process::{ActorHostConfig, serve_assigned_host, spawn_javascript_process};
use crate::actor::{ActorExecutorListener, WarmExecutor};

pub(super) struct WarmHost {
    pub readiness: Option<tokio::sync::oneshot::Sender<super::process::HostReadiness>>,
    pub listener: TcpListener,
    pub executor: WarmExecutor,
    pub javascript: tokio::process::Child,
    pub entrypoint: String,
}

pub async fn serve_spare(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let socket = std::env::var("DURABLE_ACTORS_EXECUTOR_SOCKET")
        .unwrap_or_else(|_| "/tmp/durable-actors-executor.sock".into());
    let token = std::env::var("DURABLE_ACTORS_SPARE_TOKEN").context("spare token missing")?;
    ensure!(token.len() >= 32, "spare token is too short");
    let control_bind =
        std::env::var("DURABLE_ACTORS_SPARE_BIND").unwrap_or_else(|_| "0.0.0.0:7102".into());
    let control = TcpListener::bind(control_bind).await?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let stop = tokio_util::sync::CancellationToken::new();
    let _stop_guard = stop.clone().drop_guard();
    let mut server = tokio::spawn(async move {
        axum::serve(control, super::assignment::router(token, send))
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
    });
    let ready = std::env::var("DURABLE_ACTORS_SPARE_READY_FILE")
        .unwrap_or_else(|_| "/tmp/durable-actors-spare-ready".into());
    let bind = std::env::var("DURABLE_ACTORS_HOST_BIND").unwrap_or_else(|_| "0.0.0.0:7101".into());
    let listener = TcpListener::bind(bind).await?;
    let ipc = ActorExecutorListener::bind(&socket).await?;
    let mut javascript = spawn_javascript_process(true, &socket)?;
    let executor = tokio::time::timeout(Duration::from_secs(30), ipc.accept_warm()).await??;
    tokio::fs::write(&ready, b"ready\n").await?;
    tokio::pin!(shutdown);
    let assigned = tokio::select! {
        value = receive => value?,
        result = &mut server => anyhow::bail!("spare assignment server stopped: {result:?}"),
        status = javascript.wait() => anyhow::bail!("generic Bun executor exited: {}", status?),
        () = &mut shutdown => return Ok(()),
    };
    tokio::fs::remove_file(&ready).await?;
    let environment = assigned.environment;
    let config = ActorHostConfig::from_lookup(|name| environment.get(name).cloned())?;
    ensure!(
        config.actor.is_some(),
        "spare assignment must name exactly one actor"
    );
    let entrypoint = environment
        .get("DURABLE_ACTORS_ENTRYPOINT")
        .context("missing customer entrypoint")?
        .clone();
    ensure!(
        entrypoint.starts_with("/customer/")
            && entrypoint.ends_with(".mjs")
            && !Path::new(&entrypoint)
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "customer entrypoint must be a compiled module under /customer"
    );
    let warm = WarmHost {
        readiness: Some(assigned.ready),
        listener,
        executor,
        javascript,
        entrypoint,
    };
    serve_assigned_host(config, Some(warm), shutdown).await
}

#[cfg(test)]
#[path = "../../tests/unit/host/spare.rs"]
mod tests;
