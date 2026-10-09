use anyhow::Result;
use axum::{
    Router,
    extract::{Request, State},
};
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};
use tokio::net::TcpListener;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tower::ServiceExt;

pub(super) struct HostServer {
    pub address: SocketAddr,
    routes: Arc<RwLock<Router>>,
    listener: Option<TcpListener>,
    task: Option<AbortOnDropHandle<std::io::Result<()>>>,
    stop: CancellationToken,
}

impl HostServer {
    pub fn new(listener: TcpListener, routes: Router) -> Self {
        let address = listener
            .local_addr()
            .expect("bound listener has an address");
        let routes = Arc::new(RwLock::new(routes));
        let router = Router::new().fallback(dispatch).with_state(routes.clone());
        let stop = CancellationToken::new();
        let shutdown = stop.clone().cancelled_owned();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown)
                .await
        }));
        Self {
            address,
            routes,
            listener: None,
            task: Some(task),
            stop,
        }
    }

    pub fn bound(listener: TcpListener) -> Self {
        Self {
            address: listener
                .local_addr()
                .expect("bound listener has an address"),
            listener: Some(listener),
            routes: Arc::new(RwLock::new(Router::new())),
            task: None,
            stop: CancellationToken::new(),
        }
    }

    pub fn install(&self, routes: Router) -> Result<()> {
        *self.routes.write().expect("host routes poisoned") = routes;
        Ok(())
    }

    pub async fn stopped(&mut self) -> Result<()> {
        self.task.as_mut().expect("running warm server").await??;
        Ok(())
    }

    pub async fn serve(mut self, stop: CancellationToken) -> Result<()> {
        if let Some(listener) = self.listener.take() {
            let routes = self.routes.read().expect("host routes poisoned").clone();
            axum::serve(listener, routes)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await?;
            return Ok(());
        }
        let mut task = self.task.take().expect("running warm server");
        tokio::select! {
            result = &mut task => { result??; },
            () = stop.cancelled() => {
                self.stop.cancel();
                task.await??;
            }
        }
        Ok(())
    }
}

async fn dispatch(
    State(routes): State<Arc<RwLock<Router>>>,
    request: Request,
) -> axum::response::Response {
    let router = routes.read().expect("host routes poisoned").clone();
    match router.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/server.rs"]
mod tests;
