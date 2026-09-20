use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, oneshot};

use super::process::HostReadiness;

pub(super) struct Assignment {
    pub environment: HashMap<String, String>,
    pub ready: oneshot::Sender<HostReadiness>,
}

struct AssignmentState {
    authorization: String,
    pending: Mutex<Option<oneshot::Sender<Assignment>>>,
}

pub(super) fn router(token: String, pending: oneshot::Sender<Assignment>) -> Router {
    Router::new()
        .route("/assign", post(assign))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(Arc::new(AssignmentState {
            authorization: format!("Bearer {token}"),
            pending: Mutex::new(Some(pending)),
        }))
}

async fn assign(
    State(state): State<Arc<AssignmentState>>,
    headers: HeaderMap,
    Json(environment): Json<HashMap<String, String>>,
) -> Result<Json<HostReadiness>, StatusCode> {
    let supplied = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !bool::from(state.authorization.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let pending = state
        .pending
        .lock()
        .await
        .take()
        .ok_or(StatusCode::CONFLICT)?;
    let (ready, receive) = oneshot::channel();
    pending
        .send(Assignment { environment, ready })
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    receive
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::HostId;

    #[tokio::test]
    async fn assignment_is_authenticated_single_use_and_waits_for_readiness() -> anyhow::Result<()>
    {
        let (send, receive) = tokio::sync::oneshot::channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/assign", listener.local_addr()?);
        let server = tokio::spawn(async move {
            axum::serve(listener, router("secret".into(), send))
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await?;
        assert_eq!(response.status(), 401);
        let request = client
            .post(&url)
            .bearer_auth("secret")
            .json(&serde_json::json!({"actor": "one"}));
        let call = tokio::spawn(async move { request.send().await });
        let assigned = tokio::time::timeout(std::time::Duration::from_secs(1), receive).await??;
        assert_eq!(assigned.environment["actor"], "one");
        assert!(
            !call.is_finished(),
            "assignment must wait for hydration and ownership"
        );
        let duplicate = client
            .post(&url)
            .bearer_auth("secret")
            .json(&serde_json::json!({}))
            .send()
            .await?;
        assert_eq!(duplicate.status(), 409);
        assert!(
            assigned
                .ready
                .send(super::super::process::HostReadiness {
                    host_id: HostId::new("host.v3.test.one"),
                    session_id: "session".into(),
                    route: "https://host.test".into(),
                    canonical_region: "north-america-east".into(),
                    owner_epoch: 42,
                    lease: crate::host_leases::HostLease {
                        id: HostId::new("host.v3.test.one"),
                        session_id: "session".into(),
                        route: "https://host.test".into(),
                        expires_at_ms: 60_000,
                    },
                })
                .is_ok()
        );
        let reply = call.await??;
        assert_eq!(reply.status(), 200);
        assert_eq!(reply.json::<serde_json::Value>().await?["ownerEpoch"], 42);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn failed_initialization_never_reports_ready() -> anyhow::Result<()> {
        let (send, receive) = tokio::sync::oneshot::channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/assign", listener.local_addr()?);
        let server = tokio::spawn(async move {
            axum::serve(listener, router("secret".into(), send))
                .await
                .unwrap();
        });
        let request = reqwest::Client::new()
            .post(url)
            .bearer_auth("secret")
            .json(&serde_json::json!({}));
        let call = tokio::spawn(async move { request.send().await });
        drop(receive.await?);
        assert_eq!(call.await??.status(), 503);
        server.abort();
        Ok(())
    }
}
