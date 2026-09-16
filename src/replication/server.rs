use std::sync::Arc;

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    routing::get,
};

use crate::state_log::StateSnapshot;

use super::{ReplicaAccess, ReplicaStore, access::AccessQuery};

#[derive(Clone)]
struct ReplicaServer {
    store: Arc<ReplicaStore>,
    access: ReplicaAccess,
    host_id: String,
}

pub fn replica_router(store: Arc<ReplicaStore>, access: ReplicaAccess, host_id: String) -> Router {
    Router::new()
        .route("/_replica/state", get(read).put(write))
        .route("/health", get(|| async { StatusCode::OK }))
        .layer(DefaultBodyLimit::max(32 * 1024 * 1024))
        .with_state(ReplicaServer {
            store,
            access,
            host_id,
        })
}

async fn read(
    State(server): State<ReplicaServer>,
    Query(query): Query<AccessQuery>,
) -> Result<Bytes, StatusCode> {
    let grant = server
        .access
        .verify(&query.token, "GET")
        .map_err(|_| StatusCode::FORBIDDEN)?;
    if grant.host_id != server.host_id {
        return Err(StatusCode::FORBIDDEN);
    }
    server
        .store
        .read(&grant.object)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Bytes::from)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn write(
    State(server): State<ReplicaServer>,
    Query(query): Query<AccessQuery>,
    bytes: Bytes,
) -> Result<StatusCode, StatusCode> {
    let grant = server
        .access
        .verify(&query.token, "PUT")
        .map_err(|_| StatusCode::FORBIDDEN)?;
    if grant.host_id != server.host_id || grant.archive_url.is_empty() {
        return Err(StatusCode::FORBIDDEN);
    }
    StateSnapshot::decode(&bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    server
        .store
        .put(&grant.object, &grant.archive_url, &bytes)
        .await
        .map_err(|error| {
            tracing::warn!(event = "replica_write_failed", object = %grant.object, %error);
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    Ok(StatusCode::CREATED)
}
