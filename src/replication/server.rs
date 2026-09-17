use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    routing::{get, post},
};

use crate::state_log::StateSnapshot;

use super::{ReplicaAccess, ReplicaStore, access::AccessQuery};

#[derive(Clone)]
struct ReplicaServer {
    store: Arc<dyn ReplicaStore>,
    access: ReplicaAccess,
    host_id: String,
}

pub fn replica_router(
    store: Arc<dyn ReplicaStore>,
    access: ReplicaAccess,
    host_id: String,
) -> Router {
    Router::new()
        .route("/_replica/state", get(read).put(write))
        .route("/_replica/stream", post(initialize).get(head).put(append))
        .route("/_replica/seal", post(seal))
        .route("/health", get(|| async { StatusCode::OK }))
        .layer(DefaultBodyLimit::max(32 * 1024 * 1024))
        .with_state(ReplicaServer {
            store,
            access,
            host_id,
        })
}

impl ReplicaServer {
    fn stream_grant(
        &self,
        query: &AccessQuery,
        operation: &str,
    ) -> Result<super::ReplicaGrant, StatusCode> {
        let grant = self
            .access
            .verify(&query.token, operation)
            .map_err(|_| StatusCode::FORBIDDEN)?;
        if grant.host_id != self.host_id
            || grant
                .stream
                .as_ref()
                .is_none_or(|stream| stream.prefix != grant.object)
        {
            return Err(StatusCode::FORBIDDEN);
        }
        Ok(grant)
    }
}

async fn initialize(
    State(server): State<ReplicaServer>,
    Query(query): Query<AccessQuery>,
) -> Result<StatusCode, StatusCode> {
    let grant = server.stream_grant(&query, "INITIALIZE")?;
    server
        .store
        .initialize_stream(grant.stream.as_ref().unwrap())
        .await
        .map_err(unavailable)?;
    Ok(StatusCode::CREATED)
}

async fn head(
    State(server): State<ReplicaServer>,
    Query(query): Query<AccessQuery>,
) -> Result<Json<super::StreamHead>, StatusCode> {
    let grant = server.stream_grant(&query, "HEAD")?;
    server
        .store
        .stream_head(grant.stream.as_ref().unwrap())
        .await
        .map(Json)
        .map_err(unavailable)
}

async fn seal(
    State(server): State<ReplicaServer>,
    Query(query): Query<AccessQuery>,
) -> Result<Json<super::StreamHead>, StatusCode> {
    let grant = server.stream_grant(&query, "SEAL")?;
    server
        .store
        .seal(grant.stream.as_ref().unwrap())
        .await
        .map(Json)
        .map_err(unavailable)
}

async fn append(
    State(server): State<ReplicaServer>,
    Query(query): Query<AccessQuery>,
    bytes: Bytes,
) -> Result<StatusCode, StatusCode> {
    let grant = server.stream_grant(&query, "APPEND")?;
    if grant.archive_url.is_empty() {
        return Err(StatusCode::FORBIDDEN);
    }
    server
        .store
        .append(grant.stream.as_ref().unwrap(), &grant.archive_url, &bytes)
        .await
        .map_err(unavailable)?;
    Ok(StatusCode::CREATED)
}

fn unavailable(error: anyhow::Error) -> StatusCode {
    tracing::warn!(%error, "replica stream operation failed");
    StatusCode::SERVICE_UNAVAILABLE
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
