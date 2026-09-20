use std::sync::{Arc, OnceLock};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use super::{ReplicaAccess, ReplicaScope, ReplicaStore, server::ReplicaBinding};
use crate::clock::SystemClock;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReplicaAssignment {
    pub host_id: String,
    pub scope: ReplicaScope,
    pub secret: String,
}

struct AssignmentState {
    authorization: String,
    store: Arc<dyn ReplicaStore>,
    binding: Arc<OnceLock<ReplicaBinding>>,
    assigned: Mutex<Option<ReplicaAssignment>>,
}

pub(super) fn routers(store: Arc<dyn ReplicaStore>, token: String) -> (Router, Router) {
    let binding = Arc::new(OnceLock::new());
    let storage = super::server::routes(store.clone(), binding.clone());
    let assignment = Router::new()
        .route("/assign", post(assign))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(Arc::new(AssignmentState {
            authorization: format!("Bearer {token}"),
            store,
            binding,
            assigned: Mutex::new(None),
        }));
    (storage, assignment)
}

async fn assign(
    State(state): State<Arc<AssignmentState>>,
    headers: HeaderMap,
    Json(assignment): Json<ReplicaAssignment>,
) -> Result<StatusCode, StatusCode> {
    let supplied = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !bool::from(state.authorization.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    assignment
        .scope
        .actor
        .validate()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if assignment.host_id.is_empty()
        || assignment.scope.host.as_str().is_empty()
        || assignment.scope.session.is_empty()
        || assignment.secret.len() < 32
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut assigned = state.assigned.lock().await;
    if let Some(existing) = assigned.as_ref() {
        return if *existing == assignment {
            Ok(StatusCode::NO_CONTENT)
        } else {
            Err(StatusCode::CONFLICT)
        };
    }
    state
        .store
        .initialize_session(&assignment.scope.identity())
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    state
        .binding
        .set(ReplicaBinding {
            access: ReplicaAccess::new(&assignment.secret, Arc::new(SystemClock)),
            host_id: assignment.host_id.clone(),
            scope: assignment.scope.clone(),
        })
        .map_err(|_| StatusCode::CONFLICT)?;
    *assigned = Some(assignment);
    Ok(StatusCode::NO_CONTENT)
}
