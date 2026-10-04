use std::{collections::HashMap, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::TryStreamExt;
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, oneshot};

use super::process::HostReadiness;

pub(super) struct Assignment {
    pub environment: HashMap<String, String>,
    pub ready: oneshot::Sender<HostReadiness>,
}

struct AssignmentState {
    authorization: Authorization,
    code_root: PathBuf,
    pending: Mutex<Option<oneshot::Sender<Assignment>>>,
}

pub(super) enum Authorization {
    Bearer(String),
    Signed(crate::control_plane::assignment::AssignmentVerifier),
}

impl From<String> for Authorization {
    fn from(token: String) -> Self {
        Self::Bearer(format!("Bearer {token}"))
    }
}

impl Authorization {
    fn accepts(&self, supplied: &[u8]) -> bool {
        match self {
            Self::Bearer(expected) => bool::from(expected.as_bytes().ct_eq(supplied)),
            Self::Signed(verifier) => std::str::from_utf8(supplied)
                .ok()
                .and_then(|value| value.strip_prefix("Bearer "))
                .is_some_and(|token| verifier.verify(token).is_ok()),
        }
    }
}

pub(super) fn router(
    authorization: Authorization,
    pending: oneshot::Sender<Assignment>,
    code_root: PathBuf,
) -> Router {
    Router::new()
        .route("/assign", post(assign))
        .route("/prepare-code", post(prepare_code))
        .layer(DefaultBodyLimit::disable())
        .with_state(Arc::new(AssignmentState {
            authorization,
            code_root,
            pending: Mutex::new(Some(pending)),
        }))
}

async fn assign(
    State(state): State<Arc<AssignmentState>>,
    headers: HeaderMap,
    Json(environment): Json<HashMap<String, String>>,
) -> Result<Json<HostReadiness>, StatusCode> {
    authorize(&state, &headers)?;
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

async fn prepare_code(
    State(state): State<Arc<AssignmentState>>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, StatusCode> {
    authorize(&state, &headers)?;
    let pending = state.pending.lock().await;
    if pending.is_none() {
        return Err(StatusCode::CONFLICT);
    }
    let artifact = headers
        .get("terse-code-artifact")
        .and_then(|header| URL_SAFE_NO_PAD.decode(header.as_bytes()).ok())
        .and_then(|bytes| serde_json::from_slice::<crate::artifacts::ArtifactFile>(&bytes).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    crate::artifacts::install_file(
        &state.code_root,
        &artifact,
        body.into_data_stream().map_err(anyhow::Error::from),
    )
    .await
    .map_err(|error| {
        tracing::warn!(%error, "code preparation failed");
        StatusCode::BAD_REQUEST
    })?;
    Ok(StatusCode::NO_CONTENT)
}

fn authorize(state: &AssignmentState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let supplied = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if state.authorization.accepts(supplied.as_bytes()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/assignment.rs"]
mod tests;
