use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;

use crate::actor::ActorScope;

use super::{
    admin::{AdminService, validate_component},
    public_api::{ApiError, authorized_admin},
};

pub(super) fn router(admin: AdminService) -> Router {
    Router::new()
        .route("/v1/contract", get(get_contract))
        .route("/v1/namespaces/{namespace_id}/contract", get(get_contract))
        .with_state(admin)
}

async fn get_contract(
    State(admin): State<AdminService>,
    Path(path): Path<NamespacePath>,
    Query(query): Query<ContractQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&admin, &headers)?;
    let namespace = path
        .namespace_id
        .as_deref()
        .unwrap_or(&admin.default_namespace);
    ActorScope {
        namespace_id: namespace.into(),
    }
    .validate()
    .map_err(ApiError::bad_request)?;
    if let Some(revision) = &query.revision {
        validate_component("code revision", revision, 128).map_err(ApiError::bad_request)?;
    }
    let contract = admin
        .deployment_contract(namespace, query.revision.as_deref())
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "contract_not_found",
                "no public actor contract is published for this deployment revision",
            )
        })?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(contract)).into_response())
}

#[derive(Deserialize)]
struct NamespacePath {
    namespace_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractQuery {
    revision: Option<String>,
}
