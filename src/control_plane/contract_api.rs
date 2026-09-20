use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;

use super::{
    admin::{AdminService, validate_component},
    public_api::{ApiError, ProjectPath, authorized_admin, project_id},
};

pub(super) fn router(admin: AdminService) -> Router {
    Router::new()
        .route(
            "/v1/projects/{project_id}/deployment/contract",
            get(get_contract),
        )
        .with_state(admin)
}

async fn get_contract(
    State(admin): State<AdminService>,
    path: Path<ProjectPath>,
    query: Result<Query<ContractQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&admin, &headers)?;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    if let Some(revision) = &query.revision {
        validate_component("code revision", revision, 128).map_err(ApiError::bad_request)?;
    }
    let contract = admin
        .deployment_contract(&project_id(path)?, query.revision.as_deref())
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "no public actor contract is published for this deployment revision",
            )
        })?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(contract)).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractQuery {
    revision: Option<String>,
}
