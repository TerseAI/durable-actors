use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};

use super::{
    admin::AdminService,
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
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&admin, &headers)?;
    let contract = admin
        .deployment_contract(&project_id(path)?)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "no public actor contract is published for this deployment",
            )
        })?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(contract)).into_response())
}
