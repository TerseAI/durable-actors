use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::{ActorKey, ActorScope},
    actor_state::ActorStorageKey,
    placement::{ObjectPlacement, ObjectPlacementStore},
    state_log::StateSnapshot,
    state_transport::StateTransport,
    storage_urls::{StorageUrlSigner, validate_snapshot_object_name},
};

use super::{
    admin::AdminService,
    public_api::{ActorPath, ApiError, authorized_admin},
};

pub(super) fn router(inspector: ActorInspector, admin: AdminService) -> Router {
    Router::new()
        .route("/v1/durability", get(durability))
        .route("/v1/objects", get(list_objects))
        .route(
            "/v1/actors/{actor_type}/{actor_id}/state",
            get(inspect_object),
        )
        .route(
            "/v1/namespaces/{namespace_id}/actors/{actor_type}/{actor_id}/state",
            get(inspect_object),
        )
        .with_state(InspectionApi { inspector, admin })
}

#[derive(Clone)]
struct InspectionApi {
    inspector: ActorInspector,
    admin: AdminService,
}

async fn durability(
    State(state): State<InspectionApi>,
    headers: HeaderMap,
) -> Result<Json<crate::replication::DurabilityPolicy>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    Ok(Json(state.inspector.storage.durability()))
}

async fn list_objects(
    State(state): State<InspectionApi>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    query.validate().map_err(ApiError::bad_request)?;
    let page = state
        .inspector
        .list(&query)
        .await
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(page)).into_response())
}

async fn inspect_object(
    State(state): State<InspectionApi>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let actor = path.into_actor(&state.admin.default_namespace);
    actor.validate().map_err(ApiError::bad_request)?;
    let object = tokio::time::timeout(Duration::from_secs(25), state.inspector.inspect(&actor))
        .await
        .map_err(|_| ApiError::unavailable("Object inspection timed out"))?
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "Object not found"))?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(object)).into_response())
}

#[derive(Clone)]
pub(super) struct ActorInspector {
    placements: Arc<dyn ObjectPlacementStore>,
    storage: Arc<dyn StorageUrlSigner>,
    transport: Arc<dyn StateTransport>,
}

impl ActorInspector {
    pub(super) fn new(
        placements: Arc<dyn ObjectPlacementStore>,
        storage: Arc<dyn StorageUrlSigner>,
        transport: Arc<dyn StateTransport>,
    ) -> Self {
        Self {
            placements,
            storage,
            transport,
        }
    }

    async fn list(&self, query: &ListQuery) -> Result<ObjectPage> {
        let mut placements = self
            .placements
            .list_committed(
                query.namespace.as_deref(),
                query.after.as_deref(),
                query.limit + 1,
            )
            .await?;
        let has_more = placements.len() > query.limit as usize;
        placements.truncate(query.limit as usize);
        let next_cursor = has_more.then(|| placements.last().unwrap().object.as_str().to_owned());
        let objects = placements
            .iter()
            .map(|placement| {
                Ok(SavedObject::new(
                    actor_from_placement(placement)?,
                    placement,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ObjectPage {
            objects,
            next_cursor,
        })
    }

    async fn inspect(&self, actor: &ActorKey) -> Result<Option<ObjectInspection>> {
        let Some(placement) = self.placements.get(&actor.storage_key()).await? else {
            return Ok(None);
        };
        let state = if placement.state_version == 0 {
            None
        } else {
            let stored_actor = actor_from_placement(&placement)?;
            ensure!(
                stored_actor == *actor,
                "committed actor identity does not match the requested object"
            );
            Some(self.read_state(&placement).await?)
        };
        Ok(Some(ObjectInspection {
            object: SavedObject::new(actor.clone(), &placement),
            state,
        }))
    }

    async fn read_state(&self, placement: &ObjectPlacement) -> Result<Value> {
        let object = placement
            .state_object
            .as_deref()
            .context("committed state object is missing")?;
        let url = self
            .storage
            .read_url(&placement.home_region, object)
            .await?;
        let snapshot = StateSnapshot::decode(&self.transport.read(&url).await?)?;
        ensure!(
            snapshot.state_version == placement.state_version
                && Some(snapshot.request_id.as_str()) == placement.last_request_id.as_deref()
                && snapshot.owner_epoch <= placement.owner_epoch,
            "snapshot does not match committed state"
        );
        Ok(snapshot.state)
    }
}

fn actor_from_placement(placement: &ObjectPlacement) -> Result<ActorKey> {
    let object = placement
        .state_object
        .as_deref()
        .context("committed state object is missing")?;
    let mut components = object.split('/').skip(1);
    let actor = ActorKey {
        namespace_id: components
            .next()
            .context("snapshot namespace is missing")?
            .to_owned(),
        actor_type: components
            .nth(1)
            .context("snapshot actor type is missing")?
            .to_owned(),
        actor_id: components
            .next()
            .context("snapshot actor ID is missing")?
            .to_owned(),
    };
    actor.validate()?;
    validate_snapshot_object_name(&actor, placement.state_version, object)?;
    ensure!(
        actor.storage_key() == placement.object,
        "snapshot identity does not match the object"
    );
    Ok(actor)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    namespace: Option<String>,
    after: Option<String>,
    #[serde(default = "page_size")]
    limit: u32,
}

impl ListQuery {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=500).contains(&self.limit),
            "limit must be between 1 and 500"
        );
        if let Some(namespace_id) = &self.namespace {
            ActorScope {
                namespace_id: namespace_id.clone(),
            }
            .validate()?;
        }
        if let Some(after) = &self.after {
            ActorStorageKey::new(after).validate()?;
        }
        Ok(())
    }
}

fn page_size() -> u32 {
    100
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectPage {
    objects: Vec<SavedObject>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedObject {
    namespace_id: String,
    actor_type: String,
    actor_id: String,
    object_id: String,
    home_region: String,
    state_version: u64,
    state_object: Option<String>,
    last_request_id: Option<String>,
}

impl SavedObject {
    fn new(actor: ActorKey, placement: &ObjectPlacement) -> Self {
        Self {
            namespace_id: actor.namespace_id,
            actor_type: actor.actor_type,
            actor_id: actor.actor_id,
            object_id: placement.object.as_str().to_owned(),
            home_region: placement.home_region.clone(),
            state_version: placement.state_version,
            state_object: placement.state_object.clone(),
            last_request_id: placement.last_request_id.clone(),
        }
    }
}

#[derive(Serialize)]
struct ObjectInspection {
    #[serde(flatten)]
    object: SavedObject,
    state: Option<Value>,
}
