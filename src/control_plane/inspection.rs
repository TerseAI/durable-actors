use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    placement::{ObjectPlacement, ObjectPlacementStore},
    state_log::StateSnapshot,
    storage::{SnapshotReader, validate_snapshot_object_name},
};

use super::{
    admin::AdminService,
    public_api::{ActorPath, ApiError, authorized_admin},
};

pub(super) fn router(inspector: ActorInspector, admin: AdminService) -> Router {
    Router::new()
        .route("/v1/observe/actors", get(actor_inventory))
        .route("/v1/observe/events", get(actor_events))
        .route(
            "/v1/observe/query",
            post(observe_query).layer(axum::extract::DefaultBodyLimit::max(65_536)),
        )
        .route("/v1/observe/requests/events", get(request_events))
        .route("/v1/actors", get(list_objects))
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}",
            get(inspect_object),
        )
        .with_state(InspectionApi { inspector, admin })
}

#[derive(Clone)]
struct InspectionApi {
    inspector: ActorInspector,
    admin: AdminService,
}

async fn list_objects(
    State(state): State<InspectionApi>,
    headers: HeaderMap,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Query(query) = query.map_err(ApiError::bad_request)?;
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
    query: Result<Query<InspectQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Query(query) = query.map_err(ApiError::bad_request)?;
    query.validate().map_err(ApiError::bad_request)?;
    let actor = path.into_actor();
    actor.validate().map_err(ApiError::bad_request)?;
    let object = tokio::time::timeout(
        Duration::from_secs(25),
        state.inspector.inspect(&actor, query.include.is_some()),
    )
    .await
    .map_err(|_| ApiError::unavailable("Object inspection timed out"))?
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "Object not found"))?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(object)).into_response())
}

#[derive(Clone)]
pub(super) struct ActorInspector {
    traces: crate::request_traces::TraceStore,
    inventory: Arc<dyn crate::placement::ActorInventoryReader>,
    changes: tokio::sync::watch::Sender<()>,
    placements: Arc<dyn ObjectPlacementStore>,
    storage: Arc<dyn SnapshotReader>,
}

impl ActorInspector {
    pub(super) fn new(
        placements: Arc<dyn ObjectPlacementStore>,
        storage: Arc<dyn SnapshotReader>,
        inventory: Arc<dyn crate::placement::ActorInventoryReader>,
        changes: tokio::sync::watch::Sender<()>,
    ) -> Self {
        Self {
            traces: crate::request_traces::TraceStore::default(),
            inventory,
            changes,
            placements,
            storage,
        }
    }

    pub(super) fn with_traces(mut self, traces: crate::request_traces::TraceStore) -> Self {
        self.traces = traces;
        self
    }

    async fn list(&self, query: &ListQuery) -> Result<ObjectPage> {
        let mut placements = self
            .placements
            .list_committed(query.after.as_deref(), query.limit + 1)
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
            actors: objects,
            next_cursor,
        })
    }

    async fn inspect(
        &self,
        actor: &ActorKey,
        include_state: bool,
    ) -> Result<Option<ObjectInspection>> {
        let Some(placement) = self.placements.get(&actor.storage_key()).await? else {
            return Ok(None);
        };
        let state = if !include_state {
            None
        } else if placement.state_version == 0 {
            Some(Value::Null)
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
        let snapshot = StateSnapshot::decode(
            &self
                .storage
                .read_snapshot(&placement.home_region, object)
                .await?,
        )?;
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
    let actor = crate::storage_paths::actor_from_snapshot(object)?;
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
    actors: Vec<SavedObject>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedObject {
    actor_name: String,
    actor_id: String,
    home_region: String,
    state_version: u64,
    last_request_id: Option<String>,
}

impl SavedObject {
    fn new(actor: ActorKey, placement: &ObjectPlacement) -> Self {
        Self {
            actor_name: actor.actor_name,
            actor_id: actor.actor_id,
            home_region: placement.home_region.clone(),
            state_version: placement.state_version,
            last_request_id: placement.last_request_id.clone(),
        }
    }
}

#[derive(Serialize)]
struct ObjectInspection {
    #[serde(flatten)]
    object: SavedObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectQuery {
    include: Option<String>,
}

impl InspectQuery {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.include.as_deref().is_none_or(|value| value == "state"),
            "include must be state"
        );
        Ok(())
    }
}

async fn actor_inventory(
    State(state): State<InspectionApi>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let inventory = tokio::time::timeout(Duration::from_secs(25), read_inventory(&state))
        .await
        .map_err(|_| ApiError::unavailable("Actor inventory timed out"))?
        .map_err(ApiError::internal)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "actors": inventory })),
    )
        .into_response())
}

async fn read_inventory(state: &InspectionApi) -> Result<Vec<crate::placement::ActorInventory>> {
    let mut rows: std::collections::BTreeMap<_, _> = state
        .inspector
        .inventory
        .actor_inventory()
        .await?
        .into_iter()
        .map(|row| (row.actor_name.clone(), row))
        .collect();
    if let Some(contract) = state.admin.deployment_contract("default", None).await? {
        if let Some(actors) = contract.contract["actors"].as_array() {
            for actor in actors {
                if let Some(name) = actor["actorName"].as_str() {
                    rows.entry(name.to_owned()).or_insert_with(|| {
                        crate::placement::ActorInventory {
                            actor_name: name.to_owned(),
                            ..Default::default()
                        }
                    });
                }
            }
        }
    }
    Ok(rows.into_values().collect())
}

async fn actor_events(
    State(state): State<InspectionApi>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use std::convert::Infallible;
    use tokio_stream::wrappers::ReceiverStream;

    authorized_admin(&state.admin, &headers)?;
    let mut changes = state.inspector.changes.subscribe();
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(1);
    tokio::spawn(async move {
        let mut previous = None;
        loop {
            let result = tokio::select! {
                _ = sender.closed() => return,
                result = tokio::time::timeout(Duration::from_secs(25), read_inventory(&state)) => result,
            };
            let data = match result {
                Ok(Ok(actors)) => serde_json::json!({"actors": actors}).to_string(),
                _ => {
                    let _ = sender
                        .send(Ok(Event::default()
                            .event("error")
                            .data("Inventory unavailable")))
                        .await;
                    return;
                }
            };
            if previous.as_ref() != Some(&data) {
                if sender
                    .send(Ok(Event::default().event("inventory").data(data.clone())))
                    .await
                    .is_err()
                {
                    return;
                }
                previous = Some(data);
            }
            tokio::select! {
                _ = sender.closed() => return,
                _ = tokio::time::sleep(Duration::from_secs(15)) => {},
                _ = changes.changed() => {},
            }
        }
    });
    Ok((
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Sse::new(ReceiverStream::new(receiver))
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(10))),
    )
        .into_response())
}

async fn observe_query(
    State(state): State<InspectionApi>,
    headers: HeaderMap,
    body: Result<
        Json<crate::request_traces::query::SqlQuery>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(query) = body.map_err(ApiError::bad_request)?;
    query.validate().map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .query(&query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

fn trace_query_error(error: anyhow::Error) -> ApiError {
    if error.is::<crate::request_traces::replay::InvalidTraceCursor>()
        || error.is::<crate::request_traces::query::SqlQueryError>()
    {
        ApiError::bad_request(error)
    } else {
        ApiError::internal(error)
    }
}

#[derive(Default, Deserialize)]
struct RequestReplay {
    after: Option<String>,
}

async fn request_events(
    State(state): State<InspectionApi>,
    query: Result<Query<RequestReplay>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    use crate::request_traces::replay::ReplayQuery;
    use axum::response::sse::{KeepAlive, Sse};
    use tokio_stream::wrappers::ReceiverStream;
    authorized_admin(&state.admin, &headers)?;
    let Query(replay) = query.map_err(ApiError::bad_request)?;
    let cursor = replay.after.or_else(|| {
        headers
            .get("last-event-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    });
    let query = ReplayQuery {
        cursor,
        ..Default::default()
    };
    query.validate().map_err(ApiError::bad_request)?;
    let store = state.inspector.traces;
    // Subscribe before reading so a commit between the read and wait is not missed.
    let changes = store.changes.subscribe();
    let page = store.replay(&query).await.map_err(trace_query_error)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(stream_requests(store, changes, sender, page));
    Ok((
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Sse::new(ReceiverStream::new(receiver))
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(10))),
    )
        .into_response())
}

async fn stream_requests(
    store: crate::request_traces::TraceStore,
    mut changes: tokio::sync::watch::Receiver<()>,
    sender: tokio::sync::mpsc::Sender<Result<axum::response::sse::Event, std::convert::Infallible>>,
    mut page: crate::request_traces::TracePage,
) {
    use crate::request_traces::replay::ReplayQuery;
    use axum::response::sse::Event;
    loop {
        let more = page.next_cursor.is_some();
        let query = ReplayQuery {
            cursor: Some(page.resume_cursor.clone()),
            ..Default::default()
        };
        let event = Event::default()
            .event("requests")
            .id(&page.resume_cursor)
            .json_data(&page)
            .expect("serializable trace page");
        if sender.send(Ok(event)).await.is_err() {
            return;
        }
        if !more {
            tokio::select! {
                _ = sender.closed() => return,
                _ = changes.changed() => {},
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            }
        }
        page = match store.replay(&query).await {
            Ok(page) => page,
            Err(error) => {
                tracing::error!(%error, "request trace query failed");
                let _ = sender
                    .send(Ok(Event::default()
                        .event("error")
                        .data("Request history unavailable")))
                    .await;
                return;
            }
        };
    }
}
