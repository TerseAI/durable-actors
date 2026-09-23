use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;

use super::{
    admin::AdminService,
    public_api::{ApiError, authorized_admin},
};

pub(super) fn router(inspector: ActorInspector, admin: AdminService) -> Router {
    Router::new()
        .route("/v1/observe/actors", get(actor_inventory))
        .route("/v1/observe/events", get(actor_events))
        .route("/v1/observe/requests", get(request_history))
        .route("/v1/observe/metrics", get(overview_metrics))
        .route("/v1/observe/queue-waits", get(queue_waits))
        .route("/v1/observe/websockets", get(websocket_history))
        .route("/v1/observe/requests/events", get(request_events))
        .route(
            "/v1/projects/{project_id}/observe/actors",
            get(actor_inventory),
        )
        .route(
            "/v1/projects/{project_id}/observe/events",
            get(actor_events),
        )
        .route(
            "/v1/projects/{project_id}/observe/requests",
            get(request_history),
        )
        .route(
            "/v1/projects/{project_id}/observe/metrics",
            get(overview_metrics),
        )
        .route(
            "/v1/projects/{project_id}/observe/queue-waits",
            get(queue_waits),
        )
        .route(
            "/v1/projects/{project_id}/observe/websockets",
            get(websocket_history),
        )
        .route(
            "/v1/projects/{project_id}/observe/requests/events",
            get(request_events),
        )
        .with_state(InspectionApi { inspector, admin })
}

#[derive(Clone)]
struct InspectionApi {
    inspector: ActorInspector,
    admin: AdminService,
}

#[derive(Clone)]
pub(super) struct ActorInspector {
    traces: Arc<dyn crate::request_traces::reader::TraceReader>,
    trace_changes: tokio::sync::watch::Sender<()>,
    inventory: Arc<dyn crate::placement::ActorInventoryReader>,
    changes: tokio::sync::watch::Sender<()>,
}

impl ActorInspector {
    pub(super) fn new(
        inventory: Arc<dyn crate::placement::ActorInventoryReader>,
        changes: tokio::sync::watch::Sender<()>,
    ) -> Self {
        let traces = crate::request_traces::TraceStore::default();
        Self {
            trace_changes: traces.changes.clone(),
            traces: Arc::new(traces),
            inventory,
            changes,
        }
    }

    pub(super) fn with_traces(mut self, traces: crate::request_traces::TraceStore) -> Self {
        self.trace_changes = traces.changes.clone();
        self.traces = Arc::new(traces);
        self
    }
    pub(super) fn with_reader(
        mut self,
        reader: Arc<dyn crate::request_traces::reader::TraceReader>,
    ) -> Self {
        self.traces = reader;
        self
    }
}

fn project_scope(
    path: &std::collections::HashMap<String, String>,
    query: Option<String>,
) -> Result<Option<String>, ApiError> {
    if let Some(project) = path.get("project_id") {
        if query.as_ref().is_some_and(|query| query != project) {
            return Err(ApiError::bad_request("conflicting project scope"));
        }
        crate::actor::ActorKey {
            project_id: project.clone(),
            actor_name: "Actor".into(),
            actor_id: "id".into(),
        }
        .validate()
        .map_err(ApiError::bad_request)?;
        Ok(Some(project.clone()))
    } else {
        Ok(query)
    }
}

async fn actor_inventory(
    State(state): State<InspectionApi>,
    Path(path): Path<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let project = project_scope(&path, None)?;
    let inventory = tokio::time::timeout(
        Duration::from_secs(25),
        read_inventory(&state, project.as_deref()),
    )
    .await
    .map_err(|_| ApiError::unavailable("Actor inventory timed out"))?
    .map_err(ApiError::internal)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "actors": inventory })),
    )
        .into_response())
}

async fn read_inventory(
    state: &InspectionApi,
    project: Option<&str>,
) -> Result<Vec<crate::placement::ActorInventory>> {
    let mut rows: std::collections::BTreeMap<_, _> = state
        .inspector
        .inventory
        .actor_inventory(project)
        .await?
        .into_iter()
        .map(|row| (row.actor_name.clone(), row))
        .collect();
    if let Some(contract) = state
        .admin
        .deployment_contract(project.unwrap_or("default"))
        .await?
    {
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
    Path(path): Path<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use std::convert::Infallible;
    use tokio_stream::wrappers::ReceiverStream;

    authorized_admin(&state.admin, &headers)?;
    let project = project_scope(&path, None)?;
    let mut changes = state.inspector.changes.subscribe();
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(1);
    tokio::spawn(async move {
        let mut previous = None;
        loop {
            let result = tokio::select! {
                _ = sender.closed() => return,
                result = tokio::time::timeout(Duration::from_secs(25), read_inventory(&state, project.as_deref())) => result,
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

async fn overview_metrics(
    State(state): State<InspectionApi>,
    Path(path): Path<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    query: Result<Query<crate::request_traces::metrics::TimeRange>, QueryRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Query(mut query) = query.map_err(ApiError::bad_request)?;
    query.project_id = project_scope(&path, query.project_id.take())?;
    query.validate().map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .metrics(&query)
        .await
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn queue_waits(
    State(state): State<InspectionApi>,
    Path(path): Path<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    query: Result<Query<crate::request_traces::metrics::QueueWaitQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Query(mut query) = query.map_err(ApiError::bad_request)?;
    query.project_id = project_scope(&path, query.project_id.take())?;
    query.validate().map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .queue_waits(&query)
        .await
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn websocket_history(
    State(state): State<InspectionApi>,
    Path(path): Path<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    query: Result<Query<crate::request_traces::metrics::TimeRange>, QueryRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Query(mut query) = query.map_err(ApiError::bad_request)?;
    query.project_id = project_scope(&path, query.project_id.take())?;
    query.validate().map_err(ApiError::bad_request)?;
    let result = state
        .inspector
        .traces
        .websockets(&query)
        .await
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(result)).into_response())
}

async fn request_history(
    State(state): State<InspectionApi>,
    Path(path): Path<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    query: Result<Query<crate::request_traces::history::HistoryQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Query(mut query) = query.map_err(ApiError::bad_request)?;
    query.project_id = project_scope(&path, query.project_id.take())?;
    query.validate().map_err(ApiError::bad_request)?;
    let page = state
        .inspector
        .traces
        .history(&query)
        .await
        .map_err(trace_query_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(page)).into_response())
}

fn trace_query_error(error: anyhow::Error) -> ApiError {
    if error.is::<crate::request_traces::replay::InvalidTraceCursor>() {
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
    Path(path): Path<std::collections::HashMap<String, String>>,
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
        project_id: project_scope(&path, None)?,
        cursor,
        ..Default::default()
    };
    query.validate().map_err(ApiError::bad_request)?;
    let store = state.inspector.traces;
    // Subscribe before reading so a commit between the read and wait is not missed.
    let changes = state.inspector.trace_changes.subscribe();
    let page = store.replay(&query).await.map_err(trace_query_error)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(stream_requests(
        store,
        changes,
        sender,
        page,
        query.project_id,
    ));
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
    store: Arc<dyn crate::request_traces::reader::TraceReader>,
    mut changes: tokio::sync::watch::Receiver<()>,
    sender: tokio::sync::mpsc::Sender<Result<axum::response::sse::Event, std::convert::Infallible>>,
    mut page: crate::request_traces::TracePage,
    project_id: Option<String>,
) {
    use crate::request_traces::replay::ReplayQuery;
    use axum::response::sse::Event;
    loop {
        let more = page.next_cursor.is_some();
        let query = ReplayQuery {
            project_id: project_id.clone(),
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
