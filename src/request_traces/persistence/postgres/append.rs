use super::*;

pub(super) async fn insert(
    transaction: &tokio_postgres::Transaction<'_>,
    project: &str,
    head: i64,
    events: &[&TraceEvent],
) -> Result<()> {
    let payloads = events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    let timestamps = events
        .iter()
        .map(|e| i64::try_from(e.trace.started_at_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let ids: Vec<_> = events.iter().map(|e| e.event_id.as_str()).collect();
    let actors: Vec<_> = events.iter().map(|e| e.trace.actor_name.as_str()).collect();
    let actor_ids: Vec<_> = events.iter().map(|e| e.trace.actor_id.as_str()).collect();
    let outcomes: Vec<_> = events.iter().map(|e| e.trace.outcome.as_str()).collect();
    let durations: Vec<_> = events.iter().map(|e| e.trace.duration_ms).collect();
    let waits: Vec<_> = events.iter().map(|e| e.trace.queue_wait_ms).collect();
    let kinds: Vec<_> = events.iter().map(|e| e.trace.kind.as_str()).collect();
    let operations: Vec<_> = events.iter().map(|e| e.trace.operation.as_str()).collect();
    let connections: Vec<_> = events
        .iter()
        .map(|e| e.trace.connection_id.as_deref())
        .collect();
    let hosts: Vec<_> = events.iter().map(|e| e.host_id.as_str()).collect();
    transaction
        .execute(
            include_str!("append.sql"),
            &[
                &project,
                &head,
                &ids,
                &payloads,
                &timestamps,
                &actors,
                &actor_ids,
                &outcomes,
                &durations,
                &waits,
                &kinds,
                &operations,
                &connections,
                &hosts,
            ],
        )
        .await?;
    Ok(())
}
