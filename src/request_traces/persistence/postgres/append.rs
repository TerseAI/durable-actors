use super::*;

pub(super) async fn insert(
    transaction: &tokio_postgres::Transaction<'_>,
    project: &str,
    head: i64,
    events: &[&TraceEvent],
) -> Result<(i64, Option<i64>)> {
    let payloads = events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    let timestamps = events
        .iter()
        .map(|event| i64::try_from(event.trace.started_at_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let outcomes = events
        .iter()
        .map(|event| serde_json::to_value(event.trace.outcome))
        .collect::<Result<Vec<_>, _>>()?;
    let kinds = events
        .iter()
        .map(|event| serde_json::to_value(event.trace.kind))
        .collect::<Result<Vec<_>, _>>()?;
    let row = transaction
        .query_one(
            include_str!("append.sql"),
            &[
                &project,
                &head,
                &events
                    .iter()
                    .map(|event| event.event_id.as_str())
                    .collect::<Vec<_>>(),
                &payloads,
                &timestamps,
                &events
                    .iter()
                    .map(|event| event.trace.actor_name.as_str())
                    .collect::<Vec<_>>(),
                &events
                    .iter()
                    .map(|event| event.trace.actor_id.as_str())
                    .collect::<Vec<_>>(),
                &outcomes
                    .iter()
                    .map(serde_json::Value::as_str)
                    .collect::<Vec<_>>(),
                &events
                    .iter()
                    .map(|event| event.trace.duration_ms)
                    .collect::<Vec<_>>(),
                &events
                    .iter()
                    .map(|event| event.trace.queue_wait_ms)
                    .collect::<Vec<_>>(),
                &kinds
                    .iter()
                    .map(serde_json::Value::as_str)
                    .collect::<Vec<_>>(),
                &events
                    .iter()
                    .map(|event| event.trace.operation.as_str())
                    .collect::<Vec<_>>(),
                &events
                    .iter()
                    .map(|event| event.trace.connection_id.as_deref())
                    .collect::<Vec<_>>(),
                &events
                    .iter()
                    .map(|event| event.host_id.as_str())
                    .collect::<Vec<_>>(),
            ],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}
