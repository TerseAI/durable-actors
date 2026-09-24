use super::super::pagination::History;
use super::*;

pub(super) async fn query(
    database: &PostgresDatabase,
    project: &str,
    query: &HistoryQuery,
) -> Result<TracePage> {
    let mut client = database.connection().await?;
    let transaction = transaction(&mut client, true).await?;
    let (metadata, retained) = metadata(&transaction, project).await?;
    let page = History::new(project, query, metadata)?;
    let outcome = query
        .outcome
        .map(serde_json::to_value)
        .transpose()?
        .and_then(|value| value.as_str().map(str::to_owned));
    let rows = transaction
        .query(
            include_str!("history.sql"),
            &[
                &project,
                &(page.watermark as i64),
                &query.actor_name,
                &query.actor_id,
                &outcome,
                &query.from_ms.map(|ms| ms as i64),
                &query.to_ms.map(|ms| ms as i64),
                &page.after.as_ref().map(|c| c.time as i64),
                &page.after.as_ref().map(|c| c.sequence as i64),
                &((query.limit + 1) as i64),
            ],
        )
        .await?;
    page.finish(query.limit, retained, records(rows)?)
}
