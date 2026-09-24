use super::super::pagination::Replay;
use super::*;

pub(super) async fn query(
    database: &PostgresDatabase,
    project: &str,
    query: &ReplayQuery,
) -> Result<TracePage> {
    let mut client = database.connection().await?;
    let transaction = transaction(&mut client, true).await?;
    let (metadata, _) = metadata(&transaction, project).await?;
    let page = Replay::new(project, query, metadata)?;
    let order = if page.after.is_some() {
        "position ASC"
    } else {
        "started_at_ms DESC, position DESC"
    };
    let rows = transaction.query(&format!(
        "SELECT position, event FROM durable_actors_traces WHERE project_id = $1 AND position > $2 AND position <= $3 ORDER BY {order} LIMIT $4"
    ), &[&project, &(page.after.unwrap_or(0) as i64), &(page.head() as i64), &((query.limit + 1) as i64)]).await?;
    page.finish(query.limit, records(rows)?)
}
