use super::*;

/// Creates the tables and views the collector writes into and the queries read, retrying until
/// ClickHouse accepts them.
pub(super) async fn create(client: Client, retention: u64, stop: CancellationToken) {
    let mut delay = Duration::from_secs(1);
    let work = async {
        loop {
            match tokio::time::timeout(Duration::from_secs(15), initialize(&client, retention))
                .await
            {
                Ok(Ok(())) => return,
                error => tracing::warn!(?error, "ClickHouse schema was not created; retrying"),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    };
    tokio::select! { _ = work => {}, _ = stop.cancelled() => {} }
}

pub(super) async fn initialize(client: &Client, retention: u64) -> Result<()> {
    let schema = include_str!("schema.sql").replace("RETENTION_SECONDS", &retention.to_string());
    for statement in schema
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
    {
        client.query(statement).execute().await?;
    }
    Ok(())
}
