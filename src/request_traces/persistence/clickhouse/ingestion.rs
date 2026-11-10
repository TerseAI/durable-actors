use super::*;

const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_INSERT_ROWS: usize = 1024;

pub(super) async fn run(
    client: Client,
    mut receiver: mpsc::UnboundedReceiver<Report>,
    retention: u64,
    stop: CancellationToken,
) {
    let work = async {
        retry(|| initialize(&client, retention)).await;
        while let Some(first) = receiver.recv().await {
            let mut batch = Batch::default();
            batch.push(first);
            let deadline = tokio::time::sleep(FLUSH_INTERVAL);
            tokio::pin!(deadline);
            while batch.len() < MAX_INSERT_ROWS {
                tokio::select! {
                    _ = &mut deadline => break,
                    report = receiver.recv() => match report {
                        Some(report) => batch.push(report),
                        None => break,
                    },
                }
            }
            retry(|| batch.write(&client)).await;
        }
    };
    tokio::select! {
        _ = work => {},
        _ = stop.cancelled() => {
            tracing::info!(queued_reports = receiver.len(), "ClickHouse writer stopped");
        },
    }
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

async fn retry<F, Fut>(mut operation: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delay = Duration::from_secs(1);
    loop {
        match tokio::time::timeout(WRITE_TIMEOUT, operation()).await {
            Ok(Ok(())) => return,
            error => {
                tracing::warn!(?error, "ClickHouse write failed; retaining batch for retry");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

#[derive(Default)]
struct Batch {
    lifecycle: Vec<crate::actor_lifecycle::LifecycleEvent>,
    assignments: Vec<resources::AssignmentRow>,
    kills: Vec<resources::KillRow>,
    // Released only after the batch is written, so retried rows still count against the bound.
    permits: Vec<OwnedSemaphorePermit>,
}

impl Batch {
    fn push(&mut self, report: Report) {
        match report.row {
            QueuedRow::Lifecycle(event) => self.lifecycle.push(event),
            QueuedRow::Assignment(assignment) => self
                .assignments
                .push(resources::AssignmentRow::from(assignment)),
            QueuedRow::Kill(kill) => self.kills.push(resources::KillRow::from(kill)),
        }
        self.permits.push(report.permit);
    }

    fn len(&self) -> usize {
        self.permits.len()
    }

    async fn write(&self, client: &Client) -> Result<()> {
        if !self.lifecycle.is_empty() {
            let rows = self
                .lifecycle
                .iter()
                .map(lifecycle::LifecycleRow::from_event)
                .collect::<Result<Vec<_>>>()?;
            insert(client, "actor_lifecycle", &rows).await?;
        }
        insert(client, "actor_sandboxes", &self.assignments).await?;
        insert(client, "sandbox_kills", &self.kills).await
    }
}

async fn insert<T>(client: &Client, table: &str, rows: &[T]) -> Result<()>
where
    T: clickhouse::RowOwned + clickhouse::RowWrite,
{
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<T>(table).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
