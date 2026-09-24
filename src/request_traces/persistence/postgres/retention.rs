use super::*;
use tokio_util::sync::CancellationToken;

const PRUNE_BATCH_SIZE: i64 = 1000;

impl PostgresTracePersistence {
    pub(crate) fn start_retention(&self, stop: CancellationToken) {
        let store = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = stop.cancelled() => return, _ = interval.tick() => {} }
                loop {
                    let result = tokio::select! {
                        _ = stop.cancelled() => return,
                        result = store.prune_batch() => result,
                    };
                    match result {
                        Ok(count) if count == PRUNE_BATCH_SIZE as u64 => {}
                        Ok(_) => break,
                        Err(error) => {
                            tracing::warn!(%error, "actor analytics retention failed");
                            break;
                        }
                    }
                    tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(50)) => {} }
                }
            }
        });
    }

    pub(crate) async fn prune_batch(&self) -> Result<u64> {
        let mut client = self.database.connection().await?;
        let transaction = transaction(&mut client, false).await?;
        let locked: bool = transaction.query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext(current_schema()), hashtext('actor-analytics-retention'))", &[],
        ).await?.get(0);
        if !locked {
            return Ok(0);
        }
        let rows = transaction.query(
            "SELECT project_id, position FROM durable_actors_traces WHERE received_at < now() - $1::double precision * interval '1 second' ORDER BY received_at LIMIT $2",
            &[&self.retention.as_secs_f64(), &PRUNE_BATCH_SIZE],
        ).await?;
        let mut projects: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        for row in rows {
            projects.entry(row.get(0)).or_default().push(row.get(1));
        }
        let mut deleted = 0;
        for (project, positions) in projects {
            lock_project(&transaction, &project).await?;
            let row = transaction.query_one(
                "WITH deleted AS (DELETE FROM durable_actors_traces WHERE project_id = $1 AND position = ANY($2) RETURNING position) SELECT COUNT(*), MAX(position) FROM deleted",
                &[&project, &positions],
            ).await?;
            let count: i64 = row.get(0);
            let pruned: Option<i64> = row.get(1);
            transaction.execute(
                "UPDATE durable_actors_trace_projects SET pruned = GREATEST(pruned, $2), retained = retained - $3 WHERE project_id = $1",
                &[&project, &pruned, &count],
            ).await?;
            deleted += count as u64;
        }
        transaction.commit().await?;
        Ok(deleted)
    }
}
