use super::*;

impl SparePool {
    pub(super) async fn forget_stopped(&self) -> Result<()> {
        // Capture identities before observing Kubernetes so concurrent activations are never pruned.
        let rows = self.store.0.connection().await?.query(
            "SELECT name, handle FROM durable_actors_spares WHERE status IN ('ready', 'active') AND kind = $1",
            &[&self.config.kind.as_str()],
        ).await?;
        let spares = rows.iter().map(decode_handle).collect::<Result<Vec<_>>>()?;
        if spares.is_empty() {
            return Ok(());
        }
        let stopped = tokio::time::timeout(
            Duration::from_secs(30),
            self.provider.stopped_spares(&spares),
        )
        .await
        .context("sandbox inspection timed out")??;
        let stopped_at: std::collections::HashMap<_, _> = stopped
            .into_iter()
            .map(|spare| (spare.resource_id, spare.stopped_at_ms))
            .collect();
        let resource_ids: Vec<_> = stopped_at.keys().collect();
        let mut connection = self.store.0.connection().await?;
        let transaction = connection.transaction().await?;
        let rows = transaction.query(
            "DELETE FROM durable_actors_spares WHERE status IN ('ready', 'active') AND kind = $1 AND handle::json->>'resourceId' = ANY($2) RETURNING usage_assignment, handle::json->>'resourceId'",
            &[&self.config.kind.as_str(), &resource_ids],
        ).await?;
        super::enqueue_stops(&transaction, &rows, |row| {
            stopped_at.get(&row.get::<_, String>(1)).copied().flatten()
        })
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/pool_cleanup.rs"]
mod tests;
