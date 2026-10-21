use super::*;
use crate::sandbox::StoppedSpare;

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
        self.forget(&stopped).await
    }

    async fn forget(&self, stopped: &[StoppedSpare]) -> Result<()> {
        let resources: Vec<_> = stopped
            .iter()
            .map(|spare| spare.resource_id.as_str())
            .collect();
        let rows = self.store.0.connection().await?.query(
            "DELETE FROM durable_actors_spares WHERE status IN ('ready', 'active') AND kind = $1 AND handle::json->>'resourceId' = ANY($2) \
             RETURNING host_id, handle::json->>'resourceId'",
            &[&self.config.kind.as_str(), &resources],
        ).await?;
        for row in rows {
            let Some(host) = row.get::<_, Option<String>>(0) else {
                continue;
            };
            let resource = row.get::<_, &str>(1);
            let finished_at_ms = stopped
                .iter()
                .find(|spare| spare.resource_id == resource)
                .and_then(|spare| spare.finished_at_ms);
            self.ended(host, finished_at_ms);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/pool_cleanup.rs"]
mod tests;
