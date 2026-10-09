use super::*;
use crate::sandbox::StoppedSpare;

impl SparePool {
    pub(super) async fn forget_stopped(&self) -> Result<()> {
        // Capture identities before observing Kubernetes so concurrent activations are never pruned.
        let rows = self.store.0.connection().await?.query(
            "SELECT name, handle FROM durable_actors_spares WHERE status IN ('ready', 'active', 'stopping') AND kind = $1",
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
        let recorded = self.record_stops(stopped).await?;
        self.release(recorded).await
    }

    async fn record_stops(&self, stopped: Vec<StoppedSpare>) -> Result<Vec<SpareHandle>> {
        let stopped_at: std::collections::HashMap<_, _> = stopped
            .into_iter()
            .map(|spare| (spare.resource_id, spare.stopped_at_ms))
            .collect();
        let resource_ids: Vec<_> = stopped_at.keys().collect();
        let mut connection = self.store.0.connection().await?;
        let transaction = connection.transaction().await?;
        let rows = transaction.query(
            "DELETE FROM durable_actors_spares WHERE status IN ('ready', 'active', 'stopping') AND kind = $1 AND handle::json->>'resourceId' = ANY($2) RETURNING name, handle, usage_assignment",
            &[&self.config.kind.as_str(), &resource_ids],
        ).await?;
        let detected_at_ms = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
        let mut recorded = Vec::new();
        for row in &rows {
            let handle = decode_handle(row)?;
            if let Some(assignment) = row.get::<_, Option<serde_json::Value>>("usage_assignment") {
                let stopped_at_ms = stopped_at.get(&handle.resource_id).copied().flatten();
                crate::usage::UsageOutbox::enqueue_in(
                    &transaction,
                    &serde_json::from_value(assignment)?,
                    crate::usage::UsageEventType::Stopped,
                    stopped_at_ms.unwrap_or(detected_at_ms),
                )
                .await?;
            }
            recorded.push(handle);
        }
        transaction.commit().await?;
        Ok(recorded)
    }

    async fn release(&self, handles: Vec<SpareHandle>) -> Result<()> {
        use futures_util::StreamExt;
        let results = futures_util::stream::iter(handles)
            .map(|handle| async move {
                tokio::time::timeout(Duration::from_secs(30), self.provider.retire_spare(&handle))
                    .await
                    .context("spare release timed out")?
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/pool_cleanup.rs"]
mod tests;
