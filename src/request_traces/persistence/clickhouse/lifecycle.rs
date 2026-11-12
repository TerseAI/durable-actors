use super::*;
use crate::actor_lifecycle::{LifecycleEvent, LifecyclePersistence};

#[async_trait]
impl LifecyclePersistence for ClickHouseTracePersistence {
    /// The collector stores the reports the control plane emits.
    async fn record(&self, _event: &LifecycleEvent) -> Result<()> {
        Ok(())
    }

    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(
                    "SELECT argMaxMerge(event) AS event_json FROM actor_inventory
             WHERE project_id = {project:String} GROUP BY actor_name, actor_id
             ORDER BY actor_name, actor_id",
                )
                .param("project", project)
                .fetch_all::<InventoryRow>(),
        )
        .await??;
        rows.into_iter()
            .map(|row| Ok(serde_json::from_str(&row.event_json)?))
            .collect()
    }

    async fn history(
        &self,
        project: &str,
        actor_name: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<LifecycleEvent>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(include_str!("residency.sql"))
                .param("project", project)
                .param("actor_name", actor_name)
                .param("from", from)
                .param("to", to)
                .fetch_all::<InventoryRow>(),
        )
        .await??;
        rows.into_iter()
            .map(|row| Ok(serde_json::from_str(&row.event_json)?))
            .collect()
    }
}

#[derive(Row, Deserialize)]
struct InventoryRow {
    event_json: String,
}
