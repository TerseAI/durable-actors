use super::*;
use crate::{
    actor_lifecycle::{
        LifecycleEvent, LifecyclePersistence, STALE_AFTER_MS, instance_overview,
        residency::{RUN_LIMIT, ResidencyRun},
    },
    placement::{
        ActorResidency, ActorTypesPage, ActorTypesQuery, InstancePage, InstancePageQuery,
        InstanceRow, InstanceSort, ObservedActorType, ResidencyQuery,
    },
};

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

    async fn instance_page(
        &self,
        project: &str,
        query: &InstancePageQuery,
        now: u64,
        sockets: &std::collections::BTreeMap<String, u64>,
    ) -> Result<InstancePage> {
        let order = instance_order(query);
        let sql =
            inventory_query(include_str!("instance_page.sql")).replace("/* page order */", &order);
        let sockets = serde_json::to_string(
            &sockets
                .iter()
                .map(|(id, count)| (&query.actor_name, id, count))
                .collect::<Vec<_>>(),
        )?;
        let row = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(&sql)
                .param("project", project)
                .param("actor_name", &query.actor_name)
                .param("actor_id", query.actor_id.as_deref().unwrap_or_default())
                .param("status", status_filter(query.status))
                .param("search", &query.search)
                .param("now", now)
                .param("stale", STALE_AFTER_MS)
                .param("sockets", sockets)
                .param("limit", query.limit)
                .param("offset", query.offset)
                .fetch_one::<InstancePageRow>(),
        )
        .await??;
        let (live, dormant, unknown, total) = row.counts;
        let mut instances = row
            .instances
            .into_iter()
            .map(|(id, event, count)| {
                if event.is_empty() {
                    return Ok(InstanceRow::socket_only(id, count));
                }
                let event = serde_json::from_str(&event)?;
                Ok(InstanceRow::from_overview(
                    instance_overview(event, now),
                    count,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        query.sort_rows(&mut instances);
        Ok(InstancePage {
            live,
            dormant,
            unknown,
            total,
            instances,
            methods: None,
        })
    }

    async fn actor_types_page(
        &self,
        project: &str,
        query: &ActorTypesQuery,
        now: u64,
        sockets: &std::collections::BTreeMap<(String, String), u64>,
        deployed: &[String],
    ) -> Result<ActorTypesPage> {
        let sql = inventory_query(include_str!("actor_types_page.sql"));
        let sockets = serde_json::to_string(
            &sockets
                .iter()
                .map(|((name, id), count)| (name, id, count))
                .collect::<Vec<_>>(),
        )?;
        let row = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(&sql)
                .param("project", project)
                .param("actor_name", "")
                .param("actor_id", "")
                .param("now", now)
                .param("stale", STALE_AFTER_MS)
                .param("sockets", sockets)
                .param("deployed", serde_json::to_string(deployed)?)
                .param("limit", query.limit)
                .param("offset", query.offset)
                .fetch_one::<ActorTypesPageRow>(),
        )
        .await??;
        let (live, dormant, total, largest) = row.counts;
        let mut page = ActorTypesPage::from_types(
            row.actors
                .into_iter()
                .map(|(actor_name, live, dormant, unknown)| ObservedActorType {
                    actor_name,
                    live,
                    dormant,
                    unknown,
                })
                .collect(),
            &ActorTypesQuery {
                offset: 0,
                limit: query.limit,
            },
        );
        page.live = live;
        page.dormant = dormant;
        page.total = total;
        page.largest = largest;
        Ok(page)
    }

    /// One more run than the limit keeps, so truncation shows.
    async fn residency_runs(
        &self,
        project: &str,
        query: &ResidencyQuery,
    ) -> Result<Vec<ResidencyRun>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.client
                .query(include_str!("residency.sql"))
                .param("project", project)
                .param("actor_name", &query.actor_name)
                .param("actor_id", query.actor_id.as_deref().unwrap_or_default())
                .param("from", query.from_ms)
                .param("to", query.to_ms)
                .param("stale", STALE_AFTER_MS)
                .param("limit", RUN_LIMIT + 1)
                .fetch_all::<RunRow>(),
        )
        .await??;
        Ok(rows.into_iter().map(ResidencyRun::from).collect())
    }
}

fn inventory_query(sql: &str) -> String {
    sql.replace(
        "/* inventory snapshot */",
        include_str!("inventory_snapshot.sql").trim_end(),
    )
}

#[derive(Row, Deserialize)]
struct ActorTypesPageRow {
    counts: (u64, u64, u64, u64),
    actors: Vec<(String, u64, u64, u64)>,
}

fn instance_order(query: &InstancePageQuery) -> String {
    let direction = if query.descending { "DESC" } else { "ASC" };
    let column = match query.sort {
        InstanceSort::Instance => "actor_id",
        InstanceSort::Sockets => "socket_count",
        InstanceSort::LastActive => "last_active",
        InstanceSort::Disk => "disk",
    };
    format!("{column} {direction} NULLS LAST, actor_id ASC")
}

fn status_filter(status: Option<ActorResidency>) -> &'static str {
    match status {
        Some(ActorResidency::Live) => "live",
        Some(ActorResidency::Dormant) => "dormant",
        Some(ActorResidency::Unknown) => "unknown",
        None => "",
    }
}

#[derive(Row, Deserialize)]
struct InstancePageRow {
    counts: (u64, u64, u64, u64),
    instances: Vec<(String, String, u64)>,
}

#[derive(Row, Deserialize)]
struct InventoryRow {
    event_json: String,
}

#[derive(Row, Deserialize)]
struct RunRow {
    actor_id: String,
    first_epoch: u64,
    first_sequence: u64,
    run_status: u8,
    started_at_ms: u64,
    last_at_ms: u64,
    last_stopped: bool,
}

impl From<RunRow> for ResidencyRun {
    fn from(row: RunRow) -> Self {
        Self {
            actor_id: row.actor_id,
            owner_epoch: row.first_epoch,
            sequence: row.first_sequence,
            status: match row.run_status {
                1 => ActorResidency::Live,
                2 => ActorResidency::Dormant,
                _ => ActorResidency::Unknown,
            },
            started_at_ms: row.started_at_ms,
            last_at_ms: row.last_at_ms,
            stopped: row.last_stopped,
        }
    }
}
