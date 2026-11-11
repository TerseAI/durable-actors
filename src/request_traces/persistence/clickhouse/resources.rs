use super::*;
use crate::{
    actor::ActorKey,
    actor_resources::{
        KillPoint, KillReason, ResourceHistory, ResourcePersistence, SamplePoint,
        SandboxAssignment, SandboxKill, bucket_ms,
    },
};

const MAX_KILLS: u64 = 1_000;

#[async_trait]
impl ResourcePersistence for ClickHouseTracePersistence {
    async fn record_assignment(&self, assignment: SandboxAssignment) -> Result<()> {
        self.send(QueuedRow::Assignment(assignment))
    }

    async fn record_kill(&self, kill: SandboxKill) -> Result<()> {
        self.send(QueuedRow::Kill(kill))
    }

    async fn history(&self, actor: &ActorKey, from_ms: u64, to_ms: u64) -> Result<ResourceHistory> {
        let (samples, kills) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::try_join!(
                self.actor_query(include_str!("resource_samples.sql"), actor, from_ms, to_ms)
                    .param("bucket", bucket_ms(from_ms, to_ms))
                    .fetch_all::<SamplePointRow>(),
                self.actor_query(include_str!("sandbox_kills.sql"), actor, from_ms, to_ms)
                    .param("limit", MAX_KILLS)
                    .fetch_all::<KillPointRow>(),
            )
        })
        .await??;
        Ok(ResourceHistory {
            samples: samples.into_iter().map(SamplePoint::from).collect(),
            kills: kills
                .into_iter()
                .map(KillPoint::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl ClickHouseTracePersistence {
    pub(super) fn actor_query(
        &self,
        sql: &str,
        actor: &ActorKey,
        from_ms: u64,
        to_ms: u64,
    ) -> clickhouse::query::Query {
        self.client
            .query(sql)
            .param("project", &actor.project_id)
            .param("actor_name", &actor.actor_name)
            .param("actor_id", &actor.actor_id)
            .param("from", from_ms)
            .param("to", to_ms)
    }
}

#[derive(Row, Serialize)]
pub(super) struct AssignmentRow {
    project_id: String,
    actor_name: String,
    actor_id: String,
    sandbox_id: String,
    assigned_at_ms: u64,
    memory_limit_bytes: Option<u64>,
}

impl From<SandboxAssignment> for AssignmentRow {
    fn from(assignment: SandboxAssignment) -> Self {
        Self {
            project_id: assignment.actor.project_id,
            actor_name: assignment.actor.actor_name,
            actor_id: assignment.actor.actor_id,
            sandbox_id: assignment.sandbox_id,
            assigned_at_ms: assignment.assigned_at_ms,
            memory_limit_bytes: assignment.memory_limit_bytes,
        }
    }
}

#[derive(Row, Serialize)]
pub(super) struct KillRow {
    sandbox_id: String,
    killed_at_ms: u64,
    reason: String,
    message: String,
}

impl From<SandboxKill> for KillRow {
    fn from(kill: SandboxKill) -> Self {
        Self {
            sandbox_id: kill.sandbox_id,
            killed_at_ms: kill.killed_at_ms,
            reason: kill.reason.as_str().into(),
            message: kill.message.unwrap_or_default(),
        }
    }
}

#[derive(Row, Deserialize)]
struct SamplePointRow {
    at_ms: u64,
    cpu_millicores: Option<u64>,
    memory_bytes: u64,
    memory_limit_bytes: Option<u64>,
}

impl From<SamplePointRow> for SamplePoint {
    fn from(row: SamplePointRow) -> Self {
        Self {
            at_ms: row.at_ms,
            cpu_millicores: row.cpu_millicores,
            memory_bytes: row.memory_bytes,
            memory_limit_bytes: row.memory_limit_bytes,
        }
    }
}

#[derive(Row, Deserialize)]
struct KillPointRow {
    killed_at_ms: u64,
    reason: String,
    message: String,
}

impl TryFrom<KillPointRow> for KillPoint {
    type Error = anyhow::Error;

    fn try_from(row: KillPointRow) -> Result<Self> {
        Ok(Self {
            at_ms: row.killed_at_ms,
            reason: KillReason::parse(&row.reason)?,
            message: (!row.message.is_empty()).then_some(row.message),
        })
    }
}
