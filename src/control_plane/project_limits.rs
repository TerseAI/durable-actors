use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::postgres::PostgresDatabase;

mod memory;
mod redis;
pub(super) use memory::MemoryRateLimits;
pub(super) use redis::RedisRateLimits;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectLimits {
    pub starts_per_second: u32,
    pub start_burst: u32,
}

impl Default for ProjectLimits {
    fn default() -> Self {
        Self {
            starts_per_second: 50,
            start_burst: 100,
        }
    }
}

impl ProjectLimits {
    pub(super) fn from_lookup(
        get: &mut impl FnMut(&str) -> Option<String>,
        enabled: bool,
    ) -> Result<Option<Self>> {
        let enabled = get("DURABLE_ACTORS_PROJECT_LIMITS_ENABLED")
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("DURABLE_ACTORS_PROJECT_LIMITS_ENABLED must be boolean")?
            .unwrap_or(enabled);
        if !enabled {
            return Ok(None);
        }
        let defaults = Self::default();
        Ok(Some(Self {
            starts_per_second: positive(
                get,
                "DURABLE_ACTORS_PROJECT_ACTOR_STARTS_PER_SECOND",
                defaults.starts_per_second,
            )?,
            start_burst: positive(
                get,
                "DURABLE_ACTORS_PROJECT_ACTOR_START_BURST",
                defaults.start_burst,
            )?,
        }))
    }
}

pub(super) fn local_rate_limiter(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<Arc<ProjectRateLimiter>>> {
    Ok(ProjectLimits::from_lookup(get, false)?.map(|limits| {
        Arc::new(ProjectRateLimiter::new(
            limits,
            Arc::new(MemoryRateLimits::new(Arc::new(crate::clock::SystemClock))),
        ))
    }))
}

pub(super) struct ProjectRateLimiter {
    defaults: ProjectLimits,
    database: Option<PostgresDatabase>,
    store: Arc<dyn RateLimitStore>,
}

impl ProjectRateLimiter {
    pub fn new(defaults: ProjectLimits, store: Arc<dyn RateLimitStore>) -> Self {
        Self {
            defaults,
            database: None,
            store,
        }
    }

    pub fn with_database(mut self, database: PostgresDatabase) -> Self {
        self.database = Some(database);
        self
    }

    pub async fn check(&self, project: &str) -> Result<()> {
        let allowed = async { self.store.allow(project, self.limits(project).await?).await }
            .await
            .map_err(StartLimitError::Unavailable)?;
        ensure!(allowed, StartLimitError::Limited);
        Ok(())
    }

    async fn limits(&self, project: &str) -> Result<ProjectLimits> {
        let Some(database) = &self.database else {
            return Ok(self.defaults);
        };
        let row = database.query_opt(
            "SELECT actor_starts_per_second, actor_start_burst FROM durable_actors_project_limits WHERE project_id = $1", &[&project],
        ).await?;
        let Some(row) = row else {
            return Ok(self.defaults);
        };
        Ok(ProjectLimits {
            starts_per_second: row
                .get::<_, Option<i64>>(0)
                .map(u32::try_from)
                .transpose()?
                .unwrap_or(self.defaults.starts_per_second),
            start_burst: row
                .get::<_, Option<i64>>(1)
                .map(u32::try_from)
                .transpose()?
                .unwrap_or(self.defaults.start_burst),
        })
    }
}

#[async_trait]
pub(super) trait RateLimitStore: Send + Sync {
    async fn allow(&self, project: &str, limits: ProjectLimits) -> Result<bool>;
}

#[derive(Debug)]
pub(super) enum StartLimitError {
    Limited,
    Unavailable(anyhow::Error),
}

impl std::fmt::Display for StartLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limited => f.write_str("project actor start rate limit has been reached"),
            Self::Unavailable(error) => write!(f, "project rate limiter unavailable: {error:#}"),
        }
    }
}

impl std::error::Error for StartLimitError {}

fn positive(get: &mut impl FnMut(&str) -> Option<String>, name: &str, default: u32) -> Result<u32> {
    let value = get(name)
        .map(|value| value.parse::<u32>())
        .transpose()
        .with_context(|| format!("{name} must be a positive integer"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be a positive integer");
    Ok(value)
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/project_limits.rs"]
mod tests;
