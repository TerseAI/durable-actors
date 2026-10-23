use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;

use super::{ProjectLimits, RateLimitStore};
use crate::clock::Clock;

pub(in crate::control_plane) struct MemoryRateLimits {
    projects: Mutex<HashMap<String, Bucket>>,
    clock: Arc<dyn Clock>,
}

impl MemoryRateLimits {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            projects: Mutex::new(HashMap::new()),
            clock,
        }
    }
}

#[async_trait]
impl RateLimitStore for MemoryRateLimits {
    async fn allow(&self, project: &str, limits: ProjectLimits) -> Result<bool> {
        let now = self.clock.now_ms()?;
        let burst = f64::from(limits.start_burst);
        let mut projects = self.projects.lock().unwrap();
        let bucket = projects.entry(project.into()).or_insert(Bucket {
            tokens: burst,
            updated: now,
        });
        bucket.tokens = (bucket.tokens
            + now.saturating_sub(bucket.updated) as f64 * f64::from(limits.starts_per_second)
                / 1000.0)
            .min(burst);
        bucket.updated = bucket.updated.max(now);
        if bucket.tokens < 1.0 {
            return Ok(false);
        }
        bucket.tokens -= 1.0;
        Ok(true)
    }
}

struct Bucket {
    tokens: f64,
    updated: u64,
}
