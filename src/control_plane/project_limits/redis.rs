use anyhow::Result;
use async_trait::async_trait;

use super::{ProjectLimits, RateLimitStore};
use crate::redis_client::Redis;

pub(in crate::control_plane) struct RedisRateLimits(pub Redis);

#[async_trait]
impl RateLimitStore for RedisRateLimits {
    async fn allow(&self, project: &str, limits: ProjectLimits) -> Result<bool> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("limit.lua"))
            .arg(1)
            .arg(format!("durable-actors:starts:v1:{project}"))
            .arg(limits.starts_per_second)
            .arg(limits.start_burst);
        self.0.run(command).await
    }
}
