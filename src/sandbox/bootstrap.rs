use anyhow::Result;
use async_trait::async_trait;

use super::EnsureHostRequest;
use crate::bucket::ActivationHandoff;

#[async_trait]
pub(crate) trait HostBootstrap: Send + Sync {
    async fn credentials(&self, request: &EnsureHostRequest) -> Result<String>;
    async fn claim(&self, request: &EnsureHostRequest, route: &str) -> Result<ActivationHandoff>;
    async fn renew(&self, request: &EnsureHostRequest, route: &str) -> Result<ActivationHandoff>;
    async fn release(&self, request: &EnsureHostRequest) -> Result<()>;
}
