use crate::{
    bucket::{ActivationHandoff, OwnershipHint, RuntimeStorageReader, access::RuntimeAccess},
    host_leases::HostLeaseRequest,
    sandbox::{EnsureHostRequest, substrate::HostBootstrap},
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;

pub(super) struct RuntimeBootstrap {
    access: Arc<RuntimeAccess>,
    storage: Arc<RuntimeStorageReader>,
}

impl RuntimeBootstrap {
    pub(super) fn new(access: Arc<RuntimeAccess>, storage: Arc<RuntimeStorageReader>) -> Self {
        Self { access, storage }
    }
}

#[async_trait]
impl HostBootstrap for RuntimeBootstrap {
    async fn credentials(&self, request: &EnsureHostRequest) -> Result<String> {
        self.access
            .bootstrap(
                &request.canonical_region,
                request.actor.as_ref().context("actor identity required")?,
                request.code_snapshot.as_deref(),
            )
            .await
    }

    async fn claim(&self, request: &EnsureHostRequest, route: &str) -> Result<ActivationHandoff> {
        let hint: Option<OwnershipHint> = request
            .owner_hint
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?;
        self.storage
            .prepare_activation(
                request.actor.as_ref().context("actor identity required")?,
                &HostLeaseRequest {
                    id: request.host_id.clone(),
                    session_id: request.session_id.clone(),
                    route: route.into(),
                    duration_ms: 30_000,
                },
                &request.canonical_region,
                request.actor_is_new,
                hint.as_ref(),
            )
            .await
    }

    async fn release(&self, request: &EnsureHostRequest) -> Result<()> {
        self.storage
            .release_activation(
                request.actor.as_ref().context("actor identity required")?,
                &request.host_id,
                &request.session_id,
            )
            .await
    }
}
