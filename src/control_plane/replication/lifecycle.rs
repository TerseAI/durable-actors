use super::{ActorReplicaFleet, store::Group};
use crate::{
    bucket::RuntimeStorage,
    clock::{Clock, SystemClock},
    placement::ObjectPlacementStore,
};
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

impl ActorReplicaFleet {
    pub(crate) fn start(self: &Arc<Self>, storage: Arc<RuntimeStorage>, stop: CancellationToken) {
        let fleet = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(20));
            loop {
                tokio::select! { _ = stop.cancelled() => return, _ = interval.tick() => {} }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    result = fleet.reconcile(&storage) => {
                        if let Err(error) = result { tracing::warn!(%error, "actor replica reconciliation failed"); }
                    }
                }
            }
        });
    }

    async fn reconcile(&self, storage: &RuntimeStorage) -> Result<()> {
        for group in self.store.candidates().await? {
            let id = group.scope.identity();
            if let Err(error) = self.reconcile_group(storage, group).await {
                tracing::warn!(%error, activation = %id, "actor replica cleanup deferred");
            }
            self.store.checked(&id).await?;
        }
        Ok(())
    }

    async fn reconcile_group(&self, storage: &RuntimeStorage, group: Group) -> Result<()> {
        let owner = storage.get_owner(&group.scope.actor.storage_key()).await?;
        let now = SystemClock.now_ms()?;
        let active = owner.is_some_and(|owner| {
            owner.owner == group.scope.host
                && owner.lease.session_id == group.scope.session
                && owner.lease.expires_at_ms > now
        });
        if active {
            return self.retire_superseded(storage, &group).await;
        }
        let group = self.store.retire(&group.scope.identity()).await?;
        storage.retire_replication(&group.scope).await?;
        for slot in &group.slots {
            for instance in &slot.instances {
                self.provider.retire(&instance.host).await?;
            }
        }
        self.store.delete(&group.scope.identity()).await
    }

    async fn retire_superseded(&self, storage: &RuntimeStorage, group: &Group) -> Result<()> {
        if group.slots.iter().all(|slot| slot.instances.len() == 1) {
            return Ok(());
        }
        let members = storage.replica_members(&group.scope).await?;
        for slot in &group.slots {
            for instance in slot
                .instances
                .iter()
                .take(slot.instances.len().saturating_sub(1))
            {
                if members.iter().any(|m| m.host_id == instance.host) {
                    continue;
                }
                self.provider.retire(&instance.host).await?;
                self.store
                    .forget_instance(&group.scope.identity(), &instance.host)
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/control_plane/replica_lifecycle.rs"]
mod tests;
