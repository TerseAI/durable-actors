use std::{collections::HashMap, sync::Mutex};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    ObjectPlacement, ObjectPlacementStore, PlacementClaim, StateCommit, StateCommitRequest,
    is_replayed_commit, validate_region, validate_state_commit,
};
use crate::{actor_state::ActorStorageKey, host::HostId};

#[derive(Default)]
pub(crate) struct LocalObjectPlacementStore {
    placements: Mutex<HashMap<ActorStorageKey, ObjectPlacement>>,
}

#[async_trait]
impl ObjectPlacementStore for LocalObjectPlacementStore {
    async fn list_committed(
        &self,
        namespace: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>> {
        let placements = self.placements.lock().unwrap();
        let mut result: Vec<_> = placements
            .values()
            .filter(|placement| {
                placement.state_version > 0
                    && placement.state_object.as_deref().is_some_and(|object| {
                        namespace
                            .is_none_or(|namespace| object.split('/').nth(1) == Some(namespace))
                    })
                    && after.is_none_or(|after| placement.object.as_str() > after)
            })
            .cloned()
            .collect();
        result.sort_by(|a, b| a.object.as_str().cmp(b.object.as_str()));
        result.truncate(limit as usize);
        Ok(result)
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?
            .get(object)
            .cloned())
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        home_region: &str,
    ) -> Result<PlacementClaim> {
        validate_region(home_region)?;
        let mut placements = self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?;
        match placements.get(object) {
            None if expected.is_none() => {
                let placement = ObjectPlacement {
                    object: object.clone(),
                    owner: owner.clone(),
                    owner_epoch: 1,
                    home_region: home_region.to_owned(),
                    state_version: 0,
                    state_object: None,
                    last_request_id: None,
                };
                placements.insert(object.clone(), placement.clone());
                Ok(PlacementClaim::Acquired(placement))
            }
            Some(current) if expected == Some(current) => {
                ensure!(
                    current.home_region == home_region,
                    "object home region cannot change"
                );
                if &current.owner == owner {
                    return Ok(PlacementClaim::Current(current.clone()));
                }
                let placement = ObjectPlacement {
                    object: object.clone(),
                    owner: owner.clone(),
                    owner_epoch: current
                        .owner_epoch
                        .checked_add(1)
                        .context("object owner epoch overflow")?,
                    home_region: home_region.to_owned(),
                    state_version: current.state_version,
                    state_object: current.state_object.clone(),
                    last_request_id: current.last_request_id.clone(),
                };
                placements.insert(object.clone(), placement.clone());
                Ok(PlacementClaim::Acquired(placement))
            }
            Some(current) => Ok(PlacementClaim::Current(current.clone())),
            None => anyhow::bail!("expected object placement no longer exists"),
        }
    }

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit> {
        validate_state_commit(request)?;
        let mut placements = self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?;
        let current = placements
            .get(&request.object)
            .cloned()
            .context("actor placement does not exist")?;
        if is_replayed_commit(&current, request) {
            return Ok(StateCommit::Committed(current));
        }
        if current.owner != request.owner
            || current.owner_epoch != request.owner_epoch
            || current.state_version != request.expected_version
        {
            return Ok(StateCommit::Current(current));
        }
        let mut committed = current;
        committed.state_version = committed
            .state_version
            .checked_add(1)
            .context("actor state version overflow")?;
        committed.state_object = Some(request.state_object.clone());
        committed.last_request_id = Some(request.request_id.clone());
        placements.insert(request.object.clone(), committed.clone());
        Ok(StateCommit::Committed(committed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorKey;

    #[tokio::test]
    async fn claims_once_and_increments_epoch_on_transfer() -> Result<()> {
        let store = LocalObjectPlacementStore::default();
        let object = ActorStorageKey::new("object.v1.project.Counter.one");
        let first = match store
            .claim(&object, None, &HostId::new("host-a"), "us-east")
            .await?
        {
            PlacementClaim::Acquired(placement) => placement,
            claim => anyhow::bail!("unexpected claim: {claim:?}"),
        };
        assert_eq!(first.owner_epoch, 1);

        let second = match store
            .claim(&object, Some(&first), &HostId::new("host-b"), "us-east")
            .await?
        {
            PlacementClaim::Acquired(placement) => placement,
            claim => anyhow::bail!("unexpected claim: {claim:?}"),
        };
        assert_eq!(second.owner, HostId::new("host-b"));
        assert_eq!(second.owner_epoch, 2);
        assert_eq!(second.home_region, "us-east");
        Ok(())
    }

    #[tokio::test]
    async fn stale_claim_observes_the_current_owner() -> Result<()> {
        let store = LocalObjectPlacementStore::default();
        let object = ActorStorageKey::new("object.v1.project.Counter.one");
        let PlacementClaim::Acquired(first) = store
            .claim(&object, None, &HostId::new("host-a"), "us-east")
            .await?
        else {
            anyhow::bail!("first claim was not acquired")
        };
        let PlacementClaim::Acquired(second) = store
            .claim(&object, Some(&first), &HostId::new("host-b"), "us-east")
            .await?
        else {
            anyhow::bail!("second claim was not acquired")
        };
        assert_eq!(
            store
                .claim(&object, Some(&first), &HostId::new("host-c"), "us-east",)
                .await?,
            PlacementClaim::Current(second)
        );
        Ok(())
    }

    #[tokio::test]
    async fn state_head_advances_once_and_replays_the_same_commit() -> Result<()> {
        let store = LocalObjectPlacementStore::default();
        let actor = actor();
        let host = HostId::new("host-a");
        let PlacementClaim::Acquired(placement) = store
            .claim(&actor.storage_key(), None, &host, "us-east")
            .await?
        else {
            anyhow::bail!("first claim was not acquired")
        };
        let request = commit(&actor.storage_key(), &host, placement.owner_epoch);

        let StateCommit::Committed(committed) = store.commit_state(&request).await? else {
            anyhow::bail!("first state commit did not succeed")
        };
        assert_eq!(committed.state_version, 1);
        assert_eq!(
            committed.state_object.as_deref(),
            Some("snapshots/a/state-1.json")
        );

        let StateCommit::Committed(replayed) = store.commit_state(&request).await? else {
            anyhow::bail!("identical state commit was not idempotent")
        };
        assert_eq!(replayed, committed);

        let mut conflicting = request;
        conflicting.state_object = "snapshots/b/state-1.json".into();
        assert!(matches!(
            store.commit_state(&conflicting).await?,
            StateCommit::Current(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn transferred_ownership_fences_an_old_state_commit() -> Result<()> {
        let store = LocalObjectPlacementStore::default();
        let actor = actor();
        let old_host = HostId::new("host-a");
        let PlacementClaim::Acquired(first) = store
            .claim(&actor.storage_key(), None, &old_host, "us-east")
            .await?
        else {
            anyhow::bail!("first claim was not acquired")
        };
        let PlacementClaim::Acquired(_) = store
            .claim(
                &actor.storage_key(),
                Some(&first),
                &HostId::new("host-b"),
                "us-east",
            )
            .await?
        else {
            anyhow::bail!("ownership transfer was not acquired")
        };

        assert!(matches!(
            store
                .commit_state(&commit(&actor.storage_key(), &old_host, first.owner_epoch,))
                .await?,
            StateCommit::Current(_)
        ));
        Ok(())
    }

    fn actor() -> ActorKey {
        ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        }
    }

    fn commit(object: &ActorStorageKey, host: &HostId, owner_epoch: u64) -> StateCommitRequest {
        StateCommitRequest {
            object: object.clone(),
            owner: host.clone(),
            session_id: "session-a".into(),
            owner_epoch,
            expected_version: 0,
            state_object: "snapshots/a/state-1.json".into(),
            request_id: "request-1".into(),
        }
    }
}
