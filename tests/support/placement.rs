use std::{collections::HashMap, sync::Mutex};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{ObjectPlacement, ObjectPlacementStore, PlacementClaim, validate_region};
use crate::{actor_state::ActorStorageKey, host::HostId};

#[derive(Default)]
pub(crate) struct LocalObjectPlacementStore {
    placements: Mutex<HashMap<ActorStorageKey, ObjectPlacement>>,
}

#[async_trait]
impl ObjectPlacementStore for LocalObjectPlacementStore {
    async fn list_committed(
        &self,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>> {
        let placements = self.placements.lock().unwrap();
        let mut result: Vec<_> = placements
            .values()
            .filter(|placement| {
                placement.state_version > 0
                    && placement.state_object.is_some()
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
}

impl LocalObjectPlacementStore {
    pub async fn claim(
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
}
