use std::{collections::HashMap, sync::Mutex};

use anyhow::Result;
use async_trait::async_trait;

use super::{ObjectPlacement, ObjectPlacementStore, validate_region};
use crate::{actor_state::ActorStorageKey, host_leases::HostLease};

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
    pub fn set_owner(
        &self,
        object: &ActorStorageKey,
        lease: HostLease,
        home_region: &str,
    ) -> Result<()> {
        validate_region(home_region)?;
        self.placements.lock().unwrap().insert(
            object.clone(),
            ObjectPlacement {
                owner: lease.id.clone(),
                lease,
                object: object.clone(),
                owner_epoch: 1,
                home_region: home_region.into(),
                state_version: 0,
                state_object: None,
                last_request_id: None,
            },
        );
        Ok(())
    }
}
