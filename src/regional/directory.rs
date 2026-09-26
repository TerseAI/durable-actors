use std::sync::Arc;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::Region;
use crate::actor::ActorKey;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectAssignment {
    pub object_id: String,
    pub actor: ActorKey,
    pub first_ingress_region: Region,
    pub home_region: Region,
}

#[async_trait]
pub trait DirectoryStore: Send + Sync {
    async fn by_name(&self, actor: &ActorKey) -> Result<Option<ObjectAssignment>>;
    async fn by_id(&self, object_id: &str) -> Result<Option<ObjectAssignment>>;
    async fn create(&self, proposed: &ObjectAssignment) -> Result<ObjectAssignment>;
}

pub struct ActorDirectory {
    store: Arc<dyn DirectoryStore>,
}

impl ActorDirectory {
    pub fn new(store: Arc<dyn DirectoryStore>) -> Self {
        Self { store }
    }

    pub async fn get_or_create(
        &self,
        actor: &ActorKey,
        ingress: Region,
    ) -> Result<ObjectAssignment> {
        if let Some(existing) = self.lookup_by_name(actor).await? {
            return Ok(existing);
        }
        let proposed = ObjectAssignment {
            object_id: uuid::Uuid::new_v4().to_string(),
            actor: actor.clone(),
            first_ingress_region: ingress,
            home_region: ingress,
        };
        let assigned = self.store.create(&proposed).await?;
        assigned.validate()?;
        ensure!(assigned.actor == *actor, "directory returned another actor");
        Ok(assigned)
    }

    pub async fn lookup_by_name(&self, actor: &ActorKey) -> Result<Option<ObjectAssignment>> {
        actor.validate()?;
        let assignment = self.store.by_name(actor).await?;
        if let Some(assignment) = &assignment {
            assignment.validate()?;
            ensure!(
                assignment.actor == *actor,
                "directory returned another actor"
            );
        }
        Ok(assignment)
    }

    pub async fn lookup_by_id(&self, object_id: &str) -> Result<Option<ObjectAssignment>> {
        uuid::Uuid::parse_str(object_id)?;
        let assignment = self.store.by_id(object_id).await?;
        if let Some(assignment) = &assignment {
            assignment.validate()?;
            ensure!(
                assignment.object_id == object_id,
                "directory returned another object ID"
            );
        }
        Ok(assignment)
    }
}

impl ObjectAssignment {
    fn validate(&self) -> Result<()> {
        uuid::Uuid::parse_str(&self.object_id)?;
        self.actor.validate()?;
        ensure!(
            self.home_region == self.first_ingress_region,
            "invalid initial home assignment"
        );
        Ok(())
    }
}
