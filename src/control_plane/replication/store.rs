use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    postgres::PostgresDatabase,
    replication::{ReplicaScope, ReplicaTarget},
};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Group {
    pub scope: ReplicaScope,
    pub image: String,
    pub retiring: bool,
    pub slots: Vec<Slot>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Slot {
    pub region: String,
    pub instances: Vec<Instance>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Instance {
    pub host: String,
    pub target: Option<ReplicaTarget>,
}

impl Instance {
    fn new() -> Self {
        let id = uuid::Uuid::new_v4().simple().to_string();
        Self {
            host: format!("replica.{id}"),
            target: None,
        }
    }
}

#[derive(Clone)]
pub(super) struct Store(pub PostgresDatabase);

impl Store {
    pub async fn contains(&self, id: &str) -> Result<bool> {
        Ok(self
            .0
            .query_opt(
                "SELECT 1 FROM durable_actors_replica_groups WHERE id = $1",
                &[&id],
            )
            .await?
            .is_some())
    }

    pub async fn prepare(
        &self,
        scope: &ReplicaScope,
        regions: &[String],
        image: &str,
        failed: &[String],
    ) -> Result<Group> {
        let group = Group {
            scope: scope.clone(),
            image: image.into(),
            retiring: false,
            slots: regions
                .iter()
                .map(|region| Slot {
                    region: region.clone(),
                    instances: vec![Instance::new()],
                })
                .collect(),
        };
        self.0.execute("INSERT INTO durable_actors_replica_groups (id, config) VALUES ($1, $2) ON CONFLICT DO NOTHING", &[&scope.identity(), &serde_json::to_string(&group)?]).await?;
        self.update(&scope.identity(), true, false, |group| {
            ensure!(
                !group.retiring && group.scope == *scope,
                "replica activation is retired or changed"
            );
            for slot in &mut group.slots {
                let current = slot.instances.last().context("replica slot is empty")?;
                if failed.contains(&current.host) {
                    slot.instances.push(Instance::new());
                }
            }
            Ok(())
        })
        .await
    }

    pub async fn record(
        &self,
        scope: &ReplicaScope,
        slot: usize,
        target: ReplicaTarget,
    ) -> Result<()> {
        self.update(&scope.identity(), true, false, |group| {
            ensure!(!group.retiring, "replica group is retiring");
            let current = group.slots[slot]
                .instances
                .last_mut()
                .context("replica slot is empty")?;
            ensure!(
                current.host == target.host_id,
                "replica was replaced concurrently"
            );
            current.target = Some(target);
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn retry_assignment(&self, id: &str, host: &str) -> Result<()> {
        self.update(id, true, false, |group| {
            ensure!(!group.retiring, "replica group is retiring");
            for slot in &mut group.slots {
                if slot
                    .instances
                    .last()
                    .is_some_and(|instance| instance.host == host && instance.target.is_none())
                {
                    slot.instances.push(Instance::new());
                }
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn candidates(&self) -> Result<Vec<Group>> {
        let client = self.0.connection().await?;
        let rows = client.query("SELECT config FROM durable_actors_replica_groups WHERE updated_at < clock_timestamp() - interval '120 seconds' ORDER BY checked_at LIMIT 64", &[]).await?;
        rows.iter()
            .map(|row| Ok(serde_json::from_str(row.get(0))?))
            .collect()
    }

    pub async fn checked(&self, id: &str) -> Result<()> {
        self.0.execute("UPDATE durable_actors_replica_groups SET checked_at = clock_timestamp() WHERE id = $1", &[&id]).await?;
        Ok(())
    }

    pub async fn retire(&self, id: &str) -> Result<Group> {
        self.update(id, false, true, |group| {
            group.retiring = true;
            Ok(())
        })
        .await
    }

    pub async fn forget_instance(&self, id: &str, host: &str) -> Result<()> {
        self.update(id, false, false, |group| {
            for slot in &mut group.slots {
                let current = slot.instances.last().map(|i| i.host.clone());
                slot.instances
                    .retain(|i| i.host != host || current.as_ref() == Some(&i.host));
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        self.0
            .execute(
                "DELETE FROM durable_actors_replica_groups WHERE id = $1",
                &[&id],
            )
            .await?;
        Ok(())
    }

    async fn update(
        &self,
        id: &str,
        touch: bool,
        retiring: bool,
        change: impl FnOnce(&mut Group) -> Result<()>,
    ) -> Result<Group> {
        let mut client = self.0.connection().await?;
        let tx = client.transaction().await?;
        let row = tx.query_opt("SELECT config FROM durable_actors_replica_groups WHERE id = $1 AND (NOT $2 OR updated_at < clock_timestamp() - interval '120 seconds') FOR UPDATE", &[&id, &retiring]).await?.context("replica group missing or provisioning is in progress")?;
        let mut group: Group = serde_json::from_str(row.get(0))?;
        change(&mut group)?;
        tx.execute("UPDATE durable_actors_replica_groups SET config = $2, updated_at = CASE WHEN $3 THEN clock_timestamp() ELSE updated_at END WHERE id = $1", &[&id, &serde_json::to_string(&group)?, &touch]).await?;
        tx.commit().await?;
        Ok(group)
    }
}
