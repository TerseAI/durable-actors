use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio_postgres::{Row, types::Json};

use super::{Ownership, OwnershipStore, Stored, Version};
use crate::{actor::ActorKey, postgres::PostgresDatabase};

/// Ownership records as rows of `actor_ownership`, replaced by their `version`.
pub(crate) struct PostgresOwnership(PostgresDatabase);

impl PostgresOwnership {
    pub(crate) fn new(database: PostgresDatabase) -> Self {
        Self(database)
    }
}

#[async_trait]
impl OwnershipStore for PostgresOwnership {
    async fn load(&self, actor: &ActorKey) -> Result<Option<Stored>> {
        self.0
            .query_opt(
                "SELECT version, record FROM actor_ownership \
                 WHERE project_id = $1 AND actor_name = $2 AND actor_id = $3",
                &[&actor.project_id, &actor.actor_name, &actor.actor_id],
            )
            .await?
            .map(stored)
            .transpose()
    }

    async fn replace(&self, record: &Ownership, expected: Option<Version>) -> Result<bool> {
        let actor = &record.actor;
        let json = serde_json::to_value(record)?;
        let written = match expected {
            None => {
                self.0
                    .execute(
                        "INSERT INTO actor_ownership (project_id, actor_name, actor_id, version, sealed, record) \
                         VALUES ($1, $2, $3, 1, $4, $5) ON CONFLICT DO NOTHING",
                        &[&actor.project_id, &actor.actor_name, &actor.actor_id, &record.sealed, &json],
                    )
                    .await?
            }
            Some(Version::Postgres(version)) => {
                self.0
                    .execute(
                        "UPDATE actor_ownership SET version = version + 1, sealed = $4, record = $5 \
                         WHERE project_id = $1 AND actor_name = $2 AND actor_id = $3 AND version = $6",
                        &[&actor.project_id, &actor.actor_name, &actor.actor_id, &record.sealed, &json, &version],
                    )
                    .await?
            }
            Some(Version::Bucket(_)) => bail!("a PostgreSQL record cannot replace a bucket one"),
        };
        Ok(written == 1)
    }

    async fn unsealed(&self) -> Result<Vec<Stored>> {
        self.0
            .query(
                "SELECT version, record FROM actor_ownership WHERE NOT sealed",
                &[],
            )
            .await?
            .into_iter()
            .map(stored)
            .collect()
    }
}

fn stored(row: Row) -> Result<Stored> {
    let Json(record) = row.try_get(1)?;
    Ok(Stored {
        version: Version::Postgres(row.try_get(0)?),
        record,
    })
}
