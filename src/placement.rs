#[cfg(test)]
pub(crate) mod testing;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{actor_state::ActorStorageKey, host::HostId, postgres::PostgresDatabase};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub owner_epoch: u64,
    pub home_region: String,
    pub state_version: u64,
    pub state_object: Option<String>,
    pub last_request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementClaim {
    Acquired(ObjectPlacement),
    Current(ObjectPlacement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateCommitRequest {
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub session_id: String,
    pub owner_epoch: u64,
    pub expected_version: u64,
    pub state_object: String,
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateCommit {
    Committed(ObjectPlacement),
    Current(ObjectPlacement),
}

#[async_trait]
pub trait ObjectPlacementStore: Send + Sync {
    async fn get_owner(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        self.get(object).await
    }
    async fn claim_actor(
        &self,
        actor: &crate::actor::ActorKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        region: &str,
    ) -> Result<PlacementClaim> {
        self.claim(&actor.storage_key(), expected, owner, region)
            .await
    }

    async fn matches_lease(
        &self,
        _placement: &ObjectPlacement,
        _lease: &crate::host_leases::HostLease,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>>;

    async fn list_committed(
        &self,
        namespace: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>>;

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        home_region: &str,
    ) -> Result<PlacementClaim>;

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit>;
}

pub struct PostgresObjectPlacementStore {
    database: PostgresDatabase,
}

impl PostgresObjectPlacementStore {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::from_database(PostgresDatabase::connect(url).await?))
    }

    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl ObjectPlacementStore for PostgresObjectPlacementStore {
    async fn list_committed(
        &self,
        namespace: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>> {
        self.database.query(
            "SELECT owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id, object_id \
             FROM durable_object_placements WHERE state_version > 0 AND state_object IS NOT NULL \
               AND ($1::text IS NULL OR split_part(state_object, '/', 4) = $1) \
               AND ($2::text IS NULL OR object_id COLLATE \"C\" > $2) \
             ORDER BY object_id COLLATE \"C\" LIMIT $3",
            &[&namespace, &after, &i64::from(limit)],
        ).await?.iter().map(|row| placement_from_row(&ActorStorageKey::new(row.get::<_, String>(6)), row)).collect()
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        let row = self
            .database
            .query_opt(
                "SELECT owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id \
                 FROM durable_object_placements WHERE object_id = $1",
                &[&object.as_str()],
            )
            .await
            .context("load PostgreSQL object placement")?;
        row.map(|row| placement_from_row(object, &row)).transpose()
    }

    async fn claim(
        &self,
        object: &ActorStorageKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        home_region: &str,
    ) -> Result<PlacementClaim> {
        object.validate()?;
        validate_region(home_region)?;
        if expected.is_none() {
            if let Some(row) = self
                .database
                .query_opt(
                    "INSERT INTO durable_object_placements \
                     (object_id, owner_host_id, owner_epoch, home_region) \
                     VALUES ($1, $2, 1, $3) ON CONFLICT DO NOTHING \
                     RETURNING owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id",
                    &[&object.as_str(), &owner.as_str(), &home_region],
                )
                .await
                .context("insert PostgreSQL object placement")?
            {
                return Ok(PlacementClaim::Acquired(placement_from_row(object, &row)?));
            }
            return self.current_claim(object).await;
        }

        let expected = expected.expect("checked above");
        ensure!(
            expected.object == *object && expected.home_region == home_region,
            "expected object placement does not match the claim"
        );
        if &expected.owner == owner {
            return self.current_claim(object).await;
        }
        let expected_epoch = i64::try_from(expected.owner_epoch)
            .context("object owner epoch exceeds PostgreSQL BIGINT")?;
        if let Some(row) = self
            .database
            .query_opt(
                "UPDATE durable_object_placements \
                 SET owner_host_id = $2, owner_epoch = owner_epoch + 1, updated_at = clock_timestamp() \
                 WHERE object_id = $1 AND owner_host_id = $3 AND owner_epoch = $4 AND home_region = $5 \
                 RETURNING owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id",
                &[
                    &object.as_str(),
                    &owner.as_str(),
                    &expected.owner.as_str(),
                    &expected_epoch,
                    &home_region,
                ],
            )
            .await
            .context("claim PostgreSQL object placement")?
        {
            return Ok(PlacementClaim::Acquired(placement_from_row(object, &row)?));
        }
        self.current_claim(object).await
    }

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit> {
        validate_state_commit(request)?;
        let expected_epoch = i64::try_from(request.owner_epoch)
            .context("object owner epoch exceeds PostgreSQL BIGINT")?;
        let expected_version = i64::try_from(request.expected_version)
            .context("actor state version exceeds PostgreSQL BIGINT")?;
        let row = self
            .database
            .query_opt(
                "UPDATE durable_object_placements AS placement \
                 SET state_version = state_version + 1, state_object = $6, last_request_id = $7, updated_at = clock_timestamp() \
                 FROM durable_object_host_leases AS lease \
                 WHERE placement.object_id = $1 \
                   AND placement.owner_host_id = $2 \
                   AND placement.owner_epoch = $3 \
                   AND placement.state_version = $4 \
                   AND lease.host_id = placement.owner_host_id \
                   AND lease.session_id = $5 \
                   AND lease.expires_at_ms > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
                 RETURNING placement.owner_host_id, placement.owner_epoch, placement.home_region, \
                           placement.state_version, placement.state_object, placement.last_request_id",
                &[
                    &request.object.as_str(),
                    &request.owner.as_str(),
                    &expected_epoch,
                    &expected_version,
                    &request.session_id,
                    &request.state_object,
                    &request.request_id,
                ],
            )
            .await
            .context("commit PostgreSQL actor state head")?;
        if let Some(row) = row {
            return Ok(StateCommit::Committed(placement_from_row(
                &request.object,
                &row,
            )?));
        }
        let current = self
            .get(&request.object)
            .await?
            .context("actor placement disappeared during state commit")?;
        Ok(if is_replayed_commit(&current, request) {
            StateCommit::Committed(current)
        } else {
            StateCommit::Current(current)
        })
    }
}

impl PostgresObjectPlacementStore {
    async fn current_claim(&self, object: &ActorStorageKey) -> Result<PlacementClaim> {
        self.get(object)
            .await?
            .map(PlacementClaim::Current)
            .context("object placement disappeared during claim")
    }
}

fn placement_from_row(
    object: &ActorStorageKey,
    row: &tokio_postgres::Row,
) -> Result<ObjectPlacement> {
    Ok(ObjectPlacement {
        object: object.clone(),
        owner: HostId::new(row.get::<_, String>(0)),
        owner_epoch: u64::try_from(row.get::<_, i64>(1))
            .context("PostgreSQL object owner epoch is negative")?,
        home_region: row.get(2),
        state_version: u64::try_from(row.get::<_, i64>(3))
            .context("PostgreSQL actor state version is negative")?,
        state_object: row.get(4),
        last_request_id: row.get(5),
    })
}

pub(crate) fn validate_state_commit(request: &StateCommitRequest) -> Result<()> {
    request.object.validate()?;
    ensure!(
        !request.owner.as_str().is_empty(),
        "state commit owner is empty"
    );
    ensure!(
        !request.session_id.is_empty(),
        "state commit session is empty"
    );
    ensure!(
        request.owner_epoch > 0,
        "state commit owner epoch must be positive"
    );
    ensure!(
        !request.state_object.is_empty() && request.state_object.len() <= 1024,
        "state commit object name is invalid"
    );
    ensure!(
        !request.request_id.is_empty() && request.request_id.len() <= 255,
        "state commit request ID is invalid"
    );
    Ok(())
}

pub(crate) fn is_replayed_commit(current: &ObjectPlacement, request: &StateCommitRequest) -> bool {
    current.state_version == request.expected_version.saturating_add(1)
        && current.state_object.as_deref() == Some(&request.state_object)
        && current.last_request_id.as_deref() == Some(&request.request_id)
}

pub fn validate_region(region: &str) -> Result<()> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            }),
        "sandbox region is invalid"
    );
    Ok(())
}
