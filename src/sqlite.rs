use std::{path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio_rusqlite::{
    Connection,
    rusqlite::{self, OptionalExtension, Row, TransactionBehavior, params},
};

use crate::{
    actor::ActorScope,
    actor_state::ActorStorageKey,
    control_plane::admin::{AdminRegistry, HostLaunchSpec},
    host::HostId,
    host_leases::{
        HostLease, HostLeaseRegistry, HostLeaseRequest, HostLeaseStatus, HostLeaseStore,
    },
    placement::{
        ObjectPlacement, ObjectPlacementStore, PlacementClaim, StateCommit, StateCommitRequest,
        is_replayed_commit, validate_region, validate_state_commit,
    },
};

const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

#[derive(Clone)]
pub struct SqliteStore {
    connection: Connection,
}

impl SqliteStore {
    pub async fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)
            .await
            .context("open SQLite metadata database")?;
        connection
            .call(|connection| -> rusqlite::Result<_> {
                connection.busy_timeout(Duration::from_secs(5))?;
                connection.execute_batch(include_str!("sqlite/schema.sql"))?;
                Ok(())
            })
            .await?;
        Ok(Self { connection })
    }

    pub(crate) async fn reset_local_leases(&self) -> Result<()> {
        self.connection
            .call(|connection| -> rusqlite::Result<_> {
                connection.execute("DELETE FROM host_leases", [])?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl HostLeaseRegistry for SqliteStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        let request = request.clone();
        let lease = self.connection.call(move |connection| -> rusqlite::Result<_> {
            connection.query_row(&format!(
                "INSERT INTO host_leases (host_id, session_id, route, expires_at_ms) \
                 VALUES (?1, ?2, ?3, {NOW_MS} + ?4) \
                 ON CONFLICT (host_id) DO UPDATE SET session_id = excluded.session_id, \
                   route = excluded.route, expires_at_ms = excluded.expires_at_ms \
                 WHERE host_leases.session_id = excluded.session_id OR host_leases.expires_at_ms <= {NOW_MS} \
                 RETURNING expires_at_ms"),
                params![request.id.as_str(), request.session_id, request.route, request.duration_ms],
                |row| Ok(HostLease { id: request.id.clone(), session_id: request.session_id.clone(), route: request.route.clone(), expires_at_ms: row.get(0)? }),
            ).optional()
        }).await?;
        lease.context("host lease is held by a different active session")
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
        let id = id.as_str().to_owned();
        let session = session_id.to_owned();
        self.connection
            .call(move |connection| -> rusqlite::Result<_> {
                connection.execute(
                    "DELETE FROM host_leases WHERE host_id = ?1 AND session_id = ?2",
                    params![id, session],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl HostLeaseStore for SqliteStore {
    async fn lease_status(&self, id: &HostId) -> Result<HostLeaseStatus> {
        let id = id.clone();
        Ok(self
            .connection
            .call(move |connection| -> rusqlite::Result<_> {
                connection.query_row(
                    &format!(
                        "SELECT {NOW_MS}, session_id, route, expires_at_ms \
                 FROM (SELECT 1) LEFT JOIN host_leases ON host_id = ?1"
                    ),
                    [id.as_str()],
                    |row| {
                        let session: Option<String> = row.get(1)?;
                        Ok(HostLeaseStatus {
                            store_now_ms: row.get(0)?,
                            lease: session
                                .map(|session_id| {
                                    Ok::<_, tokio_rusqlite::rusqlite::Error>(HostLease {
                                        id: id.clone(),
                                        session_id,
                                        route: row.get(2)?,
                                        expires_at_ms: row.get(3)?,
                                    })
                                })
                                .transpose()?,
                        })
                    },
                )
            })
            .await?)
    }
}

#[async_trait]
impl ObjectPlacementStore for SqliteStore {
    async fn list_committed(
        &self,
        namespace: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>> {
        if let Some(namespace_id) = namespace {
            ActorScope {
                namespace_id: namespace_id.to_owned(),
            }
            .validate()?;
        }
        let pattern = namespace.map(|namespace| format!("snapshots/{namespace}/*"));
        let after = after.map(str::to_owned);
        Ok(self.connection.call(move |connection| -> rusqlite::Result<_> {
            let mut statement = connection.prepare(
                "SELECT owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id, object_id \
                 FROM placements WHERE state_version > 0 AND state_object IS NOT NULL \
                   AND (?1 IS NULL OR state_object GLOB ?1) AND (?2 IS NULL OR object_id > ?2) \
                 ORDER BY object_id LIMIT ?3"
            )?;
            statement.query_map(params![pattern, after, limit], |row| {
                placement_from_row(&ActorStorageKey::new(row.get::<_, String>(6)?), row)
            })?.collect::<rusqlite::Result<Vec<_>>>()
        }).await?)
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        object.validate()?;
        let object = object.clone();
        Ok(self
            .connection
            .call(move |connection| read_placement(connection, &object))
            .await?)
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
        if let Some(expected) = expected {
            ensure!(
                expected.object == *object && expected.home_region == home_region,
                "expected object placement does not match the claim"
            );
            if &expected.owner == owner {
                return self
                    .get(object)
                    .await?
                    .map(PlacementClaim::Current)
                    .context("object placement disappeared during claim");
            }
        }
        let (object, expected, owner, region) = (
            object.clone(),
            expected.cloned(),
            owner.clone(),
            home_region.to_owned(),
        );
        Ok(self.connection.call(move |connection| -> rusqlite::Result<_> {
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = if let Some(expected) = expected {
                transaction.execute(
                    "UPDATE placements SET owner_host_id = ?2, owner_epoch = owner_epoch + 1 \
                     WHERE object_id = ?1 AND owner_host_id = ?3 AND owner_epoch = ?4 AND home_region = ?5",
                    params![object.as_str(), owner.as_str(), expected.owner.as_str(), expected.owner_epoch, region],
                )?
            } else {
                transaction.execute(
                    "INSERT INTO placements (object_id, owner_host_id, owner_epoch, home_region) \
                     VALUES (?1, ?2, 1, ?3) ON CONFLICT DO NOTHING", params![object.as_str(), owner.as_str(), region],
                )?
            };
            let current = read_placement(&transaction, &object)?.ok_or(tokio_rusqlite::rusqlite::Error::QueryReturnedNoRows)?;
            transaction.commit()?;
            Ok(if changed == 1 { PlacementClaim::Acquired(current) } else { PlacementClaim::Current(current) })
        }).await?)
    }

    async fn commit_state(&self, request: &StateCommitRequest) -> Result<StateCommit> {
        validate_state_commit(request)?;
        let request = request.clone();
        Ok(self.connection.call(move |connection| -> rusqlite::Result<_> {
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let changed = transaction.execute(&format!(
                "UPDATE placements SET state_version = state_version + 1, state_object = ?6, last_request_id = ?7 \
                 WHERE object_id = ?1 AND owner_host_id = ?2 AND owner_epoch = ?3 AND state_version = ?4 \
                   AND EXISTS (SELECT 1 FROM host_leases WHERE host_id = ?2 AND session_id = ?5 AND expires_at_ms > {NOW_MS})"),
                params![request.object.as_str(), request.owner.as_str(), request.owner_epoch, request.expected_version, request.session_id, request.state_object, request.request_id],
            )?;
            let current = read_placement(&transaction, &request.object)?.ok_or(tokio_rusqlite::rusqlite::Error::QueryReturnedNoRows)?;
            transaction.commit()?;
            Ok(if changed == 1 || is_replayed_commit(&current, &request) { StateCommit::Committed(current) } else { StateCommit::Current(current) })
        }).await?)
    }
}

fn read_placement(
    connection: &tokio_rusqlite::rusqlite::Connection,
    object: &ActorStorageKey,
) -> tokio_rusqlite::rusqlite::Result<Option<ObjectPlacement>> {
    connection.query_row(
        "SELECT owner_host_id, owner_epoch, home_region, state_version, state_object, last_request_id FROM placements WHERE object_id = ?1",
        [object.as_str()], |row| placement_from_row(object, row),
    ).optional()
}

fn placement_from_row(
    object: &ActorStorageKey,
    row: &Row<'_>,
) -> tokio_rusqlite::rusqlite::Result<ObjectPlacement> {
    Ok(ObjectPlacement {
        object: object.clone(),
        owner: HostId::new(row.get::<_, String>(0)?),
        owner_epoch: row.get(1)?,
        home_region: row.get(2)?,
        state_version: row.get(3)?,
        state_object: row.get(4)?,
        last_request_id: row.get(5)?,
    })
}

#[async_trait]
impl AdminRegistry for SqliteStore {
    async fn ensure_namespace_and_register_deployment(
        &self,
        spec: &HostLaunchSpec,
    ) -> Result<bool> {
        spec.validate()?;
        let namespace = spec.namespace_id.clone();
        let body = serde_json::to_string(spec)?;
        Ok(self.connection.call(move |connection| -> rusqlite::Result<_> {
            Ok(connection.execute(
                "INSERT INTO deployments (namespace_id, body) VALUES (?1, ?2) \
                 ON CONFLICT (namespace_id) DO UPDATE SET body = excluded.body WHERE deployments.body <> excluded.body", params![namespace, body],
            )? == 1)
        }).await?)
    }

    async fn launch_spec(&self, namespace_id: &str) -> Result<Option<HostLaunchSpec>> {
        ActorScope {
            namespace_id: namespace_id.to_owned(),
        }
        .validate()?;
        let namespace = namespace_id.to_owned();
        let body: Option<String> = self
            .connection
            .call(move |connection| -> rusqlite::Result<_> {
                connection
                    .query_row(
                        "SELECT body FROM deployments WHERE namespace_id = ?1",
                        [namespace],
                        |row| row.get(0),
                    )
                    .optional()
            })
            .await?;
        body.map(|body| serde_json::from_str(&body).context("decode SQLite deployment"))
            .transpose()
    }

    async fn remove_deployment(&self, namespace_id: &str) -> Result<()> {
        ActorScope {
            namespace_id: namespace_id.to_owned(),
        }
        .validate()?;
        let namespace = namespace_id.to_owned();
        self.connection
            .call(move |connection| -> rusqlite::Result<_> {
                connection.execute(
                    "DELETE FROM deployments WHERE namespace_id = ?1",
                    [namespace],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}
