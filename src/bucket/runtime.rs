use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    host::HostId,
    host_leases::{HostLease, HostLeaseStore},
    placement::{
        ObjectPlacement, ObjectPlacementStore, PlacementClaim, StateCommit, StateCommitRequest,
    },
    replication::{
        ArchiveTicket, ReplicaAccess, ReplicaGrant, ReplicaProvisioner, ReplicaStream,
        ReplicaTarget, ReplicationTicket, SnapshotRef, access::AccessQuery,
    },
    storage_urls::{StateWriteTicket, StorageUrlSigner, snapshot_object_name},
};

use super::{Bucket, ReplicaPeers, component, peers::grant, replace};

pub struct RuntimeStorage {
    authority: Arc<dyn Bucket>,
    states: HashMap<String, Arc<dyn Bucket>>,
    leases: Arc<dyn HostLeaseStore>,
    fleet: Arc<dyn ReplicaProvisioner>,
    peers: Arc<dyn ReplicaPeers>,
    access: ReplicaAccess,
    origin: String,
    count: usize,
}

#[derive(Clone, Serialize, Deserialize)]
struct Ownership {
    actor: ActorKey,
    placement: ObjectPlacement,
    session: String,
    stream: ReplicaStream,
    replicas: Vec<ReplicaTarget>,
    base: Option<SnapshotRef>,
    active: bool,
    predecessor: Option<Box<Ownership>>,
    mutation: String,
}

impl RuntimeStorage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authority: Arc<dyn Bucket>,
        states: HashMap<String, Arc<dyn Bucket>>,
        leases: Arc<dyn HostLeaseStore>,
        fleet: Arc<dyn ReplicaProvisioner>,
        peers: Arc<dyn ReplicaPeers>,
        access: ReplicaAccess,
        origin: String,
        count: usize,
    ) -> Result<Self> {
        ensure!(
            !states.is_empty() && count <= crate::replication::MAX_REPLICAS,
            "invalid runtime storage configuration"
        );
        Ok(Self {
            authority,
            states,
            leases,
            fleet,
            peers,
            access,
            origin,
            count,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/_replica/runtime", get(read).put(write))
            .route("/_replica/runtime-archive", get(archive))
            .layer(DefaultBodyLimit::max(32 * 1024 * 1024))
            .with_state(self)
    }
}

#[async_trait]
impl ObjectPlacementStore for RuntimeStorage {
    async fn get_owner(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self.load(object).await?.map(|(_, record)| record.placement))
    }
    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        match self.load(object).await? {
            Some((_, record)) => Ok(Some(self.current_placement(&record).await?)),
            None => Ok(None),
        }
    }

    async fn list_committed(
        &self,
        namespace: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>> {
        let mut records = Vec::new();
        for key in self.authority.list("runtime/owners/").await? {
            if let Some(object) = self.authority.get(&key).await? {
                let record: Ownership = serde_json::from_slice(&object.bytes)?;
                if record.active
                    && namespace.is_none_or(|id| id == record.actor.namespace_id)
                    && after.is_none_or(|id| record.placement.object.as_str() > id)
                {
                    records.push(record);
                }
            }
        }
        records.sort_by(|a, b| a.placement.object.as_str().cmp(b.placement.object.as_str()));
        let mut placements = Vec::new();
        for record in records {
            let placement = self.current_placement(&record).await?;
            if placement.state_version > 0 {
                placements.push(placement);
            }
            if placements.len() >= limit as usize {
                break;
            }
        }
        Ok(placements)
    }

    async fn claim_actor(
        &self,
        actor: &ActorKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        region: &str,
    ) -> Result<PlacementClaim> {
        self.acquire(actor, expected, owner, region).await
    }

    async fn claim(
        &self,
        _: &ActorStorageKey,
        _: Option<&ObjectPlacement>,
        _: &HostId,
        _: &str,
    ) -> Result<PlacementClaim> {
        anyhow::bail!("bucket ownership requires the actor identity")
    }

    async fn matches_lease(&self, placement: &ObjectPlacement, lease: &HostLease) -> Result<bool> {
        Ok(self
            .load(&placement.object)
            .await?
            .is_some_and(|(_, record)| {
                record.active
                    && record.placement.owner_epoch == placement.owner_epoch
                    && record.placement.owner == lease.id
                    && record.session == lease.session_id
            }))
    }

    async fn commit_state(&self, _: &StateCommitRequest) -> Result<StateCommit> {
        anyhow::bail!("bucket runtime writes are committed by their durability proof")
    }
}

#[async_trait]
impl StorageUrlSigner for RuntimeStorage {
    fn uses_epoch_streams(&self) -> bool {
        true
    }
    fn durability(&self) -> crate::replication::DurabilityPolicy {
        let mut policy = crate::replication::DurabilityPolicy::new(self.count);
        policy.replica_regions = self.fleet.replica_regions();
        if !policy.replica_regions.is_empty() {
            policy.mode = "cross_region_preview".into();
        }
        policy
    }

    fn regions(&self) -> Vec<String> {
        let mut regions: Vec<_> = self.states.keys().cloned().collect();
        regions.sort();
        regions
    }

    async fn read_url(&self, region: &str, object: &str) -> Result<String> {
        self.access.url(
            &self.origin,
            "runtime",
            &grant("RUNTIME_READ", region, object, 60_000)?,
        )
    }

    async fn write_ticket(
        &self,
        region: &str,
        actor: &ActorKey,
        version: u64,
    ) -> Result<StateWriteTicket> {
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor has no ownership")?;
        ensure!(
            record.actor == *actor
                && record.active
                && record.placement.home_region == region
                && version > record.stream.base_version,
            "ownership is not ready for writes"
        );
        let mut capability = grant("RUNTIME_WRITE", region, &record.stream.prefix, 60_000)?;
        capability.stream = Some(record.stream.clone());
        let url = self.access.url(&self.origin, "runtime", &capability)?;
        let expires_at_ms = i64::try_from(capability.expires_at_ms)?;
        capability.operation = "RUNTIME_ARCHIVE".into();
        capability.expires_at_ms = u64::MAX;
        let archive_url = self
            .access
            .url(&self.origin, "runtime-archive", &capability)?;
        let mut targets = Vec::new();
        for peer in &record.replicas {
            let target = ReplicaGrant {
                operation: "APPEND".into(),
                host_id: peer.host_id.clone(),
                archive_url: archive_url.clone(),
                expires_at_ms: expires_at_ms as u64,
                ..capability.clone()
            };
            targets.push(ReplicaTarget {
                host_id: peer.host_id.clone(),
                region: peer.region.clone(),
                url: self.access.url(&peer.url, "stream", &target)?,
            });
        }
        Ok(StateWriteTicket {
            state_version: version,
            object_name: record.stream.object(version),
            url,
            expires_at_ms,
            stream: Some(record.stream),
            replication: if targets.is_empty() {
                None
            } else {
                Some(ReplicationTicket {
                    required_replicas: targets.len(),
                    replicas: targets,
                    archive_url,
                })
            },
        })
    }
}

impl RuntimeStorage {
    async fn acquire(
        &self,
        actor: &ActorKey,
        expected: Option<&ObjectPlacement>,
        owner: &HostId,
        region: &str,
    ) -> Result<PlacementClaim> {
        actor.validate()?;
        ensure!(
            expected.is_none_or(|placement| placement.object == actor.storage_key()),
            "expected placement belongs to another actor"
        );
        self.bucket(region)?;
        let status = self.leases.lease_status(owner).await?;
        ensure!(status.is_active(), "new owner has no active lease");
        let lease = status.lease.context("new owner lease is missing")?;
        let current = self.load(&actor.storage_key()).await?;
        ensure!(
            current
                .as_ref()
                .map(|(_, record)| (record.placement.owner.clone(), record.placement.owner_epoch))
                == expected.map(|p| (p.owner.clone(), p.owner_epoch)),
            "ownership changed concurrently"
        );
        if let Some((_, record)) = &current {
            ensure!(record.actor == *actor, "ownership belongs to another actor");
            ensure!(
                record.placement.home_region == region,
                "ownership region cannot change"
            );
            if record.active
                && record.placement.owner == *owner
                && record.session == lease.session_id
            {
                return Ok(PlacementClaim::Current(
                    self.current_placement(record).await?,
                ));
            }
            self.ensure_previous_owner_stopped(record, &lease).await?;
        }
        let record = self.pending_ownership(
            actor,
            current.as_ref().map(|(_, record)| record),
            &lease,
            region,
        )?;
        let key = ownership_key(&actor.storage_key());
        ensure!(
            replace(
                self.authority.as_ref(),
                &key,
                current.map(|(generation, _)| generation),
                serde_json::to_vec(&record)?
            )
            .await?,
            "ownership claim lost"
        );
        self.activate(record).await.map(PlacementClaim::Acquired)
    }

    async fn ensure_previous_owner_stopped(
        &self,
        previous: &Ownership,
        next: &HostLease,
    ) -> Result<()> {
        if !previous.active
            && previous.placement.owner == next.id
            && previous.session == next.session_id
        {
            return Ok(());
        }
        let status = self.leases.lease_status(&previous.placement.owner).await?;
        ensure!(
            !status.is_active()
                || status
                    .lease
                    .is_none_or(|lease| lease.session_id != previous.session),
            "previous owner lease is still active"
        );
        Ok(())
    }

    fn pending_ownership(
        &self,
        actor: &ActorKey,
        previous: Option<&Ownership>,
        lease: &HostLease,
        region: &str,
    ) -> Result<Ownership> {
        let epoch = previous.map_or(Ok(1), |record| {
            record
                .placement
                .owner_epoch
                .checked_add(1)
                .context("owner epoch overflow")
        })?;
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let name = snapshot_object_name(actor, 1, &nonce)?;
        let predecessor = previous.and_then(|record| {
            if record.active {
                Some(Box::new(record.clone()))
            } else {
                record.predecessor.clone()
            }
        });
        Ok(Ownership {
            actor: actor.clone(),
            session: lease.session_id.clone(),
            placement: ObjectPlacement {
                object: actor.storage_key(),
                owner: lease.id.clone(),
                owner_epoch: epoch,
                home_region: region.into(),
                state_version: 0,
                state_object: None,
                last_request_id: None,
            },
            stream: ReplicaStream {
                prefix: name.strip_suffix("1.json").unwrap().into(),
                owner_epoch: epoch,
                base_version: 0,
            },
            replicas: Vec::new(),
            base: None,
            active: false,
            predecessor,
            mutation: uuid::Uuid::new_v4().to_string(),
        })
    }

    async fn activate(&self, mut record: Ownership) -> Result<ObjectPlacement> {
        if let Some(previous) = &record.predecessor {
            record.base = self.latest(previous, true).await?;
            if let Some(snapshot) = &record.base {
                let bytes = self.recover(previous, snapshot).await?;
                self.persist(&record.placement.home_region, &snapshot.object, bytes)
                    .await?;
            }
        }
        apply_snapshot(&mut record.placement, record.base.as_ref());
        record.stream.base_version = record.placement.state_version;
        record.replicas = self.initialize_replicas(&record).await;
        let (generation, current) = self
            .load(&record.actor.storage_key())
            .await?
            .context("pending ownership disappeared")?;
        ensure!(
            current.mutation == record.mutation,
            "recovery claimant was replaced"
        );
        record.predecessor = None;
        record.active = true;
        record.mutation = uuid::Uuid::new_v4().to_string();
        ensure!(
            replace(
                self.authority.as_ref(),
                &ownership_key(&record.actor.storage_key()),
                Some(generation),
                serde_json::to_vec(&record)?
            )
            .await?,
            "ownership activation lost"
        );
        Ok(record.placement)
    }

    async fn initialize_replicas(&self, record: &Ownership) -> Vec<ReplicaTarget> {
        if self.count == 0 {
            return Vec::new();
        }
        let result = async {
            let replicas = self
                .fleet
                .ensure(&record.actor, &record.placement.home_region, self.count)
                .await?;
            ReplicationTicket {
                required_replicas: self.count,
                replicas: replicas.clone(),
                archive_url: "initialization".into(),
            }
            .validate()?;
            let mut pending = JoinSet::new();
            for peer in &replicas {
                let peers = self.peers.clone();
                let target = peer.clone();
                let stream = record.stream.clone();
                pending.spawn(async move { peers.initialize(&target, &stream).await });
            }
            while let Some(result) = pending.join_next().await {
                result??;
            }
            anyhow::Ok(replicas)
        };
        match tokio::time::timeout(Duration::from_secs(20), result).await {
            Ok(Ok(replicas)) => replicas,
            error => {
                tracing::warn!(?error, "new ownership epoch will use object storage");
                Vec::new()
            }
        }
    }

    async fn latest(&self, record: &Ownership, seal: bool) -> Result<Option<SnapshotRef>> {
        let mut latest = record.base.clone();
        let mut pending = JoinSet::new();
        for target in &record.replicas {
            let peers = self.peers.clone();
            let target = target.clone();
            let stream = record.stream.clone();
            pending.spawn(async move {
                if seal {
                    peers.seal(&target, &stream).await
                } else {
                    peers.head(&target, &stream).await
                }
            });
        }
        let mut witnesses = 0;
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(head)) = result {
                ensure!(
                    head.stream == record.stream,
                    "replica returned another stream"
                );
                if head.initialized && (!seal || head.sealed) {
                    witnesses += 1;
                    advance(&mut latest, head.latest)?;
                }
            }
        }
        ensure!(
            record.replicas.is_empty() || witnesses > 0,
            "no complete replica witness; refusing to lose acknowledged state"
        );
        let bucket = self.bucket(&record.placement.home_region)?;
        let newest = bucket
            .list(&record.stream.prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                let version = key
                    .strip_prefix(&record.stream.prefix)?
                    .strip_suffix(".json")?
                    .parse::<u64>()
                    .ok()?;
                Some((version, key))
            })
            .max_by_key(|(version, _)| *version);
        if let Some((version, key)) = newest
            && latest
                .as_ref()
                .is_none_or(|snapshot| version >= snapshot.state_version)
        {
            let object = bucket
                .get(&key)
                .await?
                .context("listed snapshot disappeared")?;
            let snapshot = record.stream.snapshot(&object.bytes)?;
            ensure!(
                snapshot.object == key,
                "snapshot version does not match its object"
            );
            advance(&mut latest, Some(snapshot))?;
        }
        Ok(latest)
    }

    async fn recover(&self, record: &Ownership, snapshot: &SnapshotRef) -> Result<Vec<u8>> {
        if let Some(object) = self
            .bucket(&record.placement.home_region)?
            .get(&snapshot.object)
            .await?
        {
            snapshot.verify(&object.bytes)?;
            return Ok(object.bytes);
        }
        for peer in &record.replicas {
            if let Ok(bytes) = self.peers.read(peer, &snapshot.object).await {
                snapshot.verify(&bytes)?;
                return Ok(bytes);
            }
        }
        anyhow::bail!("acknowledged snapshot is unavailable")
    }

    async fn persist(&self, region: &str, object: &str, bytes: Vec<u8>) -> Result<()> {
        ensure!(
            replace(self.bucket(region)?.as_ref(), object, None, bytes).await?,
            "conflicting immutable snapshot"
        );
        Ok(())
    }

    async fn current_placement(&self, record: &Ownership) -> Result<ObjectPlacement> {
        let mut placement = record.placement.clone();
        if record.active {
            apply_snapshot(&mut placement, self.latest(record, false).await?.as_ref());
        }
        Ok(placement)
    }

    async fn write_snapshot(&self, grant: &ReplicaGrant, bytes: Vec<u8>) -> Result<()> {
        let stream = grant.stream.as_ref().context("missing write stream")?;
        let snapshot = stream.snapshot(&bytes)?;
        self.persist(&grant.region, &snapshot.object, bytes).await?;
        if grant.operation == "RUNTIME_WRITE" {
            let actor = actor_from_object(&snapshot.object)?;
            let (_, record) = self
                .load(&actor.storage_key())
                .await?
                .context("actor ownership is missing")?;
            ensure!(
                record.active && record.stream == *stream,
                "snapshot writer has been fenced"
            );
        }
        Ok(())
    }

    async fn load(&self, object: &ActorStorageKey) -> Result<Option<(i64, Ownership)>> {
        self.authority
            .get(&ownership_key(object))
            .await?
            .map(|value| Ok((value.generation, serde_json::from_slice(&value.bytes)?)))
            .transpose()
    }

    fn bucket(&self, region: &str) -> Result<&Arc<dyn Bucket>> {
        self.states
            .get(region)
            .context("actor region has no bucket")
    }
}

async fn read(
    State(runtime): State<Arc<RuntimeStorage>>,
    Query(query): Query<AccessQuery>,
) -> Result<Bytes, StatusCode> {
    let grant = runtime
        .access
        .verify(&query.token, "RUNTIME_READ")
        .map_err(|_| StatusCode::FORBIDDEN)?;
    let result = async {
        if let Some(object) = runtime.bucket(&grant.region)?.get(&grant.object).await? {
            return Ok(Bytes::from(object.bytes));
        }
        let actor = actor_from_object(&grant.object)?;
        let (_, record) = runtime
            .load(&actor.storage_key())
            .await?
            .context("missing ownership")?;
        let mut pending = JoinSet::new();
        for peer in record.replicas {
            let peers = runtime.peers.clone();
            let object = grant.object.clone();
            pending.spawn(async move { peers.read(&peer, &object).await });
        }
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(bytes)) = result {
                return Ok(Bytes::from(bytes));
            }
        }
        anyhow::bail!("snapshot unavailable")
    }
    .await;
    result.map_err(unavailable)
}

async fn write(
    State(runtime): State<Arc<RuntimeStorage>>,
    Query(query): Query<AccessQuery>,
    bytes: Bytes,
) -> Result<StatusCode, StatusCode> {
    let grant = runtime
        .access
        .verify(&query.token, "RUNTIME_WRITE")
        .or_else(|_| runtime.access.verify(&query.token, "RUNTIME_ARCHIVE_WRITE"))
        .map_err(|_| StatusCode::FORBIDDEN)?;
    runtime
        .write_snapshot(&grant, bytes.to_vec())
        .await
        .map_err(unavailable)?;
    Ok(StatusCode::CREATED)
}

async fn archive(
    State(runtime): State<Arc<RuntimeStorage>>,
    Query(query): Query<AccessQuery>,
) -> Result<Json<ArchiveTicket>, StatusCode> {
    let mut capability = runtime
        .access
        .verify(&query.token, "RUNTIME_ARCHIVE")
        .map_err(|_| StatusCode::FORBIDDEN)?;
    let object = query.object.ok_or(StatusCode::BAD_REQUEST)?;
    let stream = capability.stream.as_ref().ok_or(StatusCode::FORBIDDEN)?;
    let version = object
        .strip_prefix(&stream.prefix)
        .and_then(|value| value.strip_suffix(".json"))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(StatusCode::FORBIDDEN)?;
    if object != stream.object(version) {
        return Err(StatusCode::FORBIDDEN);
    }
    capability.operation = "RUNTIME_ARCHIVE_WRITE".into();
    capability.expires_at_ms = grant("", &capability.region, &object, 60_000)
        .map_err(unavailable)?
        .expires_at_ms;
    let write_url = runtime
        .access
        .url(&runtime.origin, "runtime", &capability)
        .map_err(unavailable)?;
    let read_url = runtime
        .read_url(&capability.region, &object)
        .await
        .map_err(unavailable)?;
    Ok(Json(ArchiveTicket {
        write_url,
        read_url,
    }))
}

fn ownership_key(object: &ActorStorageKey) -> String {
    format!("runtime/owners/{}.json", component(object.as_str()))
}

fn apply_snapshot(placement: &mut ObjectPlacement, snapshot: Option<&SnapshotRef>) {
    if let Some(snapshot) = snapshot {
        placement.state_version = snapshot.state_version;
        placement.state_object = Some(snapshot.object.clone());
        placement.last_request_id = Some(snapshot.request_id.clone());
    }
}

fn advance(current: &mut Option<SnapshotRef>, candidate: Option<SnapshotRef>) -> Result<()> {
    if let Some(candidate) = candidate {
        if let Some(current) = current.as_ref() {
            ensure!(
                current.state_version != candidate.state_version || current == &candidate,
                "conflicting snapshot witnesses"
            );
        }
        if current
            .as_ref()
            .is_none_or(|current| candidate.state_version > current.state_version)
        {
            *current = Some(candidate);
        }
    }
    Ok(())
}

fn actor_from_object(object: &str) -> Result<ActorKey> {
    let parts: Vec<_> = object.split('/').collect();
    ensure!(
        parts.len() == 7 && parts[0] == "snapshots",
        "invalid snapshot object"
    );
    let actor = ActorKey {
        namespace_id: parts[3].into(),
        actor_type: parts[4].into(),
        actor_id: parts[5].into(),
    };
    actor.validate()?;
    Ok(actor)
}

fn unavailable(error: anyhow::Error) -> StatusCode {
    tracing::warn!(%error, "bucket runtime operation failed");
    StatusCode::SERVICE_UNAVAILABLE
}
