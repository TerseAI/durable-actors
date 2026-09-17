use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

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
    storage_urls::{StateWriteTicket, StorageUrlSigner, snapshot_object_name, snapshot_prefix},
};

use super::{Bucket, ReplicaPeers, component, peers::grant, replace};

mod session;

pub struct RuntimeStorage {
    owned: Mutex<HashMap<String, Ownership>>,
    sessions: tokio::sync::Mutex<HashMap<String, session::Session>>,
    authority: Arc<dyn Bucket>,
    states: HashMap<String, Arc<dyn Bucket>>,
    leases: Arc<dyn HostLeaseStore>,
    fleet: Arc<dyn ReplicaProvisioner>,
    peers: Arc<dyn ReplicaPeers>,
    access: ReplicaAccess,
    origin: String,
    count: usize,
    http: crate::state_transport::HttpStateTransport,
}

#[derive(Clone, Serialize, Deserialize)]
struct Ownership {
    actor: ActorKey,
    owner: HostId,
    session: String,
    epoch: u64,
    region: String,
    base: Option<SnapshotRef>,
}

pub(crate) struct LoadedActor {
    pub placement: ObjectPlacement,
    pub state: Option<Bytes>,
    acquired: bool,
}

struct LoadedSnapshot {
    reference: SnapshotRef,
    bytes: Bytes,
}

impl Ownership {
    fn placement(&self) -> ObjectPlacement {
        let mut placement = ObjectPlacement {
            object: self.actor.storage_key(),
            owner: self.owner.clone(),
            owner_epoch: self.epoch,
            home_region: self.region.clone(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        };
        apply_snapshot(&mut placement, self.base.as_ref());
        placement
    }

    fn stream(&self) -> Result<ReplicaStream> {
        let name = snapshot_object_name(&self.actor, 1, &format!("{:032x}", self.epoch))?;
        Ok(ReplicaStream {
            session: session::identity(&self.actor.namespace_id, &self.owner, &self.session),
            prefix: name.strip_suffix("1.json").unwrap().into(),
            owner_epoch: self.epoch,
            base_version: self.base.as_ref().map_or(0, |base| base.state_version),
        })
    }
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
            owned: Mutex::new(HashMap::new()),
            sessions: tokio::sync::Mutex::new(HashMap::new()),
            authority,
            states,
            leases,
            fleet,
            peers,
            access,
            origin,
            count,
            http: crate::state_transport::HttpStateTransport::new(),
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
        Ok(self
            .load(object)
            .await?
            .map(|(_, record)| record.placement()))
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
                if namespace.is_none_or(|id| id == record.actor.namespace_id)
                    && after.is_none_or(|id| record.actor.storage_key().as_str() > id)
                {
                    records.push(record);
                }
            }
        }
        records.sort_by_key(|record| record.actor.storage_key().as_str().to_owned());
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
        let status = self.leases.lease_status(owner).await?;
        ensure!(status.is_active(), "new owner has no active lease");
        let loaded = self
            .acquire(actor, Some(expected), &status.lease.unwrap(), region)
            .await?;
        Ok(if loaded.acquired {
            PlacementClaim::Acquired(loaded.placement)
        } else {
            PlacementClaim::Current(loaded.placement)
        })
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
                record.epoch == placement.owner_epoch
                    && record.owner == lease.id
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
            record.actor == *actor && record.region == region,
            "actor scope mismatch"
        );
        let lease = self.live_lease(&record).await?;
        let replicas = self
            .prepare_session(&actor.namespace_id, &lease, region)
            .await?;
        self.sign_write(&record, version, &replicas)
    }
}

impl RuntimeStorage {
    pub(crate) async fn activate_actor(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        region: &str,
    ) -> Result<LoadedActor> {
        self.acquire(actor, None, lease, region).await
    }

    pub(crate) async fn load_owned_actor(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        epoch: u64,
    ) -> Result<LoadedActor> {
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor has no ownership")?;
        ensure!(
            record.owner == lease.id && record.session == lease.session_id && record.epoch == epoch,
            "actor ownership changed"
        );
        self.load_actor(record, None).await
    }

    pub(crate) async fn prepare_actor_write(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        epoch: u64,
        version: u64,
    ) -> Result<StateWriteTicket> {
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        ensure!(
            record.owner == lease.id && record.session == lease.session_id && record.epoch == epoch,
            "actor ownership changed"
        );
        let replicas = self
            .prepare_session(&actor.namespace_id, lease, &record.region)
            .await?;
        self.sign_write(&record, version, &replicas)
    }

    async fn acquire(
        &self,
        actor: &ActorKey,
        expected: Option<Option<&ObjectPlacement>>,
        lease: &HostLease,
        region: &str,
    ) -> Result<LoadedActor> {
        actor.validate()?;
        self.bucket(region)?;
        let current = self.load(&actor.storage_key()).await?;
        if let Some(expected) = expected {
            ensure!(
                expected.is_none_or(|p| p.object == actor.storage_key()),
                "expected placement belongs to another actor"
            );
            ensure!(
                current.as_ref().map(|(_, r)| (&r.owner, r.epoch))
                    == expected.map(|p| (&p.owner, p.owner_epoch)),
                "ownership changed concurrently"
            );
        }
        let mut recovered = None;
        if let Some((_, record)) = &current {
            ensure!(
                record.actor == *actor && record.region == region,
                "ownership scope cannot change"
            );
            if record.owner == lease.id && record.session == lease.session_id {
                return self.load_actor(record.clone(), None).await;
            }
            self.ensure_previous_owner_stopped(record).await?;
            let known = self.recover_session(record).await?;
            recovered = self.latest(record, known).await?;
        }
        let epoch = current
            .as_ref()
            .map_or(Some(1), |(_, record)| record.epoch.checked_add(1))
            .context("owner epoch overflow")?;
        let record = Ownership {
            actor: actor.clone(),
            owner: lease.id.clone(),
            session: lease.session_id.clone(),
            epoch,
            region: region.into(),
            base: recovered.as_ref().map(|s| s.reference.clone()),
        };
        ensure!(
            replace(
                self.authority.as_ref(),
                &ownership_key(&actor.storage_key()),
                current.map(|(generation, _)| generation),
                serde_json::to_vec(&record)?
            )
            .await?,
            "ownership claim lost"
        );
        Ok(self.remember(record, recovered, true))
    }

    async fn ensure_previous_owner_stopped(&self, record: &Ownership) -> Result<()> {
        let status = self.leases.lease_status(&record.owner).await?;
        ensure!(
            !status.is_active()
                || status
                    .lease
                    .is_none_or(|lease| lease.session_id != record.session),
            "previous owner lease is still active"
        );
        Ok(())
    }

    async fn live_lease(&self, record: &Ownership) -> Result<HostLease> {
        let status = self.leases.lease_status(&record.owner).await?;
        ensure!(status.is_active(), "owner lease expired");
        let lease = status.lease.unwrap();
        ensure!(lease.session_id == record.session, "owner session changed");
        Ok(lease)
    }

    async fn load_actor(
        &self,
        record: Ownership,
        recovered: Option<LoadedSnapshot>,
    ) -> Result<LoadedActor> {
        let snapshot = self.latest(&record, recovered).await?;
        Ok(self.remember(record, snapshot, false))
    }

    fn remember(
        &self,
        record: Ownership,
        snapshot: Option<LoadedSnapshot>,
        acquired: bool,
    ) -> LoadedActor {
        let mut placement = record.placement();
        apply_snapshot(&mut placement, snapshot.as_ref().map(|s| &s.reference));
        self.owned
            .lock()
            .unwrap()
            .insert(record.actor.storage_key().as_str().into(), record);
        LoadedActor {
            placement,
            state: snapshot.map(|s| s.bytes),
            acquired,
        }
    }

    fn sign_write(
        &self,
        record: &Ownership,
        version: u64,
        replicas: &[ReplicaTarget],
    ) -> Result<StateWriteTicket> {
        ensure!(version > 0, "state version must be positive");
        let stream = record.stream()?;
        let mut capability = grant("RUNTIME_WRITE", &record.region, &stream.prefix, 60_000)?;
        capability.stream = Some(stream.clone());
        let url = self.access.url(&self.origin, "runtime", &capability)?;
        let expires_at_ms = i64::try_from(capability.expires_at_ms)?;
        capability.operation = "RUNTIME_ARCHIVE".into();
        capability.expires_at_ms = u64::MAX;
        let archive_url = self
            .access
            .url(&self.origin, "runtime-archive", &capability)?;
        let targets = replicas
            .iter()
            .map(|peer| {
                Ok(ReplicaTarget {
                    host_id: peer.host_id.clone(),
                    region: peer.region.clone(),
                    url: self.access.url(
                        &peer.url,
                        "stream",
                        &ReplicaGrant {
                            operation: "APPEND".into(),
                            host_id: peer.host_id.clone(),
                            archive_url: archive_url.clone(),
                            expires_at_ms: expires_at_ms as u64,
                            ..capability.clone()
                        },
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StateWriteTicket {
            state_version: version,
            object_name: stream.object(version),
            url,
            expires_at_ms,
            stream: Some(stream),
            replication: (!targets.is_empty()).then_some(ReplicationTicket {
                required_replicas: targets.len(),
                replicas: targets,
                archive_url,
            }),
        })
    }

    async fn latest(
        &self,
        record: &Ownership,
        known: Option<LoadedSnapshot>,
    ) -> Result<Option<LoadedSnapshot>> {
        let bucket = self.bucket(&record.region)?;
        let stream = record.stream()?;
        let newest = bucket
            .list(&stream.prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                snapshot_position(&key)
                    .filter(|(epoch, _)| *epoch == record.epoch)
                    .map(|position| (position, key))
            })
            .max_by_key(|(position, _)| *position);
        let mut loaded = match newest {
            Some((_, key)) if known.as_ref().is_some_and(|s| s.reference.object == key) => known,
            Some((_, key)) => {
                let object = bucket
                    .get(&key)
                    .await?
                    .context("listed snapshot disappeared")?;
                Some(decode_snapshot(key, object.bytes)?)
            }
            None => known,
        };
        let mut candidate = record.base.clone();
        advance(&mut candidate, loaded.as_ref().map(|s| s.reference.clone()))?;
        let replicas = self.session_replicas(record).await?;
        let mut pending = JoinSet::new();
        for target in &replicas {
            let (peers, target, stream) = (self.peers.clone(), target.clone(), stream.clone());
            pending.spawn(async move { peers.head(&target, &stream).await });
        }
        let mut witnesses = 0;
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(head)) = result {
                ensure!(head.stream == stream, "replica returned another stream");
                witnesses += 1;
                advance(&mut candidate, head.latest)?;
            }
        }
        ensure!(
            replicas.is_empty() || witnesses > 0,
            "no complete replica witness; refusing to lose acknowledged state"
        );
        if let Some(snapshot) = candidate {
            if loaded.as_ref().is_none_or(|s| s.reference != snapshot) {
                let bytes = self
                    .recover_snapshot(&record.region, &replicas, &snapshot)
                    .await?;
                loaded = Some(LoadedSnapshot {
                    reference: snapshot,
                    bytes: bytes.into(),
                });
            }
        }
        Ok(loaded)
    }

    async fn recover_snapshot(
        &self,
        region: &str,
        replicas: &[ReplicaTarget],
        snapshot: &SnapshotRef,
    ) -> Result<Vec<u8>> {
        if let Some(object) = self.bucket(region)?.get(&snapshot.object).await? {
            snapshot.verify(&object.bytes)?;
            return Ok(object.bytes);
        }
        for peer in replicas {
            if let Ok(bytes) = self.peers.read(peer, &snapshot.object).await {
                snapshot.verify(&bytes)?;
                self.persist(region, &snapshot.object, bytes.clone())
                    .await?;
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
        let mut placement = record.placement();
        apply_snapshot(
            &mut placement,
            self.latest(record, None)
                .await?
                .as_ref()
                .map(|s| &s.reference),
        );
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
                record.stream()? == *stream,
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

    async fn read_snapshot(&self, grant: &ReplicaGrant) -> Result<Bytes> {
        if let Some(object) = self.bucket(&grant.region)?.get(&grant.object).await? {
            return Ok(Bytes::from(object.bytes));
        }
        let actor = actor_from_object(&grant.object)?;
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("missing ownership")?;
        let mut pending = JoinSet::new();
        for peer in self.session_replicas(&record).await? {
            let peers = self.peers.clone();
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
    runtime.read_snapshot(&grant).await.map_err(unavailable)
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
    format!(
        "runtime/owners/{}/{}.json",
        component(
            object
                .as_str()
                .strip_prefix("object.v2.")
                .and_then(|key| key.split_once(':'))
                .map_or("", |(ns, _)| ns)
        ),
        component(object.as_str())
    )
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
                snapshot_position(&current.object) != snapshot_position(&candidate.object)
                    || current == &candidate,
                "conflicting snapshot witnesses"
            );
        }
        if current.as_ref().is_none_or(|current| {
            snapshot_position(&candidate.object) > snapshot_position(&current.object)
        }) {
            *current = Some(candidate);
        }
    }
    Ok(())
}

fn snapshot_position(object: &str) -> Option<(u64, u64)> {
    let mut parts = object.rsplit('/');
    let version = parts.next()?.strip_suffix(".json")?.parse().ok()?;
    let epoch = u64::from_str_radix(parts.next()?, 16).ok()?;
    Some((epoch, version))
}

fn decode_snapshot(object: String, bytes: Vec<u8>) -> Result<LoadedSnapshot> {
    let snapshot = crate::state_log::StateSnapshot::decode(&bytes)?;
    ensure!(
        snapshot_position(&object) == Some((snapshot.owner_epoch, snapshot.state_version)),
        "snapshot identity does not match its object"
    );
    Ok(LoadedSnapshot {
        reference: SnapshotRef::new(object, &snapshot, &bytes),
        bytes: bytes.into(),
    })
}

fn actor_from_object(object: &str) -> Result<ActorKey> {
    let parts: Vec<_> = object.split('/').collect();
    ensure!(
        parts.len() == 7 && parts[0] == "snapshots",
        "invalid snapshot object"
    );
    let actor = ActorKey {
        namespace_id: parts[1].into(),
        actor_type: parts[3].into(),
        actor_id: parts[4].into(),
    };
    actor.validate()?;
    Ok(actor)
}

fn unavailable(error: anyhow::Error) -> StatusCode {
    tracing::warn!(%error, "bucket runtime operation failed");
    StatusCode::SERVICE_UNAVAILABLE
}

#[async_trait]
impl crate::state_transport::StateTransport for RuntimeStorage {
    async fn read(&self, url: &str) -> Result<Bytes> {
        let parsed = reqwest::Url::parse(url)?;
        let token = parsed
            .query_pairs()
            .find(|(key, _)| key == "token")
            .context("state capability missing")?
            .1
            .into_owned();
        let grant = self.access.verify(&token, "RUNTIME_READ")?;
        self.read_snapshot(&grant).await
    }

    async fn write(&self, url: &str, bytes: Vec<u8>) -> Result<crate::state_transport::StateWrite> {
        let parsed = reqwest::Url::parse(url)?;
        if parsed.path() != "/_replica/runtime" {
            return self.http.write(url, bytes).await;
        }
        let token = parsed
            .query_pairs()
            .find(|(key, _)| key == "token")
            .context("state capability missing")?
            .1
            .into_owned();
        let grant = self.access.verify(&token, "RUNTIME_WRITE")?;
        self.write_snapshot(&grant, bytes).await?;
        Ok(crate::state_transport::StateWrite::Written)
    }
}
