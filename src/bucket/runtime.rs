use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::grpc::{
    proto,
    transport::{MAX_STORAGE_MESSAGE_BYTES, token, unavailable},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};

use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    host::HostId,
    host_leases::{HostLease, HostLeaseStore},
    placement::{ObjectPlacement, ObjectPlacementStore, PlacementClaim},
    replication::{
        ReplicaAccess, ReplicaGrant, ReplicaProvisioner, ReplicaStream, ReplicaTarget,
        ReplicationTicket, SnapshotRef,
    },
    storage::{SnapshotReader, WritePlan, snapshot_object_name, snapshot_prefix},
};

use super::{Bucket, ReplicaPeers, peers::grant, replace};

mod session;

pub struct RuntimeStorage {
    owned: Mutex<HashMap<String, Ownership>>,
    sessions: tokio::sync::Mutex<HashMap<String, session::Session>>,
    authority: Arc<dyn Bucket>,
    leases: Arc<dyn HostLeaseStore>,
    fleet: Arc<dyn ReplicaProvisioner>,
    peers: Arc<dyn ReplicaPeers>,
    access: ReplicaAccess,
    origin: String,
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
            session: session::identity(&self.owner, &self.session),
            prefix: name.strip_suffix("1.json").unwrap().into(),
            owner_epoch: self.epoch,
            base_version: self.base.as_ref().map_or(0, |base| base.state_version),
        })
    }
}

impl RuntimeStorage {
    pub fn new(
        authority: Arc<dyn Bucket>,
        leases: Arc<dyn HostLeaseStore>,
        fleet: Arc<dyn ReplicaProvisioner>,
        peers: Arc<dyn ReplicaPeers>,
        access: ReplicaAccess,
        origin: String,
    ) -> Result<Self> {
        ensure!(
            fleet.replica_regions().len() <= crate::replication::MAX_REPLICAS,
            "invalid runtime storage configuration"
        );
        Ok(Self {
            owned: Mutex::new(HashMap::new()),
            sessions: tokio::sync::Mutex::new(HashMap::new()),
            authority,
            leases,
            fleet,
            peers,
            access,
            origin,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        let service = RuntimeStorageService(self);
        tonic::service::Routes::from(Router::new())
            .add_service(
                proto::snapshot_service_server::SnapshotServiceServer::new(service.clone())
                    .max_decoding_message_size(MAX_STORAGE_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_STORAGE_MESSAGE_BYTES),
            )
            .add_service(
                proto::archive_service_server::ArchiveServiceServer::new(service)
                    .max_decoding_message_size(MAX_STORAGE_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_STORAGE_MESSAGE_BYTES),
            )
            .into_axum_router()
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
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ObjectPlacement>> {
        let mut records = Vec::new();
        for key in self
            .authority
            .list(&format!("{}owners/", crate::storage_paths::ROOT))
            .await?
        {
            if !key.contains("/owners/") {
                continue;
            }
            if let Some(object) = self.authority.get(&key).await? {
                let record: Ownership = serde_json::from_slice(&object.bytes)?;
                if after.is_none_or(|id| record.actor.storage_key().as_str() > id) {
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
}

#[async_trait]
impl SnapshotReader for RuntimeStorage {
    async fn read_snapshot(&self, region: &str, object: &str) -> Result<Bytes> {
        self.fetch_snapshot(&grant("RUNTIME_READ", region, object, 60_000)?)
            .await
    }
}

impl RuntimeStorage {
    pub async fn claim_actor(
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

    pub async fn read_url(&self, region: &str, object: &str) -> Result<String> {
        self.access.url(
            &self.origin,
            &grant("RUNTIME_READ", region, object, 60_000)?,
        )
    }
    pub async fn prepare_write(
        &self,
        region: &str,
        actor: &ActorKey,
        version: u64,
    ) -> Result<WritePlan> {
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor has no ownership")?;
        ensure!(
            record.actor == *actor && record.region == region,
            "actor scope mismatch"
        );
        let lease = self.live_lease(&record).await?;
        let replicas = self.prepare_session(&lease, region).await?;
        self.write_plan(&record, version, &replicas)
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
    ) -> Result<WritePlan> {
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
        let replicas = self.prepare_session(lease, &record.region).await?;
        self.write_plan(&record, version, &replicas)
    }

    async fn acquire(
        &self,
        actor: &ActorKey,
        expected: Option<Option<&ObjectPlacement>>,
        lease: &HostLease,
        region: &str,
    ) -> Result<LoadedActor> {
        actor.validate()?;
        crate::placement::validate_region(region)?;
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
                &ownership_key(&actor.storage_key())?,
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

    fn write_plan(
        &self,
        record: &Ownership,
        version: u64,
        replicas: &[ReplicaTarget],
    ) -> Result<WritePlan> {
        ensure!(version > 0, "state version must be positive");
        let stream = record.stream()?;
        let mut capability = grant("RUNTIME_ARCHIVE", &record.region, &stream.prefix, 60_000)?;
        capability.stream = Some(stream.clone());
        let expires_at_ms = i64::try_from(capability.expires_at_ms)?;
        capability.expires_at_ms = u64::MAX;
        let archive_url = self.access.url(&self.origin, &capability)?;
        let targets = replicas
            .iter()
            .map(|peer| {
                Ok(ReplicaTarget {
                    host_id: peer.host_id.clone(),
                    region: peer.region.clone(),
                    url: self.access.url(
                        &peer.url,
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
        Ok(WritePlan {
            state_version: version,
            object_name: stream.object(version),
            expires_at_ms,
            stream,
            replication: (!targets.is_empty()).then_some(ReplicationTicket {
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
        let bucket = &self.authority;
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
        if let Some(snapshot) = candidate
            && loaded.as_ref().is_none_or(|s| s.reference != snapshot)
        {
            let bytes = self.recover_snapshot(&replicas, &snapshot).await?;
            loaded = Some(LoadedSnapshot {
                reference: snapshot,
                bytes: bytes.into(),
            });
        }
        Ok(loaded)
    }

    async fn recover_snapshot(
        &self,
        replicas: &[ReplicaTarget],
        snapshot: &SnapshotRef,
    ) -> Result<Vec<u8>> {
        if let Some(object) = self.authority.get(&snapshot.object).await? {
            snapshot.verify(&object.bytes)?;
            return Ok(object.bytes);
        }
        for peer in replicas {
            if let Ok(bytes) = self.peers.read(peer, &snapshot.object).await {
                snapshot.verify(&bytes)?;
                self.persist(&snapshot.object, bytes.clone()).await?;
                return Ok(bytes);
            }
        }
        anyhow::bail!("acknowledged snapshot is unavailable")
    }

    async fn persist(&self, object: &str, bytes: Vec<u8>) -> Result<()> {
        ensure!(
            replace(self.authority.as_ref(), object, None, bytes).await?,
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

    async fn archive_snapshot(&self, grant: &ReplicaGrant, bytes: Vec<u8>) -> Result<()> {
        let stream = grant.stream.as_ref().context("missing write stream")?;
        let snapshot = stream.snapshot(&bytes)?;
        self.persist(&snapshot.object, bytes).await?;

        Ok(())
    }

    async fn load(&self, object: &ActorStorageKey) -> Result<Option<(i64, Ownership)>> {
        self.authority
            .get(&ownership_key(object)?)
            .await?
            .map(|value| Ok((value.generation, serde_json::from_slice(&value.bytes)?)))
            .transpose()
    }

    async fn fetch_snapshot(&self, grant: &ReplicaGrant) -> Result<Bytes> {
        if let Some(object) = self.authority.get(&grant.object).await? {
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
}

#[derive(Clone)]
struct RuntimeStorageService(Arc<RuntimeStorage>);

#[tonic::async_trait]
impl proto::snapshot_service_server::SnapshotService for RuntimeStorageService {
    async fn read(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::SnapshotData>, Status> {
        let grant = self
            .0
            .access
            .verify(token(&request)?, "RUNTIME_READ")
            .map_err(|_| Status::permission_denied("snapshot read capability rejected"))?;
        let data = self.0.fetch_snapshot(&grant).await.map_err(unavailable)?;
        Ok(Response::new(proto::SnapshotData {
            data: data.to_vec(),
        }))
    }

    async fn write(
        &self,
        request: Request<proto::SnapshotData>,
    ) -> Result<Response<proto::SnapshotWriteReply>, Status> {
        let grant = self
            .0
            .access
            .verify(token(&request)?, "RUNTIME_ARCHIVE_WRITE")
            .map_err(|_| Status::permission_denied("snapshot archive capability rejected"))?;
        self.0
            .archive_snapshot(&grant, request.into_inner().data)
            .await
            .map_err(unavailable)?;
        Ok(Response::new(proto::SnapshotWriteReply {
            already_exists: false,
        }))
    }
}

#[tonic::async_trait]
impl proto::archive_service_server::ArchiveService for RuntimeStorageService {
    async fn prepare(
        &self,
        request: Request<proto::ArchiveRequest>,
    ) -> Result<Response<proto::ArchiveReply>, Status> {
        let mut capability = self
            .0
            .access
            .verify(token(&request)?, "RUNTIME_ARCHIVE")
            .map_err(|_| Status::permission_denied("archive capability rejected"))?;
        let object = request.into_inner().object;
        let stream = capability
            .stream
            .as_ref()
            .ok_or_else(|| Status::permission_denied("archive stream is required"))?;
        let version = object
            .strip_prefix(&stream.prefix)
            .and_then(|value| value.strip_suffix(".json"))
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| Status::permission_denied("snapshot is outside the archive stream"))?;
        if object != stream.object(version) {
            return Err(Status::permission_denied("invalid archive object"));
        }
        capability.operation = "RUNTIME_ARCHIVE_WRITE".into();
        capability.expires_at_ms = grant("", &capability.region, &object, 60_000)
            .map_err(unavailable)?
            .expires_at_ms;
        let write_url = self
            .0
            .access
            .url(&self.0.origin, &capability)
            .map_err(unavailable)?;
        let read_url = self
            .0
            .read_url(&capability.region, &object)
            .await
            .map_err(unavailable)?;
        Ok(Response::new(proto::ArchiveReply {
            write_url,
            read_url,
        }))
    }
}

fn ownership_key(object: &ActorStorageKey) -> Result<String> {
    crate::storage_paths::owner(object)
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
    crate::storage_paths::actor_from_snapshot(object)
}

#[async_trait]
impl crate::state_transport::SnapshotWriter for RuntimeStorage {
    async fn write_snapshot(
        &self,
        plan: &WritePlan,
        bytes: Vec<u8>,
    ) -> Result<crate::state_transport::StateWrite> {
        let stream = &plan.stream;
        let snapshot = stream.snapshot(&bytes)?;
        ensure!(
            snapshot.object == plan.object_name && snapshot.state_version == plan.state_version,
            "write plan does not match snapshot"
        );
        self.persist(&snapshot.object, bytes).await?;
        let actor = actor_from_object(&snapshot.object)?;
        let (_, owner) = self
            .load(&actor.storage_key())
            .await?
            .context("actor ownership missing")?;
        ensure!(
            owner.stream()? == *stream,
            "snapshot writer has been fenced"
        );
        Ok(crate::state_transport::StateWrite::Written)
    }
}

#[async_trait]
impl crate::placement::ActorInventoryReader for RuntimeStorage {
    async fn actor_inventory(&self) -> Result<Vec<crate::placement::ActorInventory>> {
        let mut actors = std::collections::BTreeMap::new();
        let mut hosts = HashMap::new();
        let prefix = format!("{}owners/", crate::storage_paths::ROOT);
        for key in self.authority.list(&prefix).await? {
            let Some(object) = self.authority.get(&key).await? else {
                continue;
            };
            let record: Ownership = serde_json::from_slice(&object.bytes)?;
            if !hosts.contains_key(&record.owner) {
                hosts.insert(
                    record.owner.clone(),
                    self.leases.inventory_status(&record.owner).await?,
                );
            }
            let (status, residents, sockets) = &hosts[&record.owner];
            let row = actors
                .entry(record.actor.actor_type.clone())
                .or_insert_with(|| crate::placement::ActorInventory {
                    actor_type: record.actor.actor_type.clone(),
                    ..Default::default()
                });
            let residency = if !status.is_active()
                || status
                    .lease
                    .as_ref()
                    .is_none_or(|lease| lease.session_id != record.session)
            {
                crate::placement::ActorResidency::Dormant
            } else if let Some(residents) = residents {
                if residents.contains(&record.actor) {
                    crate::placement::ActorResidency::Live
                } else {
                    crate::placement::ActorResidency::Dormant
                }
            } else {
                crate::placement::ActorResidency::Unknown
            };
            match residency {
                crate::placement::ActorResidency::Live => row.live += 1,
                crate::placement::ActorResidency::Dormant => row.dormant += 1,
                crate::placement::ActorResidency::Unknown => row.unknown += 1,
            }
            let connections = if status.is_active()
                && status
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.session_id == record.session)
            {
                sockets
                    .iter()
                    .find(|entry| entry.actor == record.actor)
                    .map(|entry| {
                        entry
                            .connections
                            .iter()
                            .map(|connection| crate::placement::ActorConnectionInventory {
                                id: connection.id.clone(),
                                metadata: connection.metadata.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                vec![]
            };
            row.instances
                .push(crate::placement::ActorInstanceInventory {
                    actor_id: record.actor.actor_id,
                    status: residency,
                    connections,
                });
        }
        Ok(actors
            .into_values()
            .map(|mut actor| {
                actor
                    .instances
                    .sort_by(|left, right| left.actor_id.cmp(&right.actor_id));
                actor
            })
            .collect())
    }
}
