use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    host::HostId,
    host_leases::HostLease,
    ownership::{Ownership, apply_snapshot},
    payload::Payload,
    placement::ObjectPlacement,
    storage::{SnapshotReader, SnapshotRef, VerifiedSnapshot, WritePlan, snapshot_position},
};

use super::{Bucket, replace};

mod activation;
pub(crate) use activation::CachedBase;
mod history;
pub(crate) use history::ActorStateReader;
mod session;

#[cfg(test)]
#[path = "../../tests/unit/bucket/activation.rs"]
mod activation_tests;
pub struct RuntimeStorage {
    reader: RuntimeStorageReader,
    owned: Mutex<HashMap<String, Ownership>>,
    uploaded: Mutex<HashMap<String, UploadedSnapshots>>,
}

pub struct RuntimeStorageReader {
    restore: Arc<dyn crate::litestream::DatabaseRestore>,
    pub(crate) clock: Arc<dyn crate::clock::Clock>,
    authority: Arc<dyn Bucket>,
    snapshots: Arc<dyn super::SnapshotStore>,
    pub(crate) persistence: super::SnapshotStoreConfig,
}

pub struct LoadedActor {
    pub placement: ObjectPlacement,
    pub state: Option<VerifiedSnapshot>,
    /// The head's reference, also when `state` was not loaded.
    pub head: Option<SnapshotRef>,
}

#[derive(Default)]
struct UploadedSnapshots {
    started: u64,
    completed: u64,
    latest: Option<SnapshotRef>,
}

impl RuntimeStorage {
    pub fn new(authority: Arc<dyn Bucket>, clock: Arc<dyn crate::clock::Clock>) -> Result<Self> {
        Ok(Self {
            reader: RuntimeStorageReader::new(
                authority,
                clock,
                Arc::new(crate::litestream::EmbeddedRestore),
            )?,
            owned: Mutex::new(HashMap::new()),
            uploaded: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn with_persistence(
        mut self,
        config: super::SnapshotStoreConfig,
        snapshots: Arc<dyn super::SnapshotStore>,
    ) -> Result<Self> {
        self.reader = self.reader.with_persistence(config, snapshots)?;
        Ok(self)
    }
}

impl std::ops::Deref for RuntimeStorage {
    type Target = RuntimeStorageReader;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl RuntimeStorageReader {
    pub(crate) async fn restored_elsewhere(
        &self,
        snapshot: &VerifiedSnapshot,
        progress: super::recovery::CheckpointProgress,
        base: Option<super::BaseName>,
    ) -> Result<()> {
        self.snapshots
            .restored_elsewhere(snapshot, progress, base)
            .await
    }

    pub(crate) async fn read_sqlite(
        &self,
        snapshot: VerifiedSnapshot,
        download: Option<&std::path::Path>,
    ) -> Result<super::recovery::RestoredSqlite> {
        self.snapshots
            .restore_sqlite(snapshot, super::Bases::Newest(None), download)
            .await
    }

    pub(crate) fn new(
        authority: Arc<dyn Bucket>,
        clock: Arc<dyn crate::clock::Clock>,
        restore: Arc<dyn crate::litestream::DatabaseRestore>,
    ) -> Result<Self> {
        Ok(Self {
            restore,
            clock,
            snapshots: Arc::new(super::BucketSnapshots(authority.clone())),
            persistence: super::SnapshotStoreConfig::Local,
            authority,
        })
    }

    pub(crate) fn with_persistence(
        mut self,
        config: super::SnapshotStoreConfig,
        snapshots: Arc<dyn super::SnapshotStore>,
    ) -> Result<Self> {
        config.validate()?;
        self.persistence = config;
        self.snapshots = snapshots;
        Ok(self)
    }
}

#[async_trait]
impl SnapshotReader for RuntimeStorageReader {
    async fn read_snapshot(&self, _region: &str, object: &str) -> Result<Payload> {
        self.read_persisted(object)
            .await?
            .context("snapshot unavailable")
    }
}

impl RuntimeStorage {
    pub(crate) async fn activate_actor(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        region: &str,
    ) -> Result<LoadedActor> {
        let record = self.owned_record(actor, lease)?;
        ensure!(record.region == region, "actor ownership changed");
        self.load_actor(record).await
    }

    pub(crate) async fn load_owned_actor(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        epoch: u64,
    ) -> Result<LoadedActor> {
        let record = self.owned_record(actor, lease)?;
        ensure!(record.epoch == epoch, "actor ownership changed");
        self.load_actor(record).await
    }

    fn owned_record(&self, actor: &ActorKey, lease: &HostLease) -> Result<Ownership> {
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        ensure!(
            record.lease.id == lease.id && record.lease.session_id == lease.session_id,
            "actor ownership changed"
        );
        Ok(record)
    }

    pub async fn prepare_actor_write(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        epoch: u64,
        version: u64,
    ) -> Result<WritePlan> {
        let record = self.owned_record(actor, lease)?;
        ensure!(record.epoch == epoch, "actor ownership changed");
        self.write_plan(&record, version)
    }

    async fn load_actor(&self, record: Ownership) -> Result<LoadedActor> {
        let snapshot = self.latest(&record, None).await?;
        Ok(self.remember(record, snapshot, None))
    }

    fn remember(
        &self,
        record: Ownership,
        snapshot: Option<VerifiedSnapshot>,
        head: Option<SnapshotRef>,
    ) -> LoadedActor {
        let mut placement = record.placement();
        let head = snapshot.as_ref().map(|s| s.reference.clone()).or(head);
        apply_snapshot(&mut placement, head.as_ref());
        if let Some(head) = &head {
            self.uploaded
                .lock()
                .unwrap()
                .entry(record.lease.session_id.clone())
                .or_default()
                .latest = Some(head.clone());
        }
        self.owned
            .lock()
            .unwrap()
            .insert(record.actor.storage_key().as_str().into(), record);
        LoadedActor {
            placement,
            state: snapshot,
            head,
        }
    }

    fn write_plan(&self, record: &Ownership, version: u64) -> Result<WritePlan> {
        ensure!(version > 0, "state version must be positive");
        let stream = record.stream()?;
        Ok(WritePlan {
            state_version: version,
            object_name: stream.object(version),
            stream,
        })
    }
}

impl RuntimeStorageReader {
    async fn latest(
        &self,
        record: &Ownership,
        known: Option<VerifiedSnapshot>,
    ) -> Result<Option<VerifiedSnapshot>> {
        let latest = self
            .snapshots
            .latest(&record.stream()?.prefix)
            .await?
            .map(|(key, bytes)| VerifiedSnapshot::named(key, bytes))
            .transpose()?;
        self.load_latest(record, latest.or(known)).await
    }

    async fn load_latest(
        &self,
        record: &Ownership,
        mut loaded: Option<VerifiedSnapshot>,
    ) -> Result<Option<VerifiedSnapshot>> {
        let mut candidate = record.base.clone();
        advance(&mut candidate, loaded.as_ref().map(|s| s.reference.clone()))?;
        if let Some(snapshot) = candidate
            && loaded.as_ref().is_none_or(|s| s.reference != snapshot)
        {
            let bytes = self
                .read_persisted(&snapshot.object)
                .await?
                .context("acknowledged snapshot is unavailable")?;
            loaded = Some(VerifiedSnapshot::matching(snapshot, bytes)?);
        }
        Ok(loaded)
    }

    async fn persist(&self, object: &str, bytes: Payload) -> Result<()> {
        self.snapshots.put(object, bytes).await
    }

    async fn read_persisted(&self, object: &str) -> Result<Option<Payload>> {
        self.snapshots.get(object).await
    }

    /// The newest state a session that was never sealed wrote, which recovering also keeps.
    pub(crate) async fn recover_session(&self, record: &Ownership) -> Result<Option<SnapshotRef>> {
        Ok(self
            .snapshots
            .recover(&record.stream()?.prefix)
            .await?
            .map(|(object, bytes)| VerifiedSnapshot::named(object, bytes))
            .transpose()?
            .map(|snapshot| snapshot.reference))
    }

    pub(crate) async fn current_placement(&self, record: &Ownership) -> Result<ObjectPlacement> {
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
}

#[async_trait]
impl SnapshotReader for RuntimeStorage {
    async fn read_snapshot(&self, region: &str, object: &str) -> Result<Payload> {
        self.reader.read_snapshot(region, object).await
    }
}

pub(crate) fn advance(
    current: &mut Option<SnapshotRef>,
    candidate: Option<SnapshotRef>,
) -> Result<()> {
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

#[async_trait]
impl crate::state_transport::SnapshotWriter for RuntimeStorage {
    async fn write_snapshot(
        &self,
        plan: &WritePlan,
        bytes: Payload,
    ) -> Result<crate::state_transport::StateWrite> {
        self.ensure_write_owner(plan)?;
        let stream = &plan.stream;
        let snapshot = stream.snapshot(&bytes)?;
        ensure!(
            snapshot.object == plan.object_name && snapshot.state_version == plan.state_version,
            "write plan does not match snapshot"
        );
        if let Some(parent) = crate::state_log::StateSnapshot::decode(&bytes)?
            .sqlite
            .parent
            && !self.uploaded_latest(&stream.session, &parent)
        {
            let bytes = self
                .read_persisted(&parent.object)
                .await?
                .context("SQLite dependency missing")?;
            parent.verify(&bytes)?;
        }
        self.uploaded
            .lock()
            .unwrap()
            .entry(stream.session.clone())
            .or_default()
            .started += 1;
        self.persist(&snapshot.object, bytes).await?;
        self.ensure_write_owner(plan)?;
        let mut uploaded = self.uploaded.lock().unwrap();
        let uploaded = uploaded.entry(stream.session.clone()).or_default();
        advance(&mut uploaded.latest, Some(snapshot))?;
        uploaded.completed += 1;
        Ok(crate::state_transport::StateWrite::Written)
    }
}

impl RuntimeStorage {
    /// A parent this session restored or uploaded itself is already verified, so it needs no
    /// download.
    fn uploaded_latest(&self, session: &str, parent: &SnapshotRef) -> bool {
        self.uploaded
            .lock()
            .unwrap()
            .get(session)
            .and_then(|uploaded| uploaded.latest.as_ref())
            == Some(parent)
    }

    fn ensure_write_owner(&self, plan: &WritePlan) -> Result<()> {
        let actor = crate::storage_paths::actor_from_snapshot(&plan.object_name)?;
        let owned = self.owned.lock().unwrap();
        let record = owned
            .get(actor.storage_key().as_str())
            .context("actor is not locally activated")?;
        ensure!(
            record.stream()? == plan.stream && record.lease.expires_at_ms > self.clock.now_ms()?,
            "actor write authority expired or changed"
        );
        Ok(())
    }
}
