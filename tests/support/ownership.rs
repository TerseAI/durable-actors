use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;
use async_trait::async_trait;

use super::{ActorClaim, Ownership, OwnershipHint, OwnershipStore, Ownerships, Stored, Version};
use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    bucket::{Bucket, BucketObject, RuntimeStorageReader},
    clock::{Clock, SystemClock},
    host_leases::HostLease,
};

/// Ownership records in memory, versioned like PostgreSQL rows.
#[derive(Default)]
pub(crate) struct MemoryOwnership {
    records: Mutex<HashMap<ActorKey, (i64, Ownership)>>,
    loads: AtomicUsize,
}

#[async_trait]
impl OwnershipStore for MemoryOwnership {
    async fn load(&self, actor: &ActorKey) -> Result<Option<Stored>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .records
            .lock()
            .unwrap()
            .get(actor)
            .map(|(version, record)| Stored {
                version: Version::Postgres(*version),
                record: record.clone(),
            }))
    }

    async fn replace(&self, record: &Ownership, expected: Option<Version>) -> Result<bool> {
        let mut records = self.records.lock().unwrap();
        let current = records.get(&record.actor).map(|(version, _)| *version);
        let next = match (expected, current) {
            (None, None) => 1,
            (Some(Version::Postgres(expected)), Some(current)) if expected == current => {
                current + 1
            }
            _ => return Ok(false),
        };
        records.insert(record.actor.clone(), (next, record.clone()));
        Ok(true)
    }

    async fn unsealed(&self) -> Result<Vec<Stored>> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .values()
            .filter(|(_, record)| !record.sealed)
            .map(|(version, record)| Stored {
                version: Version::Postgres(*version),
                record: record.clone(),
            })
            .collect())
    }
}

#[derive(Default)]
struct MemoryBucket(Mutex<BTreeMap<String, BucketObject>>);

#[async_trait]
impl Bucket for MemoryBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        Ok(self.get(key).await?.map(|mut object| {
            object.bytes = object
                .bytes
                .slice(start as usize..(start + length) as usize);
            object
        }))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: crate::payload::Payload,
    ) -> Result<bool> {
        let mut objects = self.0.lock().unwrap();
        let current = objects.get(key).map(|object| object.generation);
        if current != generation {
            return Ok(false);
        }
        let generation = current.unwrap_or(0) + 1;
        objects.insert(key.into(), BucketObject { generation, bytes });
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}

/// A control plane's ownership over an in-memory store, for tests.
#[derive(Clone)]
pub(crate) struct TestOwnership {
    pub ownerships: Arc<Ownerships>,
    store: Arc<MemoryOwnership>,
}

impl Default for TestOwnership {
    fn default() -> Self {
        let reader = RuntimeStorageReader::new(
            Arc::new(MemoryBucket::default()),
            Arc::new(SystemClock),
            Arc::new(crate::litestream::EmbeddedRestore),
        )
        .expect("test storage reader");
        let store = Arc::new(MemoryOwnership::default());
        Self {
            ownerships: Arc::new(Ownerships::new(store.clone(), Arc::new(reader))),
            store,
        }
    }
}

impl TestOwnership {
    pub fn reads(&self) -> usize {
        self.store.loads.load(Ordering::SeqCst)
    }

    /// Records `lease` as the actor's live owner, as a claimed and renewed host leaves it.
    pub fn set_owner(
        &self,
        object: &ActorStorageKey,
        lease: HostLease,
        home_region: &str,
    ) -> Result<u64> {
        let actor = crate::storage_paths::actor_from_key(object)?;
        let mut records = self.store.records.lock().unwrap();
        let (version, epoch) = match records.get(&actor) {
            Some((version, current)) if current.lease.session_id == lease.session_id => {
                (version + 1, current.epoch)
            }
            Some((version, current)) => (version + 1, current.epoch + 1),
            None => (1, 1),
        };
        records.insert(
            actor.clone(),
            (
                version,
                Ownership {
                    persistence: crate::bucket::SnapshotStoreConfig::Local,
                    sealed: false,
                    lease,
                    mutation: String::new(),
                    actor,
                    epoch,
                    region: home_region.into(),
                    base: None,
                    checkpoint_progress: None,
                },
            ),
        );
        Ok(epoch)
    }
}

impl TestOwnership {
    /// Renews `claim` as the host that adopted it does, which also records its route.
    pub async fn adopt(&self, claim: &ActorClaim, route: &str) -> Result<HostLease> {
        let lease = &claim.record.lease;
        self.ownerships
            .renew(
                &claim.record.actor,
                &crate::host_leases::HostLeaseRequest {
                    id: lease.id.clone(),
                    session_id: lease.session_id.clone(),
                    route: route.into(),
                    duration_ms: 60_000,
                },
            )
            .await
    }
}

/// The lease a host that adopted `claim` reports once it serves at `route`.
pub fn ready_lease(claim: &ActorClaim, route: &str) -> HostLease {
    HostLease {
        route: route.into(),
        expires_at_ms: SystemClock.now_ms().unwrap() + 60_000,
        ..claim.record.lease.clone()
    }
}

/// The record a host leaves after sealing `actor` at epoch 3.
pub fn sealed_owner(actor: &ActorKey, region: &str) -> OwnershipHint {
    serde_json::from_value(serde_json::json!({
        "version": {"postgres": 7},
        "record": {
            "persistence": {"type": "local"},
            "sealed": true,
            "lease": {
                "id": "host.v3.revision.sealed",
                "session_id": "sealed-session",
                "route": "http://127.0.0.1:1",
                "expires_at_ms": 0
            },
            "mutation": "sealed",
            "actor": actor,
            "epoch": 3,
            "region": region,
            "base": null
        }
    }))
    .expect("sealed ownership record")
}

/// What a control plane claims for a new actor before assigning it a host of `config_key`.
pub fn new_claim(actor: &ActorKey, config_key: &str) -> ActorClaim {
    ActorClaim {
        record: Ownership {
            persistence: crate::bucket::SnapshotStoreConfig::Local,
            sealed: false,
            lease: HostLease {
                id: crate::host::HostId::new(format!(
                    "host.v3.{config_key}.{}",
                    uuid::Uuid::new_v4()
                )),
                session_id: uuid::Uuid::new_v4().to_string(),
                route: String::new(),
                expires_at_ms: SystemClock.now_ms().unwrap() + 60_000,
            },
            mutation: String::new(),
            actor: actor.clone(),
            epoch: 1,
            region: "north-america-east".into(),
            base: None,
            checkpoint_progress: None,
        },
        progress: None,
    }
}
