use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use tokio_util::sync::CancellationToken;

use super::{
    HostId,
    actor_runtime::{ActorActivation, ActorStorage},
};
use crate::{
    actor::ActorKey,
    bucket::{
        Bucket, BucketHostLeases, GcsBucket, GrpcReplicaPeers, RuntimeStorage,
        access::{HostStorageConfig, StorageToken},
    },
    clock::{Clock, SystemClock},
    control_plane::{ControlPlaneClient, LeaseFence},
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest, HostLeaseStore},
    replication::{ReplicaAccess, ReplicaSet},
    storage::WritePlan,
};

pub(crate) struct HostStorage {
    pub runtime: Arc<RuntimeStorage>,
    leases: Arc<dyn HostLeaseStore>,
    observer: Option<Arc<ControlPlaneClient>>,
    host: HostId,
    session: String,
    region: String,
    fence: Mutex<LeaseFence>,
    lease: Mutex<Option<HostLease>>,
}

impl HostStorage {
    pub async fn new(
        config: HostStorageConfig,
        host: HostId,
        session: String,
        origin: String,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
    ) -> Result<Self> {
        let credentials = HostCredentials::new(config.token, client.clone(), stop);
        let authority: Arc<dyn Bucket> = match config.bucket {
            crate::bucket::access::BucketLocation::Gcs { bucket } => {
                Arc::new(GcsBucket::with_credentials(&bucket, credentials.into()).await?)
            }
            crate::bucket::access::BucketLocation::File { directory } => {
                Arc::new(crate::bucket::FileBucket::new(directory)?)
            }
        };
        let leases = Arc::new(BucketHostLeases::new(
            authority.clone(),
            Arc::new(SystemClock),
        ));
        let access = ReplicaAccess::new(&config.replica_secret, Arc::new(SystemClock));
        let runtime = Arc::new(RuntimeStorage::new(
            authority,
            leases.clone(),
            Arc::new(ReplicaSet(config.replicas)),
            Arc::new(GrpcReplicaPeers::new(access.clone())?),
            access,
            origin,
        )?);
        Ok(Self {
            observer: Some(client),
            runtime,
            leases,
            host,
            session,
            region: config.region,
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
        })
    }

    fn notify_observer(&self) {
        if let Some(client) = self.observer.clone() {
            tokio::spawn(async move {
                let _ =
                    tokio::time::timeout(Duration::from_secs(2), client.notify_inventory_changed())
                        .await;
            });
        }
    }

    fn authorize(&self, actor: &ActorKey, host: &HostId) -> Result<()> {
        actor.validate()?;
        ensure!(*host == self.host, "host storage scope mismatch");
        self.ensure_authority()
    }
}

#[async_trait]
impl ActorStorage for HostStorage {
    fn ensure_authority(&self) -> Result<()> {
        self.fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .check(Instant::now())
    }

    async fn acquire_actor(&self, actor: &ActorKey, host: &HostId) -> Result<ActorActivation> {
        self.authorize(actor, host)?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self
            .runtime
            .activate_actor(actor, &lease, &self.region)
            .await?;
        self.ensure_authority()?;
        Ok(ActorActivation {
            owner_epoch: loaded.placement.owner_epoch,
            state_version: loaded.placement.state_version,
            state: loaded.state,
        })
    }

    async fn load_actor_state(
        &self,
        actor: &ActorKey,
        host: &HostId,
        epoch: u64,
    ) -> Result<(u64, bytes::Bytes)> {
        self.authorize(actor, host)?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self.runtime.load_owned_actor(actor, &lease, epoch).await?;
        self.ensure_authority()?;
        Ok((
            loaded.placement.state_version,
            loaded.state.unwrap_or_default(),
        ))
    }

    async fn prepare_state_write(
        &self,
        actor: &ActorKey,
        host: &HostId,
        epoch: u64,
        version: u64,
    ) -> Result<WritePlan> {
        self.authorize(actor, host)?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let ticket = self
            .runtime
            .prepare_actor_write(
                actor,
                &lease,
                epoch,
                version.checked_add(1).context("state version overflow")?,
            )
            .await?;
        self.ensure_authority()?;
        Ok(ticket)
    }
}

#[async_trait]
impl HostLeaseRegistry for HostStorage {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        self.register_with_residents(request, None).await
    }

    async fn register_with_residents(
        &self,
        request: &HostLeaseRequest,
        residents: Option<&[crate::actor::ActorKey]>,
    ) -> Result<HostLease> {
        self.register_with_inventory(request, residents, &[], None)
            .await
    }

    async fn register_with_inventory(
        &self,
        request: &HostLeaseRequest,
        residents: Option<&[crate::actor::ActorKey]>,
        sockets: &[crate::host_leases::ActorSocketInventory],
        queues: Option<&[crate::host_leases::ActorQueueInventory]>,
    ) -> Result<HostLease> {
        ensure!(
            request.id == self.host && request.session_id == self.session,
            "host lease scope mismatch"
        );
        if let Some(actors) = residents {
            ensure!(actors.len() <= 32, "resident actor scope mismatch");
        }
        let started = Instant::now();
        self.fence.lock().unwrap().begin(started)?;
        let lease = self
            .leases
            .register_with_inventory(request, residents, sockets, queues)
            .await?;
        self.fence.lock().unwrap().confirm(
            started,
            Duration::from_millis(request.duration_ms),
            Instant::now(),
        )?;
        let first = self.lease.lock().unwrap().replace(lease.clone()).is_none();
        if first {
            let (runtime, region, lease) =
                (self.runtime.clone(), self.region.clone(), lease.clone());
            tokio::spawn(async move {
                if let Err(error) = runtime.prepare_session(&lease, &region).await {
                    tracing::warn!(%error, "host replication session preparation failed");
                }
            });
        }
        self.notify_observer();
        Ok(lease)
    }
    async fn unregister(&self, host: &HostId, session: &str) -> Result<()> {
        ensure!(
            *host == self.host && session == self.session,
            "host lease scope mismatch"
        );
        self.fence.lock().unwrap().fenced = true;
        self.leases.unregister(host, session).await?;
        self.notify_observer();
        Ok(())
    }
}

#[derive(Clone)]
struct HostCredentials(Arc<std::sync::RwLock<Option<StorageToken>>>);
impl std::fmt::Debug for HostCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCredentials")
    }
}
impl HostCredentials {
    fn new(
        token: Option<StorageToken>,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
    ) -> Self {
        let credentials = Self(Arc::new(std::sync::RwLock::new(token)));
        let refreshed = credentials.clone();
        tokio::spawn(async move {
            loop {
                let now = SystemClock.now_ms().unwrap_or(u64::MAX);
                let host_expiry = client.token_expires_at_ms().unwrap_or(now);
                let expires = refreshed
                    .0
                    .read()
                    .unwrap()
                    .as_ref()
                    .map_or(host_expiry, |token| token.expires_at_ms.min(host_expiry));
                let delay = Duration::from_millis(
                    expires
                        .saturating_sub(now.saturating_add(30_000))
                        .clamp(1_000, 600_000),
                );
                tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(delay) => {} }
                match client.refresh_storage_access().await {
                    Ok(token) => {
                        *refreshed.0.write().unwrap() = token;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not refresh host storage credentials")
                    }
                }
            }
        });
        credentials
    }
}
impl CredentialsProvider for HostCredentials {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let stored = self.0.read().unwrap();
        let token = stored.as_ref().ok_or_else(|| {
            google_cloud_auth::errors::CredentialsError::from_msg(false, "GCS credentials missing")
        })?;
        let mut headers = axum::http::HeaderMap::new();
        let mut value: axum::http::HeaderValue = format!("Bearer {}", token.access_token)
            .parse()
            .map_err(|_| {
            google_cloud_auth::errors::CredentialsError::from_msg(
                false,
                "invalid storage credential",
            )
        })?;
        value.set_sensitive(true);
        headers.insert(axum::http::header::AUTHORIZATION, value);
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }
    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bucket::BucketObject, state_log::StateSnapshot, state_transport::SnapshotWriter};
    use std::collections::HashMap;

    #[derive(Default)]
    struct MemoryBucket {
        objects: Mutex<HashMap<String, BucketObject>>,
        owner_reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait]
    impl Bucket for MemoryBucket {
        async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
            if key.contains("/owners/") {
                self.owner_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            generation: Option<i64>,
            bytes: Vec<u8>,
        ) -> Result<bool> {
            let mut objects = self.objects.lock().unwrap();
            let current = objects.get(key).map(|object| object.generation);
            if current != generation {
                return Ok(false);
            }
            objects.insert(
                key.into(),
                BucketObject {
                    generation: current.unwrap_or(0) + 1,
                    bytes,
                },
            );
            Ok(true)
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    #[tokio::test]
    async fn host_registers_claims_reads_and_writes_without_a_control_plane() -> Result<()> {
        let bucket = Arc::new(MemoryBucket::default());
        let leases = Arc::new(BucketHostLeases::new(bucket.clone(), Arc::new(SystemClock)));
        let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
        let runtime = Arc::new(RuntimeStorage::new(
            bucket.clone(),
            leases.clone(),
            Arc::new(ReplicaSet(vec![])),
            Arc::new(GrpcReplicaPeers::new(access.clone())?),
            access,
            "http://control-plane-unavailable.invalid".into(),
        )?);
        let host = HostId::new("host.v3.revision.host");
        let storage = HostStorage {
            observer: None,
            runtime,
            leases,
            host: host.clone(),
            session: "session".into(),
            region: "us-east".into(),
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
        };
        let actor = ActorKey {
            actor_type: "Counter".into(),
            actor_id: "one".into(),
        };
        assert!(storage.acquire_actor(&actor, &host).await.is_err());
        storage
            .register(&HostLeaseRequest {
                id: host.clone(),
                session_id: "session".into(),
                route: "http://host".into(),
                duration_ms: 30_000,
            })
            .await?;
        let activation = storage.acquire_actor(&actor, &host).await?;
        assert_eq!(activation.owner_epoch, 1);
        assert_eq!(activation.state_version, 0);
        assert_eq!(
            bucket.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let snapshot = StateSnapshot::new(
            1,
            1,
            "write".into(),
            serde_json::json!({"count":1}),
            serde_json::json!(1),
        )?
        .encode()?;
        let ticket = storage.prepare_state_write(&actor, &host, 1, 0).await?;
        assert_eq!(
            bucket.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the first write uses locally established ownership"
        );
        storage
            .runtime
            .write_snapshot(&ticket, snapshot.clone())
            .await?;
        let (version, loaded) = storage.load_actor_state(&actor, &host, 1).await?;
        assert_eq!(version, 1);
        assert_eq!(loaded.as_ref(), snapshot.as_slice());
        assert!(
            storage
                .prepare_state_write(&actor, &host, 2, 1)
                .await
                .is_err()
        );
        assert!(
            storage
                .acquire_actor(&actor, &HostId::new("another-host"))
                .await
                .is_err()
        );
        storage.unregister(&host, "session").await?;
        assert!(storage.ensure_authority().is_err());
        assert!(storage.acquire_actor(&actor, &host).await.is_err());
        Ok(())
    }
}
