use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use tokio_util::sync::CancellationToken;

use super::{
    HostId,
    actor_runtime::{ActorActivation, CommittedState, StateCommitAuthority, StateSource},
};
use crate::{
    actor::ActorKey,
    bucket::{
        Bucket, BucketHostLeases, GcsBucket, HttpReplicaPeers, RuntimeStorage,
        access::{HostStorageConfig, StorageToken},
    },
    clock::{Clock, SystemClock},
    control_plane::{ControlPlaneClient, LeaseFence},
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest, HostLeaseStore},
    replication::{ReplicaAccess, ReplicaProvisioner, ReplicaTarget},
    storage_urls::StateWriteTicket,
};

pub(super) struct HostStorage {
    pub runtime: Arc<RuntimeStorage>,
    leases: Arc<dyn HostLeaseStore>,
    namespace: String,
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
        namespace: String,
        origin: String,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
    ) -> Result<Self> {
        let credentials = HostCredentials::new(config.token, client, stop);
        let authority = Arc::new(
            GcsBucket::with_credentials(&config.authority_bucket, credentials.clone().into())
                .await?,
        );
        let leases = Arc::new(BucketHostLeases::new(
            authority.clone(),
            Arc::new(SystemClock),
        ));
        let mut states = HashMap::new();
        for (region, bucket) in config.state_buckets {
            states.insert(
                region,
                Arc::new(GcsBucket::with_credentials(&bucket, credentials.clone().into()).await?)
                    as Arc<dyn Bucket>,
            );
        }
        let access =
            ReplicaAccess::delegated(&config.replica_secret, &namespace, Arc::new(SystemClock));
        let runtime = Arc::new(RuntimeStorage::new(
            authority,
            states,
            leases.clone(),
            Arc::new(FixedReplicas(config.replicas)),
            Arc::new(HttpReplicaPeers::new(access.clone())?),
            access,
            origin,
            config.replica_count,
        )?);
        Ok(Self {
            runtime,
            leases,
            namespace,
            host,
            session,
            region: config.region,
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
        })
    }

    fn authorize(&self, actor: &ActorKey, host: &HostId) -> Result<()> {
        actor.validate()?;
        ensure!(
            actor.namespace_id == self.namespace && *host == self.host,
            "host storage scope mismatch"
        );
        self.ensure_authority()
    }
}

#[async_trait]
impl StateCommitAuthority for HostStorage {
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
            state_object: loaded.placement.state_object,
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
    ) -> Result<Option<(u64, StateSource)>> {
        self.authorize(actor, host)?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self.runtime.load_owned_actor(actor, &lease, epoch).await?;
        self.ensure_authority()?;
        Ok(Some((
            loaded.placement.state_version,
            StateSource::Bytes(loaded.state.unwrap_or_default()),
        )))
    }

    async fn prepare_state_write(
        &self,
        actor: &ActorKey,
        host: &HostId,
        epoch: u64,
        version: u64,
    ) -> Result<StateWriteTicket> {
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

    async fn commit_state(
        &self,
        _: &ActorKey,
        _: &HostId,
        _: u64,
        _: u64,
        _: &str,
        _: &str,
    ) -> Result<CommittedState> {
        anyhow::bail!("bucket state is committed by its durability proof")
    }
}

#[async_trait]
impl HostLeaseRegistry for HostStorage {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        ensure!(
            request.id == self.host && request.session_id == self.session,
            "host lease scope mismatch"
        );
        let started = Instant::now();
        self.fence.lock().unwrap().begin(started)?;
        let lease = self.leases.register(request).await?;
        self.fence.lock().unwrap().confirm(
            started,
            Duration::from_millis(request.duration_ms),
            Instant::now(),
        )?;
        let first = self.lease.lock().unwrap().replace(lease.clone()).is_none();
        if first {
            let (runtime, namespace, region, lease) = (
                self.runtime.clone(),
                self.namespace.clone(),
                self.region.clone(),
                lease.clone(),
            );
            tokio::spawn(async move {
                if let Err(error) = runtime.prepare_session(&namespace, &lease, &region).await {
                    tracing::warn!(%error, "host replication session preparation failed");
                }
            });
        }
        Ok(lease)
    }
    async fn unregister(&self, host: &HostId, session: &str) -> Result<()> {
        ensure!(
            *host == self.host && session == self.session,
            "host lease scope mismatch"
        );
        self.fence.lock().unwrap().fenced = true;
        self.leases.unregister(host, session).await
    }
}

struct FixedReplicas(Vec<ReplicaTarget>);
#[async_trait]
impl ReplicaProvisioner for FixedReplicas {
    async fn ensure(&self, _: &ActorKey, _: &str, count: usize) -> Result<Vec<ReplicaTarget>> {
        ensure!(self.0.len() == count, "replica fleet is incomplete");
        Ok(self.0.clone())
    }
}

#[derive(Clone)]
struct HostCredentials(Arc<std::sync::RwLock<StorageToken>>);
impl std::fmt::Debug for HostCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCredentials")
    }
}
impl HostCredentials {
    fn new(token: StorageToken, client: Arc<ControlPlaneClient>, stop: CancellationToken) -> Self {
        let credentials = Self(Arc::new(std::sync::RwLock::new(token)));
        let refreshed = credentials.clone();
        tokio::spawn(async move {
            loop {
                let now = SystemClock.now_ms().unwrap_or(u64::MAX);
                let expires = refreshed
                    .0
                    .read()
                    .unwrap()
                    .expires_at_ms
                    .min(client.token_expires_at_ms().unwrap_or(now));
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
        let token = self.0.read().unwrap();
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
    use crate::{bucket::BucketObject, state_log::StateSnapshot, state_transport::StateTransport};

    #[derive(Default)]
    struct MemoryBucket {
        objects: Mutex<HashMap<String, BucketObject>>,
        owner_reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait]
    impl Bucket for MemoryBucket {
        async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
            if key.starts_with("runtime/owners/") {
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
        let access =
            ReplicaAccess::new("secret", Arc::new(SystemClock)).for_namespace("project.a")?;
        let runtime = Arc::new(RuntimeStorage::new(
            bucket.clone(),
            HashMap::from([("us-east".into(), bucket.clone() as Arc<dyn Bucket>)]),
            leases.clone(),
            Arc::new(FixedReplicas(vec![])),
            Arc::new(HttpReplicaPeers::new(access.clone())?),
            access,
            "http://control-plane-unavailable.invalid".into(),
            0,
        )?);
        let host = HostId::new("host.v2.project.a:revision.host");
        let storage = HostStorage {
            runtime,
            leases,
            namespace: "project.a".into(),
            host: host.clone(),
            session: "session".into(),
            region: "us-east".into(),
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
        };
        let actor = ActorKey {
            namespace_id: "project.a".into(),
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
        storage.runtime.write(&ticket.url, snapshot.clone()).await?;
        let (version, StateSource::Bytes(loaded)) =
            storage.load_actor_state(&actor, &host, 1).await?.unwrap()
        else {
            panic!("host should return loaded bytes")
        };
        assert_eq!(version, 1);
        assert_eq!(loaded.as_ref(), snapshot.as_slice());
        assert!(
            storage
                .prepare_state_write(&actor, &host, 2, 1)
                .await
                .is_err()
        );
        let other = ActorKey {
            namespace_id: "project.a.b".into(),
            ..actor.clone()
        };
        assert!(storage.acquire_actor(&other, &host).await.is_err());
        storage.unregister(&host, "session").await?;
        assert!(storage.ensure_authority().is_err());
        assert!(storage.acquire_actor(&actor, &host).await.is_err());
        Ok(())
    }
}
