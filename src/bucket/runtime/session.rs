use super::*;
use futures_util::{TryStreamExt, stream::FuturesUnordered};
use std::time::Instant;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Session {
    pub(super) id: String,
    pub(super) region: String,
    pub(super) replicas: Vec<ReplicaTarget>,
    pub(super) state: RecoveryState,
}

impl Session {
    pub(super) fn is_open(&self) -> bool {
        self.state == RecoveryState::Open
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum RecoveryState {
    Open,
    Recovering,
    Sealed,
}

struct PendingSnapshot {
    reference: SnapshotRef,
    download: AbortOnDropHandle<Result<DownloadedSnapshot>>,
}

impl RuntimeStorage {
    pub async fn retire_replication(&self, scope: &ReplicaScope) -> Result<()> {
        if let Some(owner) = self.get_owner(&scope.actor.storage_key()).await? {
            ensure!(
                owner.owner != scope.host
                    || owner.lease.session_id != scope.session
                    || owner.lease.expires_at_ms <= self.clock.now_ms()?,
                "actor activation is still active"
            );
        }
        let Some((session, snapshots)) = self.start_recovery(scope).await? else {
            return Ok(());
        };
        self.complete_recovery(scope, session, snapshots).await?;
        Ok(())
    }

    pub async fn replica_members(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        let key = key(&scope.host, &scope.session);
        let Some(object) = self.authority.get(&key).await? else {
            return Ok(Vec::new());
        };
        let session: Session = serde_json::from_slice(&object.bytes)?;
        ensure!(
            session.id == scope.identity() && session.region == scope.region,
            "session identity mismatch"
        );
        Ok(if session.state == RecoveryState::Sealed {
            Vec::new()
        } else {
            session.replicas
        })
    }

    pub(super) async fn recover_session(
        &self,
        owner: &Ownership,
    ) -> Result<Option<LoadedSnapshot>> {
        let started = Instant::now();
        let recovery = self.start_recovery(&owner.scope()).await?;
        let claim_and_seal_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let mut snapshot_load_ms = 0.0;
        let load = async {
            let started = Instant::now();
            let snapshot = self.bucket_snapshot(owner, None).await?;
            snapshot_load_ms = started.elapsed().as_secs_f64() * 1_000.0;
            anyhow::Ok(snapshot)
        };
        let restore = async {
            match recovery {
                Some((session, snapshots)) => {
                    self.complete_recovery(&owner.scope(), session, snapshots)
                        .await
                }
                None => Ok(Vec::new()),
            }
        };
        let (mut loaded, restored) = tokio::try_join!(load, restore)?;
        let prefix = snapshot_prefix(&owner.actor)?;
        for snapshot in restored {
            if snapshot.reference.object.starts_with(&prefix) {
                merge_snapshot(&mut loaded, snapshot)?;
            }
        }
        let loaded = self.complete_snapshot(owner, loaded, None, &[]).await?;
        tracing::info!(
            event = "actor_activation_recovery",
            project_id = %owner.actor.project_id,
            actor_name = %owner.actor.actor_name,
            actor_id = %owner.actor.actor_id,
            previous_host_id = %owner.lease.id,
            previous_session_id = %owner.lease.session_id,
            claim_and_seal_ms,
            snapshot_load_ms,
            duration_ms = started.elapsed().as_secs_f64() * 1_000.0,
        );
        Ok(loaded)
    }

    async fn start_recovery(
        &self,
        scope: &ReplicaScope,
    ) -> Result<Option<(Session, Vec<PendingSnapshot>)>> {
        let key = key(&scope.host, &scope.session);
        let id = scope.identity();
        loop {
            let object = self.authority.get(&key).await?;
            let Some(object) = object else {
                // A tombstone also fences initialization delayed past lease expiry.
                let sealed = Session {
                    id: id.clone(),
                    region: scope.region.clone(),
                    replicas: Vec::new(),
                    state: RecoveryState::Sealed,
                };
                if self.save_session(&key, None, &sealed).await? {
                    return Ok(None);
                }
                continue;
            };
            let mut session: Session = serde_json::from_slice(&object.bytes)?;
            ensure!(
                session.id == id && session.region == scope.region,
                "session identity mismatch"
            );
            match session.state {
                RecoveryState::Sealed => return Ok(None),
                RecoveryState::Recovering => {
                    let snapshots = self.seal_replicas(&session).await?;
                    return Ok(Some((session, snapshots)));
                }
                RecoveryState::Open => {
                    session.state = RecoveryState::Recovering;
                    if let Some(snapshots) = self
                        .claim_recovery(scope, object.generation, &session)
                        .await?
                    {
                        return Ok(Some((session, snapshots)));
                    }
                }
            }
        }
    }

    async fn claim_recovery(
        &self,
        scope: &ReplicaScope,
        generation: i64,
        session: &Session,
    ) -> Result<Option<Vec<PendingSnapshot>>> {
        let key = key(&scope.host, &scope.session);
        let started = Instant::now();
        let claim = async {
            let result = self.save_session(&key, Some(generation), session).await;
            (result, started.elapsed().as_secs_f64() * 1_000.0)
        };
        let seal = async {
            let result = self.seal_replicas(session).await;
            (result, started.elapsed().as_secs_f64() * 1_000.0)
        };
        let ((claimed, claim_ms), (sealed, seal_ms)) = tokio::join!(claim, seal);
        tracing::info!(event = "actor_recovery_fence", session_id = %scope.session,
            host_id = %scope.host, claim_ms, seal_ms, claimed = matches!(&claimed, Ok(true)));
        // A losing claim may have sealed an obsolete replica set.
        if claimed? {
            Ok(Some(sealed?))
        } else {
            Ok(None)
        }
    }

    async fn complete_recovery(
        &self,
        scope: &ReplicaScope,
        session: Session,
        snapshots: Vec<PendingSnapshot>,
    ) -> Result<Vec<LoadedSnapshot>> {
        let key = key(&scope.host, &scope.session);
        let started = Instant::now();
        let restore = async {
            let loaded = self.restore_snapshots(&session, snapshots).await?;
            anyhow::Ok((loaded, started.elapsed().as_secs_f64() * 1_000.0))
        };
        let read = async {
            let current = self.authority.get(&key).await?;
            anyhow::Ok((current, started.elapsed().as_secs_f64() * 1_000.0))
        };
        let ((loaded, restore_ms), (current, finish_read_ms)) = tokio::try_join!(restore, read)?;
        let finish_started = Instant::now();
        self.finish_recovery(&key, session, current).await?;
        tracing::info!(event = "actor_recovery_storage", session_id = %scope.session,
            host_id = %scope.host, restore_ms, finish_read_ms,
            finish_write_ms = finish_started.elapsed().as_secs_f64() * 1_000.0,
            duration_ms = started.elapsed().as_secs_f64() * 1_000.0);
        Ok(loaded)
    }

    async fn finish_recovery(
        &self,
        key: &str,
        mut session: Session,
        mut current: Option<crate::bucket::BucketObject>,
    ) -> Result<()> {
        session.state = RecoveryState::Sealed;
        loop {
            let object = current.context("recovery record disappeared")?;
            let record: Session = serde_json::from_slice(&object.bytes)?;
            ensure!(
                record.id == session.id && record.region == session.region,
                "session identity mismatch"
            );
            if record.state == RecoveryState::Sealed
                || self
                    .save_session(key, Some(object.generation), &session)
                    .await?
            {
                return Ok(());
            }
            current = self.authority.get(key).await?;
        }
    }

    async fn save_session(
        &self,
        key: &str,
        generation: Option<i64>,
        session: &Session,
    ) -> Result<bool> {
        replace(
            self.authority.as_ref(),
            key,
            generation,
            serde_json::to_vec(session)?,
        )
        .await
    }

    async fn seal_replicas(&self, session: &Session) -> Result<Vec<PendingSnapshot>> {
        let mut pending = JoinSet::new();
        for target in &session.replicas {
            let (peers, target, id) = (self.peers.clone(), target.clone(), session.id.clone());
            pending.spawn(async move { peers.seal(&target, &id).await });
        }
        let mut witnesses = 0;
        let mut snapshots: HashMap<String, PendingSnapshot> = HashMap::new();
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(head)) = result {
                ensure!(
                    head.session == session.id,
                    "replica returned another session"
                );
                if !head.initialized || !head.sealed {
                    continue;
                }
                witnesses += 1;
                for head in head.streams {
                    ensure!(
                        head.stream.session == session.id,
                        "replica returned another session's stream"
                    );
                    if let Some(snapshot) = head.latest {
                        ensure!(
                            snapshot.object == head.stream.object(snapshot.state_version),
                            "replica snapshot identity mismatch"
                        );
                        let current = snapshots.get(&head.stream.prefix);
                        let mut newest = current.map(|current| current.reference.clone());
                        advance(&mut newest, Some(snapshot.clone()))?;
                        if newest.as_ref() == Some(&snapshot)
                            && current.is_none_or(|current| current.reference != snapshot)
                        {
                            snapshots.insert(
                                head.stream.prefix,
                                self.prefetch_snapshot(session, snapshot),
                            );
                        }
                    }
                }
            }
        }
        ensure!(
            session.replicas.is_empty() || witnesses > 0,
            "no complete replica witness; refusing to lose acknowledged state"
        );
        Ok(snapshots.into_values().collect())
    }

    // Downloads stay read-only until the claim and every seal are validated.
    fn prefetch_snapshot(&self, session: &Session, reference: SnapshotRef) -> PendingSnapshot {
        let (authority, peers) = (self.authority.clone(), self.peers.clone());
        let replicas = session.replicas.clone();
        let snapshot = reference.clone();
        let download = tokio::spawn(async move {
            Self::download_snapshot(authority.as_ref(), peers.as_ref(), &replicas, &snapshot).await
        });
        PendingSnapshot {
            reference,
            download: AbortOnDropHandle::new(download),
        }
    }

    async fn restore_snapshots(
        &self,
        session: &Session,
        snapshots: Vec<PendingSnapshot>,
    ) -> Result<Vec<LoadedSnapshot>> {
        snapshots
            .into_iter()
            .map(|snapshot| self.restore_snapshot(session, snapshot))
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await
    }

    async fn restore_snapshot(
        &self,
        session: &Session,
        snapshot: PendingSnapshot,
    ) -> Result<LoadedSnapshot> {
        let bytes = match snapshot.download.await? {
            Ok(download) => self.persist_download(&snapshot.reference, download).await?,
            Err(_) => {
                self.recover_snapshot(&session.replicas, &snapshot.reference)
                    .await?
            }
        };
        Ok(LoadedSnapshot {
            reference: snapshot.reference,
            bytes: bytes.into(),
        })
    }
}

pub(super) fn identity(host: &HostId, session: &str) -> String {
    format!("{}/", crate::storage_paths::session(host, session))
}

fn key(host: &HostId, session: &str) -> String {
    format!("{}.json", crate::storage_paths::session(host, session))
}
