use super::*;

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
        let Some(session) = self.start_recovery(scope).await? else {
            return Ok(());
        };
        for snapshot in self.seal_replicas(&session).await? {
            self.recover_snapshot(&session.replicas, &snapshot).await?;
        }
        self.finish_recovery(scope, session).await
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
        let Some(session) = self.start_recovery(&owner.scope()).await? else {
            return Ok(None);
        };
        let snapshots = self.seal_replicas(&session).await?;
        let recovered = self.restore_session(owner, &session, snapshots).await?;
        self.finish_recovery(&owner.scope(), session).await?;
        Ok(recovered)
    }

    async fn start_recovery(&self, scope: &ReplicaScope) -> Result<Option<Session>> {
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
                RecoveryState::Recovering => return Ok(Some(session)),
                RecoveryState::Open => {
                    session.state = RecoveryState::Recovering;
                    if self
                        .save_session(&key, Some(object.generation), &session)
                        .await?
                    {
                        return Ok(Some(session));
                    }
                }
            }
        }
    }

    async fn finish_recovery(&self, scope: &ReplicaScope, mut session: Session) -> Result<()> {
        let key = key(&scope.host, &scope.session);
        session.state = RecoveryState::Sealed;
        loop {
            let current = self
                .authority
                .get(&key)
                .await?
                .context("recovery record disappeared")?;
            let record: Session = serde_json::from_slice(&current.bytes)?;
            if record.state == RecoveryState::Sealed
                || self
                    .save_session(&key, Some(current.generation), &session)
                    .await?
            {
                return Ok(());
            }
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

    async fn seal_replicas(&self, session: &Session) -> Result<Vec<SnapshotRef>> {
        let mut pending = JoinSet::new();
        for target in &session.replicas {
            let (peers, target, id) = (self.peers.clone(), target.clone(), session.id.clone());
            pending.spawn(async move { peers.seal(&target, &id).await });
        }
        let mut witnesses = 0;
        let mut snapshots = HashMap::new();
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
                        advance(
                            snapshots.entry(head.stream.prefix).or_insert(None),
                            Some(snapshot),
                        )?;
                    }
                }
            }
        }
        ensure!(
            session.replicas.is_empty() || witnesses > 0,
            "no complete replica witness; refusing to lose acknowledged state"
        );
        Ok(snapshots.into_values().flatten().collect())
    }

    async fn restore_session(
        &self,
        owner: &Ownership,
        session: &Session,
        snapshots: Vec<SnapshotRef>,
    ) -> Result<Option<LoadedSnapshot>> {
        let prefix = snapshot_prefix(&owner.actor)?;
        let mut loaded: Option<LoadedSnapshot> = None;
        for snapshot in snapshots {
            let bytes = self.recover_snapshot(&session.replicas, &snapshot).await?;
            if snapshot.object.starts_with(&prefix)
                && loaded.as_ref().is_none_or(|s| {
                    snapshot_position(&snapshot.object) > snapshot_position(&s.reference.object)
                })
            {
                loaded = Some(LoadedSnapshot {
                    reference: snapshot,
                    bytes: bytes.into(),
                });
            }
        }
        Ok(loaded)
    }
}

pub(super) fn identity(host: &HostId, session: &str) -> String {
    format!("{}/", crate::storage_paths::session(host, session))
}

fn key(host: &HostId, session: &str) -> String {
    format!("{}.json", crate::storage_paths::session(host, session))
}
