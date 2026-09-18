use super::*;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Session {
    id: String,
    region: String,
    replicas: Vec<ReplicaTarget>,
    state: RecoveryState,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum RecoveryState {
    Open,
    Recovering,
    Sealed,
}

impl RuntimeStorage {
    pub(crate) async fn prepare_session(
        &self,
        lease: &HostLease,
        region: &str,
    ) -> Result<Vec<ReplicaTarget>> {
        if self.fleet.replica_regions().is_empty() {
            return Ok(Vec::new());
        }
        let id = identity(&lease.id, &lease.session_id);
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get(&id) {
            ensure!(session.region == region, "session region cannot change");
            return Ok(session.replicas.clone());
        }
        let session = self.open_session(lease, region).await?;
        sessions.insert(id, session.clone());
        Ok(session.replicas)
    }

    async fn open_session(&self, lease: &HostLease, region: &str) -> Result<Session> {
        let id = identity(&lease.id, &lease.session_id);
        let key = key(&lease.id, &lease.session_id);
        if let Some(object) = self.authority.get(&key).await? {
            let session: Session = serde_json::from_slice(&object.bytes)?;
            ensure!(
                session.id == id
                    && session.region == region
                    && session.state == RecoveryState::Open,
                "session is recovering or sealed"
            );
            return Ok(session);
        }
        let actor = ActorKey {
            actor_type: "session".into(),
            actor_id: "replication".into(),
        };
        let replicas = match tokio::time::timeout(
            Duration::from_secs(20),
            self.initialize_session(&actor, &id, region),
        )
        .await
        {
            Ok(Ok(replicas)) => replicas,
            error => {
                tracing::warn!(?error, "host session will use object storage");
                Vec::new()
            }
        };
        let session = Session {
            id: id.clone(),
            region: region.into(),
            replicas,
            state: RecoveryState::Open,
        };
        ensure!(
            replace(
                self.authority.as_ref(),
                &key,
                None,
                serde_json::to_vec(&session)?
            )
            .await?,
            "session initialization was fenced"
        );
        Ok(session)
    }

    async fn initialize_session(
        &self,
        actor: &ActorKey,
        session: &str,
        region: &str,
    ) -> Result<Vec<ReplicaTarget>> {
        let replicas = self.fleet.ensure(actor, region).await?;
        ensure!(
            replicas.len() == self.fleet.replica_regions().len(),
            "incomplete replica set"
        );
        ReplicationTicket {
            replicas: replicas.clone(),
            archive_url: "initialization".into(),
        }
        .validate()?;
        let mut pending = JoinSet::new();
        for target in &replicas {
            let (peers, target, session) = (self.peers.clone(), target.clone(), session.to_owned());
            pending.spawn(async move { peers.initialize(&target, &session).await });
        }
        while let Some(result) = pending.join_next().await {
            result??;
        }
        Ok(replicas)
    }

    pub(super) async fn session_replicas(&self, owner: &Ownership) -> Result<Vec<ReplicaTarget>> {
        let key = key(&owner.owner, &owner.session);
        let Some(object) = self.authority.get(&key).await? else {
            return Ok(Vec::new());
        };
        let session: Session = serde_json::from_slice(&object.bytes)?;
        ensure!(
            session.id == identity(&owner.owner, &owner.session) && session.region == owner.region,
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
        let Some(session) = self.start_recovery(owner).await? else {
            return Ok(None);
        };
        let snapshots = self.seal_replicas(&session).await?;
        let recovered = self.restore_session(owner, &session, snapshots).await?;
        self.finish_recovery(owner, session).await?;
        Ok(recovered)
    }

    async fn start_recovery(&self, owner: &Ownership) -> Result<Option<Session>> {
        let key = key(&owner.owner, &owner.session);
        let id = identity(&owner.owner, &owner.session);
        loop {
            let object = self.authority.get(&key).await?;
            let Some(object) = object else {
                // A tombstone also fences initialization delayed past lease expiry.
                let sealed = Session {
                    id: id.clone(),
                    region: owner.region.clone(),
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
                session.id == id && session.region == owner.region,
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

    async fn finish_recovery(&self, owner: &Ownership, mut session: Session) -> Result<()> {
        let key = key(&owner.owner, &owner.session);
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
