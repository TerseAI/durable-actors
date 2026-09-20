use super::*;
use crate::state_transport::StateTransport;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaMembership {
    pub(super) scope: ReplicaScope,
    pub(super) replicas: Vec<ReplicaTarget>,
}

impl RuntimeStorage {
    pub(crate) fn replication_enabled(&self) -> bool {
        !self.fleet.replica_regions().is_empty()
    }

    pub async fn replacement_replicas(
        &self,
        scope: &ReplicaScope,
        failed: &[String],
    ) -> Result<Vec<ReplicaTarget>> {
        let targets = self.fleet.repair(scope, failed).await?;
        let stream = self
            .owned
            .lock()
            .unwrap()
            .get(scope.actor.storage_key().as_str())
            .context("actor is not locally activated")?
            .stream()?;
        let mut checks = JoinSet::new();
        for target in &targets {
            let (target, peers, stream) = (target.clone(), self.peers.clone(), stream.clone());
            checks.spawn(async move {
                let result =
                    tokio::time::timeout(Duration::from_secs(3), peers.head(&target, &stream))
                        .await;
                (!matches!(result, Ok(Ok(_)))).then_some(target.host_id)
            });
        }
        let mut unavailable = Vec::new();
        while let Some(result) = checks.join_next().await {
            if let Some(host) = result? {
                unavailable.push(host);
            }
        }
        if unavailable.is_empty() {
            Ok(targets)
        } else {
            self.fleet.repair(scope, &unavailable).await
        }
    }

    pub(crate) async fn current_write_plan(&self, plan: &WritePlan) -> Result<WritePlan> {
        let actor = actor_from_object(&plan.object_name)?;
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        ensure!(
            record.stream()? == plan.stream,
            "write belongs to another activation"
        );
        let sessions = self.sessions.lock().unwrap();
        let replicas = sessions
            .get(&plan.stream.session)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        self.write_plan(&record, plan.state_version, replicas)
    }

    pub(crate) async fn unhealthy_replicas(&self, scope: &ReplicaScope) -> Result<Vec<String>> {
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(scope.actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        let stream = record.stream()?;
        let targets = self
            .sessions
            .lock()
            .unwrap()
            .get(&scope.identity())
            .cloned()
            .unwrap_or_default();
        let mut checks = JoinSet::new();
        for target in targets {
            let peers = self.peers.clone();
            let stream = stream.clone();
            checks.spawn(async move {
                let result =
                    tokio::time::timeout(Duration::from_secs(3), peers.head(&target, &stream))
                        .await;
                (!matches!(result, Ok(Ok(head)) if head.stream == stream)).then_some(target.host_id)
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = checks.join_next().await {
            if let Some(host) = result? {
                failed.push(host);
            }
        }
        Ok(failed)
    }

    pub(crate) fn local_replica_members(&self, scope: &ReplicaScope) -> Vec<ReplicaTarget> {
        self.sessions
            .lock()
            .unwrap()
            .get(&scope.identity())
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn suspend_replication(&self, scope: &ReplicaScope) {
        self.sessions.lock().unwrap().remove(&scope.identity());
    }

    pub fn enable_replication(&self, membership: ReplicaMembership) -> Result<()> {
        let owned = self.owned.lock().unwrap();
        let record = owned
            .get(membership.scope.actor.storage_key().as_str())
            .context("actor is not locally activated")?;
        ensure!(
            record.scope() == membership.scope,
            "activation changed during replica catch-up"
        );
        self.sessions
            .lock()
            .unwrap()
            .insert(membership.scope.identity(), membership.replicas);
        Ok(())
    }

    pub async fn replace_replicas(
        &self,
        scope: &ReplicaScope,
        replicas: Vec<ReplicaTarget>,
        latest: Option<&(WritePlan, Vec<u8>)>,
        transport: &dyn StateTransport,
    ) -> Result<ReplicaMembership> {
        ensure!(
            replicas.len() == self.fleet.replica_regions().len(),
            "incomplete replacement replica set"
        );
        ReplicationTicket {
            replicas: replicas.clone(),
        }
        .validate()?;
        let id = scope.identity();
        let key = format!(
            "{}.json",
            crate::storage_paths::session(&scope.host, &scope.session)
        );
        let object = self.authority.get(&key).await?;
        let mut persisted: session::Session = match &object {
            Some(object) => serde_json::from_slice(&object.bytes)?,
            None => session::Session {
                id: id.clone(),
                region: scope.region.clone(),
                replicas: vec![],
                state: session::RecoveryState::Open,
            },
        };
        ensure!(
            persisted.is_open() && persisted.id == id && persisted.region == scope.region,
            "replication session changed or was fenced"
        );
        tokio::time::timeout(
            Duration::from_secs(10),
            self.seed_replicas(&id, &replicas, &persisted.replicas, latest, transport),
        )
        .await??;
        persisted.replicas = replicas.clone();
        ensure!(
            replace(
                self.authority.as_ref(),
                &key,
                object.map(|object| object.generation),
                serde_json::to_vec(&persisted)?
            )
            .await?,
            "replication replacement was fenced"
        );
        Ok(ReplicaMembership {
            scope: scope.clone(),
            replicas,
        })
    }

    pub(crate) async fn seed_replicas(
        &self,
        session: &str,
        replicas: &[ReplicaTarget],
        previous: &[ReplicaTarget],
        latest: Option<&(WritePlan, Vec<u8>)>,
        transport: &dyn StateTransport,
    ) -> Result<()> {
        let mut pending = JoinSet::new();
        for target in replicas {
            if previous.iter().any(|old| old.host_id == target.host_id) {
                continue;
            }
            let (peers, target, session) = (self.peers.clone(), target.clone(), session.to_owned());
            pending.spawn(async move { peers.initialize(&target, &session).await });
        }
        while let Some(result) = pending.join_next().await {
            result??;
        }
        if let Some((plan, bytes)) = latest {
            ensure!(
                plan.stream.session == session,
                "repair snapshot belongs to another activation"
            );
            plan.stream.snapshot(bytes)?;
            let writes = replicas.iter().map(|target| async move {
                let url = self.access.url(
                    &target.url,
                    &ReplicaGrant {
                        host_id: target.host_id.clone(),
                        stream: Some(plan.stream.clone()),
                        ..grant("APPEND", &target.region, &plan.stream.prefix, 60_000)?
                    },
                )?;
                transport.write(&url, bytes.clone()).await?;
                anyhow::Ok(())
            });
            futures_util::future::try_join_all(writes).await?;
        }
        Ok(())
    }
}
