use super::*;

impl RuntimeStorage {
    pub(crate) async fn register_initial_replicas(
        &self,
        scope: &ReplicaScope,
        replicas: Vec<ReplicaTarget>,
    ) -> Result<ReplicaMembership> {
        self.validate_initial_replicas(&replicas)?;
        let key = format!(
            "{}.json",
            crate::storage_paths::session(&scope.host, &scope.session)
        );
        let initial = session::Session {
            id: scope.identity(),
            region: scope.region.clone(),
            replicas,
            state: session::RecoveryState::Open,
        };
        let persisted = if replace(
            self.authority.as_ref(),
            &key,
            None,
            serde_json::to_vec(&initial)?,
        )
        .await?
        {
            initial
        } else {
            let object = self
                .authority
                .get(&key)
                .await?
                .context("replication session disappeared")?;
            serde_json::from_slice::<session::Session>(&object.bytes)?
        };
        ensure!(
            persisted.is_open()
                && persisted.id == scope.identity()
                && persisted.region == scope.region,
            "initial replication session changed or was fenced"
        );
        self.validate_initial_replicas(&persisted.replicas)?;
        Ok(ReplicaMembership {
            scope: scope.clone(),
            replicas: persisted.replicas,
        })
    }

    fn validate_initial_replicas(&self, replicas: &[ReplicaTarget]) -> Result<()> {
        ensure!(
            replicas.len() == self.fleet.replica_regions().len(),
            "incomplete initial replica set"
        );
        ReplicationTicket {
            replicas: replicas.to_vec(),
        }
        .validate()
    }
}
