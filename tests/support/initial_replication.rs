use super::*;

#[async_trait]
impl InitialReplicaSource for crate::bucket::access::RuntimeAccess {
    async fn targets(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        self.initial_replica_targets(scope).await
    }

    async fn prepare(&self, scope: &ReplicaScope) -> Result<ReplicaMembership> {
        self.initial_replicas(scope).await
    }
}
