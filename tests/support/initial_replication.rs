use super::*;

#[async_trait]
impl InitialReplicaSource for crate::bucket::access::RuntimeAccess {
    async fn prepare(&self, scope: &ReplicaScope) -> Result<ReplicaMembership> {
        self.initial_replicas(scope).await
    }
}
