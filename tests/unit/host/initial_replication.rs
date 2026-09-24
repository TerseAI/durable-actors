use super::*;

struct UnavailableTargets;

#[async_trait]
impl InitialReplicaSource for UnavailableTargets {
    async fn targets(&self, _: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        anyhow::bail!("connection discovery unavailable")
    }

    async fn prepare(&self, scope: &ReplicaScope) -> Result<ReplicaMembership> {
        Ok(serde_json::from_value(serde_json::json!({
            "scope": scope,
            "replicas": [{"hostId": "replica", "url": "http://127.0.0.1:1", "region": "us-east"}],
        }))?)
    }
}

#[tokio::test]
async fn connection_discovery_failure_does_not_block_membership() -> Result<()> {
    let scope = ReplicaScope {
        actor: crate::actor::ActorKey {
            project_id: "test".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        host: crate::host::HostId::new("primary"),
        session: "session".into(),
        region: "us-east".into(),
    };
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let initial = InitialReplication::start(
        Arc::new(UnavailableTargets),
        scope.clone(),
        true,
        stop,
        GrpcStateTransport::new(),
    );
    let membership = tokio::time::timeout(Duration::from_secs(1), initial.ready()).await??;
    assert_eq!(
        serde_json::to_value(membership)?["scope"],
        serde_json::to_value(scope)?
    );
    Ok(())
}
