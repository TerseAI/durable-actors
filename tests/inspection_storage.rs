use anyhow::Result;
use little_actors::{
    actor::ActorKey,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
    placement::ObjectPlacementStore,
    state_transport::SnapshotWriter,
};

#[tokio::test]
async fn bucket_lists_committed_snapshots_with_pagination() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = std::sync::Arc::new(little_actors::bucket::FileBucket::new(
        directory.path().into(),
    )?);
    let clock = std::sync::Arc::new(little_actors::clock::SystemClock);
    let leases = std::sync::Arc::new(little_actors::bucket::BucketHostLeases::new(
        bucket.clone(),
        clock.clone(),
    ));
    let access = little_actors::replication::ReplicaAccess::new("test", clock);
    let store = little_actors::bucket::RuntimeStorage::new(
        bucket,
        leases.clone(),
        std::sync::Arc::new(EmptyFleet),
        std::sync::Arc::new(little_actors::bucket::GrpcReplicaPeers::new(
            access.clone(),
        )?),
        access,
        "http://unused".into(),
    )?;
    check_listing(&store, leases.as_ref()).await
}

struct EmptyFleet;
#[async_trait::async_trait]
impl little_actors::replication::ReplicaProvisioner for EmptyFleet {
    fn replica_regions(&self) -> Vec<String> {
        Vec::new()
    }

    async fn ensure(
        &self,
        _: &ActorKey,
        _: &str,
    ) -> Result<Vec<little_actors::replication::ReplicaTarget>> {
        Ok(vec![])
    }
}

async fn check_listing(
    store: &little_actors::bucket::RuntimeStorage,
    leases: &dyn HostLeaseRegistry,
) -> Result<()> {
    let host = HostId::new(format!("host.v3.test.{}", uuid::Uuid::new_v4()));
    leases
        .register(&HostLeaseRequest {
            id: host.clone(),
            session_id: "session".into(),
            route: "http://localhost:7101".into(),
            duration_ms: 60_000,
        })
        .await?;
    for (id, committed) in [("a", true), ("b.with.dots", true), ("uncommitted", false)] {
        let actor = ActorKey {
            actor_type: "Room.with.dots".into(),
            actor_id: id.into(),
        };
        store.claim_actor(&actor, None, &host, "us-east").await?;
        if committed {
            let plan = store.prepare_write("us-east", &actor, 1).await?;
            let snapshot = little_actors::state_log::StateSnapshot::new(
                1,
                1,
                id.into(),
                serde_json::json!({"value": id}),
                serde_json::Value::Null,
            )?;
            SnapshotWriter::write_snapshot(store, &plan, snapshot.encode()?).await?;
        }
    }
    let objects = store.list_committed(None, 10).await?;
    assert_eq!(objects.len(), 2);
    assert!(objects[0].object.as_str().ends_with(":a"));
    assert!(objects[1].object.as_str().ends_with(":b.with.dots"));
    assert_eq!(store.list_committed(None, 1).await?, objects[..1]);
    assert_eq!(
        store
            .list_committed(Some(objects[0].object.as_str()), 1)
            .await?,
        objects[1..]
    );
    assert!(
        store
            .list_committed(Some(objects[1].object.as_str()), 1)
            .await?
            .is_empty()
    );
    leases.unregister(&host, "session").await?;
    Ok(())
}
