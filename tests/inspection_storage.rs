use anyhow::Result;
use little_actors::{
    actor::ActorKey,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
    placement::ObjectPlacementStore,
    state_transport::SnapshotWriter,
};

#[tokio::test]
async fn bucket_lists_snapshots_with_exact_namespace_filtering_and_pagination() -> Result<()> {
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
        std::sync::Arc::new(little_actors::bucket::HttpReplicaPeers::new(
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
    let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
    let host = HostId::new(format!("host.v2.{namespace}:test"));
    leases
        .register(&HostLeaseRequest {
            id: host.clone(),
            session_id: "session".into(),
            route: "http://localhost:7101".into(),
            duration_ms: 60_000,
        })
        .await?;
    for (scope, id, committed) in [
        (namespace.clone(), "a", true),
        (namespace.clone(), "b.with.dots", true),
        (namespace.clone(), "uncommitted", false),
        (format!("{namespace}.nested"), "c", true),
        (namespace.replace('_', "x"), "d", true),
    ] {
        let actor = ActorKey {
            namespace_id: scope,
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
    let objects = store.list_committed(Some(&namespace), None, 10).await?;
    assert_eq!(objects.len(), 2);
    assert!(objects[0].object.as_str().ends_with(":a"));
    assert!(objects[1].object.as_str().ends_with(":b.with.dots"));
    assert_eq!(
        store.list_committed(Some(&namespace), None, 1).await?,
        objects[..1]
    );
    assert_eq!(
        store
            .list_committed(Some(&namespace), Some(objects[0].object.as_str()), 1)
            .await?,
        objects[1..]
    );
    assert!(
        store
            .list_committed(Some(&namespace), Some(objects[1].object.as_str()), 1)
            .await?
            .is_empty()
    );
    let global = store
        .list_committed(None, Some(&format!("object.v2.{namespace}")), 100)
        .await?;
    assert!(global.contains(&objects[1]));
    assert!(global.iter().any(|object| {
        object
            .object
            .as_str()
            .contains(&format!("{namespace}.nested"))
    }));
    leases.unregister(&host, "session").await?;
    Ok(())
}
