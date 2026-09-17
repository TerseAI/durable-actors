use anyhow::Result;
use little_actors::{
    actor::ActorKey,
    host::HostId,
    host_leases::{HostLeaseRegistry, HostLeaseRequest, PostgresHostLeaseStore},
    placement::{ObjectPlacementStore, PostgresObjectPlacementStore, StateCommitRequest},
    sqlite::SqliteStore,
    storage_urls::snapshot_object_name,
};

#[tokio::test]
async fn sqlite_lists_committed_objects_with_exact_namespace_filtering_and_pagination() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let store = SqliteStore::open(&directory.path().join("runtime.sqlite")).await?;
    check_listing(&store, &store).await
}

#[tokio::test]
async fn postgres_lists_committed_objects_with_exact_namespace_filtering_and_pagination()
-> Result<()> {
    let Ok(url) = std::env::var("DURABLE_OBJECT_TEST_POSTGRES_URL") else {
        return Ok(());
    };
    let store = PostgresObjectPlacementStore::connect(&url).await?;
    let leases = PostgresHostLeaseStore::connect(&url).await?;
    check_listing(&store, &leases).await
}

async fn check_listing(
    store: &dyn ObjectPlacementStore,
    leases: &dyn HostLeaseRegistry,
) -> Result<()> {
    let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
    let host = HostId::new(namespace.clone());
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
        store
            .claim(&actor.storage_key(), None, &host, "us-east")
            .await?;
        if committed {
            store
                .commit_state(&StateCommitRequest {
                    object: actor.storage_key(),
                    owner: host.clone(),
                    session_id: "session".into(),
                    owner_epoch: 1,
                    expected_version: 0,
                    state_object: snapshot_object_name(
                        &actor,
                        1,
                        &uuid::Uuid::new_v4().simple().to_string(),
                    )?,
                    request_id: id.into(),
                })
                .await?;
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
