use super::*;

#[test]
fn snapshot_paths_group_an_actor_and_validate_its_identity() -> Result<()> {
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "account.42".into(),
    };
    let object = snapshot_object_name(&actor, 7, "0123456789abcdef0123456789abcdef")?;
    assert!(object.starts_with("durable-actors/v3/snapshots/"));
    assert!(object.ends_with("/Q291bnRlcg/YWNjb3VudC40Mg/0123456789abcdef0123456789abcdef/7.json"));
    validate_snapshot_object_name(&actor, 7, &object)?;
    assert!(validate_snapshot_object_name(&actor, 8, &object).is_err());
    assert!(
        validate_snapshot_object_name(
            &ActorKey {
                actor_id: "other".into(),
                ..actor
            },
            7,
            &object
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn validates_the_only_storage_configuration() {
    assert!(validate_bucket("actors").is_ok());
    for bucket in ["", "gs://actors", " actors"] {
        assert!(validate_bucket(bucket).is_err());
    }
}
