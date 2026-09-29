use super::*;

#[test]
fn credentials_are_scoped_to_one_actor_and_its_deployed_code() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "tenant".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let code = crate::artifacts::ArtifactManifest {
        bucket: "code".into(),
        files: vec![crate::artifacts::ArtifactFile {
            path: "actors.mjs".into(),
            object: "durable-actors/v3/artifacts/deploy/actors.mjs".into(),
            generation: 1,
            sha256: "unused".into(),
        }],
    };
    let value = boundary(
        "authority",
        &super::super::PersistenceConfig::Replicated {
            durability: super::super::Durability::Zonal,
            placements: vec![],
        },
        Some("code"),
        &actor,
        Some(&code),
    )?;
    let rules = value["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(
        rules[0]["availabilityCondition"]["expression"],
        format!(
            "resource.name == {}",
            serde_json::to_string(&format!(
                "projects/_/buckets/authority/objects/{}",
                crate::storage_paths::owner(&actor.storage_key())?
            ))?
        )
    );
    assert_eq!(
        rules[1]["availabilityCondition"]["expression"],
        format!(
            "resource.name in {}",
            serde_json::to_string(&vec![format!(
                "projects/_/buckets/code/objects/{}",
                code.files[0].object
            )])?
        )
    );
    assert_eq!(
        rules[1]["availablePermissions"],
        json!(["inRole:roles/storage.objectViewer"])
    );
    let mut other = actor.clone();
    other.project_id = "other".into();
    assert_ne!(
        value,
        boundary(
            "authority",
            &super::super::PersistenceConfig::Local,
            Some("code"),
            &other,
            Some(&code)
        )?
    );
    assert!(
        boundary(
            "authority",
            &super::super::PersistenceConfig::Local,
            Some("different-bucket"),
            &actor,
            Some(&code)
        )
        .is_err()
    );
    Ok(())
}
