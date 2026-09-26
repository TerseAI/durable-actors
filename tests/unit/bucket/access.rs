use super::*;

#[test]
fn one_bucket_scopes_mutable_metadata_and_immutable_snapshots_separately() -> Result<()> {
    let boundary = boundary("actors");
    let rules = boundary["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0]["availableResource"], rules[1]["availableResource"]);
    assert!(
        rules[0]["availabilityCondition"]["expression"]
            .as_str()
            .unwrap()
            .contains("durable-actors/v3/owners/")
    );
    assert_eq!(
        rules[1]["availablePermissions"],
        json!([
            "inRole:roles/storage.objectViewer",
            "inRole:roles/storage.objectCreator"
        ])
    );
    assert!(
        rules[1]["availabilityCondition"]["expression"]
            .as_str()
            .unwrap()
            .contains("durable-actors/v3/snapshots/")
    );
    Ok(())
}
