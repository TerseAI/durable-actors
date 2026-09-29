use super::*;
use serde_json::json;

fn policy(durability: &str, zones: &[&str]) -> serde_json::Value {
    json!({"type":"replicated", "durability":durability, "replicas":zones.iter().enumerate().map(|(i, zone)| json!({
        "id":format!("replica-{i}"), "address":format!("http://replica-{i}:7200"), "zone":zone
    })).collect::<Vec<_>>()})
}

#[test]
fn replica_count_and_placement_are_configurable() -> Result<()> {
    for zones in [vec!["us-west4-a"], vec!["us-west4-a"; 3]] {
        let config: PersistenceConfig = serde_json::from_value(policy("zonal", &zones))?;
        config.validate()?;
    }
    Ok(())
}

#[test]
fn placement_enforces_the_selected_failure_domain() {
    for (mode, zones, valid) in [
        (
            "regional",
            vec!["us-west4-a", "us-west4-b", "us-west4-c"],
            true,
        ),
        ("regional", vec!["us-west4-a", "us-west4-a"], false),
        ("regional", vec!["us-west4-a", "us-west4-b"], true),
        ("multi_region", vec!["us-west4-a", "us-east4-a"], true),
        ("multi_region", vec!["us-west4-a", "us-west4-b"], false),
        ("zonal", vec![], false),
    ] {
        let config: PersistenceConfig = serde_json::from_value(policy(mode, &zones)).unwrap();
        assert_eq!(config.validate().is_ok(), valid, "{mode}: {zones:?}");
    }
}

#[test]
fn duplicate_nodes_cannot_count_as_independent_replicas() -> Result<()> {
    let mut value = policy("regional", &["us-west4-a", "us-west4-b"]);
    value["replicas"][1]["address"] = value["replicas"][0]["address"].clone();
    let config: PersistenceConfig = serde_json::from_value(value)?;
    assert!(config.validate().is_err());
    Ok(())
}
