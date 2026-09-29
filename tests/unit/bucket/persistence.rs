use super::*;

fn policy(durability: Durability, zones: &[&str]) -> PersistenceConfig {
    PersistenceConfig::Rapid {
        durability,
        buckets: zones.iter().enumerate().map(|(i, zone)| RapidBucket { name: format!("rapid-{i}"), zone: (*zone).into() }).collect(),
    }
}

#[test]
fn durability_requires_the_declared_failure_domains() {
    assert!(policy(Durability::Zonal, &["us-west4-a"]).validate().is_ok());
    assert!(policy(Durability::Regional, &["us-west4-a", "us-west4-b"]).validate().is_ok());
    assert!(policy(Durability::MultiRegion, &["us-west4-a", "us-east4-a"]).validate().is_ok());
    assert!(policy(Durability::Regional, &["us-west4-a", "us-west4-a"]).validate().is_err());
    assert!(policy(Durability::Regional, &["us-west4-a", "us-east4-a"]).validate().is_err());
    assert!(policy(Durability::MultiRegion, &["us-west4-a", "us-west4-b"]).validate().is_err());
    assert!(policy(Durability::Zonal, &[]).validate().is_err());
    assert!(policy(Durability::Zonal, &["us-west4"]).validate().is_err());
}

#[test]
fn two_names_for_the_same_bucket_cannot_count_as_independent_copies() {
    let mut config = policy(Durability::Regional, &["us-west4-a", "us-west4-b"]);
    let PersistenceConfig::Rapid { buckets, .. } = &mut config else { unreachable!() };
    buckets[1].name = buckets[0].name.clone();
    assert!(config.validate().is_err());
}

#[test]
fn bucket_metadata_must_confirm_rapid_and_the_configured_zone() {
    let bucket = RapidBucket { name: "rapid".into(), zone: "us-west4-a".into() };
    assert!(bucket.validate_placement("RAPID", "US-WEST4-A").is_ok());
    assert!(bucket.validate_placement("STANDARD", "US-WEST4-A").is_err());
    assert!(bucket.validate_placement("RAPID", "US-WEST4-B").is_err());
}
