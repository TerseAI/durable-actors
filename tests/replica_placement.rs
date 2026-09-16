use anyhow::Result;
use little_actors::replication::{replica_count, replica_destinations, replica_regions};

#[test]
fn cross_region_preview_requires_distinct_explicit_replica_destinations() -> Result<()> {
    let config = |mode: &str, regions: Option<&str>| {
        let mut get = |name: &str| match name {
            "DURABLE_OBJECT_DURABILITY" => Some(mode.to_owned()),
            "DURABLE_OBJECT_REPLICA_COUNT" => Some("2".into()),
            "DURABLE_OBJECT_REPLICA_REGIONS" => regions.map(str::to_owned),
            _ => None,
        };
        let count = replica_count(&mut get)?;
        replica_regions(&mut get, count)
    };
    let remote = config(
        "cross_region_preview",
        Some(r#"["north-america-central","north-america-west"]"#),
    )?;
    assert_eq!(
        replica_destinations("north-america-east", 2, &remote)?,
        remote
    );
    assert!(replica_destinations("north-america-west", 2, &remote).is_err());
    assert!(config("cross_region_preview", None).is_err());
    assert!(
        config(
            "cross_region_preview",
            Some(r#"["north-america-west","north-america-west"]"#)
        )
        .is_err()
    );
    assert!(config("cross_region_preview", Some(r#"["north-america-west"]"#)).is_err());
    assert!(
        config(
            "zonal",
            Some(r#"["north-america-central","north-america-west"]"#)
        )
        .is_err()
    );
    assert_eq!(
        replica_destinations("north-america-east", 2, &[])?,
        vec!["north-america-east"; 2]
    );
    Ok(())
}
