use anyhow::Result;
use little_actors::replication::replica_regions;

#[test]
fn replica_placement_is_one_explicit_list_with_count_derived_from_it() -> Result<()> {
    assert!(replica_regions(&mut |_| None)?.is_empty());
    for input in [
        r#"["north-america-east","north-america-east"]"#,
        r#"["north-america-east","europe-west"]"#,
    ] {
        let regions = replica_regions(&mut |_| Some(input.into()))?;
        assert_eq!(regions.len(), 2);
    }
    assert!(replica_regions(&mut |_| Some(r#"["bad/region"]"#.into())).is_err());
    assert!(
        replica_regions(&mut |_| Some(serde_json::to_string(&vec!["us-east"; 9]).unwrap()))
            .is_err()
    );
    Ok(())
}
