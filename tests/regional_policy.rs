use durable_actors::regional::Region;

#[test]
fn regional_aliases_resolve_to_canonical_names() {
    for (name, canonical) in [
        ("us-west", "north-america-west"),
        ("us-central", "north-america-central"),
        ("us-east", "north-america-east"),
    ] {
        let home: Region = name.parse().unwrap();
        assert_eq!(home.as_str(), canonical);
    }
    assert!("europe-west".parse::<Region>().is_err());
}

#[test]
fn placement_requires_gcp_and_the_requested_region() {
    let home: Region = "us-east".parse().unwrap();
    assert!(home.attest("gcp", "us-east4").is_ok());
    assert!(home.attest("CLOUD_PROVIDER_GCP", "us-east").is_ok());
    assert!(home.attest("GCP", "us-east").is_ok());
    assert!(home.attest("CLOUD_PROVIDER_AWS", "us-east").is_err());
    assert!(home.attest("aws", "us-east-1").is_err());
    assert!(home.attest("gcp", "us-west1").is_err());
    assert!(home.attest("", "us-east4").is_err());
}
