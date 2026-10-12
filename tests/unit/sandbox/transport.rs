use super::*;

#[test]
fn substrate_requests_address_the_actor_and_preserve_the_operation() -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let request = host_request(
        &client,
        "http://router.test/substrate/staging/host-123",
        "/v1/projects/demo/actors/Counter/one/invoke",
    )?
    .build()?;
    assert_eq!(
        request.url().as_str(),
        "http://router.test/v1/projects/demo/actors/Counter/one/invoke"
    );
    assert_eq!(request.headers()["ate-target-actor"], "staging/host-123");
    Ok(())
}

#[test]
fn local_requests_keep_their_address() -> anyhow::Result<()> {
    let request =
        host_request(&reqwest::Client::new(), "http://127.0.0.1:7101", "/assign")?.build()?;
    assert_eq!(request.url().as_str(), "http://127.0.0.1:7101/assign");
    assert!(request.headers().get("ate-target-actor").is_none());
    Ok(())
}

#[test]
fn malformed_actor_routes_are_rejected() {
    for route in [
        "http://router/substrate/space",
        "http://router/substrate/space/name/extra",
        "http://router/substrate/space/name?override=1",
        "http://router/substrate/space/%2fother",
    ] {
        assert!(
            host_request(&reqwest::Client::new(), route, "/assign").is_err(),
            "{route}"
        );
    }
}
