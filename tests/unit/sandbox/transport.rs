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
