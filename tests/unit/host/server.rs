use super::*;

#[tokio::test]
async fn warmed_listener_switches_to_assigned_routes_without_rebinding() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = HostServer::new(
        listener,
        axum::Router::new().route("/assign", axum::routing::post(|| async { "assigned" })),
    );
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .post(format!("http://{address}/assign"))
            .send()
            .await?
            .text()
            .await?,
        "assigned"
    );
    server.install(axum::Router::new().route("/invoke", axum::routing::post(|| async { "42" })))?;
    assert_eq!(
        client
            .post(format!("http://{address}/invoke"))
            .send()
            .await?
            .text()
            .await?,
        "42"
    );
    assert_eq!(
        client
            .post(format!("http://{address}/assign"))
            .send()
            .await?
            .status(),
        404
    );
    drop(server);
    Ok(())
}
