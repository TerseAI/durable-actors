use super::*;

#[tokio::test]
async fn federation_uses_a_valid_provider_account_and_https_audience() -> Result<()> {
    let identity = FederatedIdentity::parse(
        r#"{"provider":"projects/123/locations/global/workloadIdentityPools/actors/providers/modal","serviceAccount":"host@test-project.iam.gserviceaccount.com"}"#,
    )?;
    identity.credentials(
        "https://control-plane.run.app",
        "modal-subject-token".into(),
    )?;
    assert!(
        identity
            .credentials("http://control-plane.run.app", "token".into())
            .is_err()
    );
    assert!(
        identity
            .credentials("https://control-plane.run.app", String::new())
            .is_err()
    );
    assert!(
        !format!("{:?}", ModalIdentity("modal-subject-token".into()))
            .contains("modal-subject-token")
    );
    Ok(())
}

#[tokio::test]
async fn metadata_identity_requires_an_https_origin() -> Result<()> {
    metadata_credentials("https://control-plane.run.app")?;
    for origin in [
        "http://control-plane.run.app",
        "https://control-plane.run.app/path",
        "https://user@control-plane.run.app",
    ] {
        assert!(metadata_credentials(origin).is_err());
    }
    Ok(())
}
