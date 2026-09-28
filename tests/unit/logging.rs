use super::LogExportConfig;

#[test]
fn log_configuration_rejects_credentials_in_urls_and_invalid_secret_references() {
    for endpoint in [
        "file:///tmp/logs",
        "https://user:secret@example.test/v1/logs",
        "https://example.test/v1/logs?key=secret",
        "not a URL",
    ] {
        let config = LogExportConfig {
            endpoint: endpoint.into(),
            headers_env: None,
        };
        let error = config.validate().unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
    let config = LogExportConfig {
        endpoint: "https://example.test/v1/logs".into(),
        headers_env: Some("bad-name".into()),
    };
    assert!(config.validate().is_err());
}
