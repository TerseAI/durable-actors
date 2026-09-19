#[tokio::test]
async fn rejects_http_and_file_storage_capabilities() {
    for url in [
        "file:///tmp/state",
        "https://host/_replica/state?token=secret",
        "grpc://host?token=first&token=second",
    ] {
        assert!(crate::grpc::transport::capability(url).is_err());
    }
}
