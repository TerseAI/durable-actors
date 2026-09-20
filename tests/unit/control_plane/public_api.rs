use super::*;

#[test]
fn deployment_registration_rejects_the_removed_image_warmup_option() {
    let request = serde_json::from_value::<RegisterDeploymentRequest>(serde_json::json!({
        "codeRevision": "revision-1",
        "imageRef": "im-runtime",
        "codeSnapshot": "im-code",
        "workingDirectory": "/customer",
        "warmRegion": "north-america-west"
    }));
    assert!(request.is_err());
}
