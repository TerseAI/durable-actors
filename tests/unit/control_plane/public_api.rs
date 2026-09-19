use super::*;

#[test]
fn deployment_registration_accepts_a_background_warm_region() {
    let request: RegisterDeploymentRequest = serde_json::from_value(serde_json::json!({
        "codeRevision": "revision-1",
        "imageRef": "im-actor",
        "workingDirectory": "/workspace",
        "warmRegion": "north-america-west"
    }))
    .unwrap();

    assert_eq!(request.warm_region.as_deref(), Some("north-america-west"));
}
