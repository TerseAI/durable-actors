use super::*;

#[test]
fn deployment_accepts_the_customer_image_and_source_entrypoint_without_a_snapshot() {
    let request = serde_json::from_value::<RegisterDeploymentRequest>(serde_json::json!({
        "imageRef": "im-customer",
        "workingDirectory": "/project",
        "actorEntrypoint": "src/actors.ts",
        "secretRefs": ["project-secrets"]
    }));
    assert!(request.is_ok());
}

#[test]
fn deployment_registration_rejects_the_removed_image_warmup_option() {
    let request = serde_json::from_value::<RegisterDeploymentRequest>(serde_json::json!({
        "imageRef": "im-runtime",
        "workingDirectory": "/customer",
        "warmRegion": "north-america-west"
    }));
    assert!(request.is_err());
}
