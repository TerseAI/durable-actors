use super::*;

#[test]
fn deployment_accepts_a_compiled_bundle() {
    let request = serde_json::from_value::<RegisterDeploymentRequest>(serde_json::json!({
        "bundle": {"bucket": "code-bucket", "files": [{"path": "actors.mjs", "object": "bundle/actors.mjs", "generation": 1, "sha256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}]},
        "contract": {"version": 1, "actors": []},
        "secretRefs": []
    }));
    assert!(request.is_ok());
}

#[test]
fn deployment_accepts_a_local_project() {
    let request = serde_json::from_value::<RegisterDeploymentRequest>(serde_json::json!({
        "localSource": {"workingDirectory": "/project", "actorEntrypoint": "src/actors.ts"},
        "secretRefs": ["project-secrets"]
    }));
    assert!(request.is_ok());
}
