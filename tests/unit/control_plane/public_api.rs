use super::*;

#[test]
fn deployment_accepts_a_source_archive() {
    let request = serde_json::from_value::<RegisterDeploymentRequest>(serde_json::json!({
        "sourceArchive": {"sha256": "a".repeat(64), "entrypoint": "src/actor.ts", "object": {"bucket": "source-bucket", "name": "project/source.zip", "generation": "1"}},
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
