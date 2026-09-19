use super::*;

#[test]
fn replica_capabilities_bind_operation_expiry_and_stream() -> Result<()> {
    let access = ReplicaAccess::new("secret", Arc::new(crate::clock::SystemClock));
    let grant = ReplicaGrant {
        stream: None,
        operation: "GET".into(),
        object: "little-actors/v2/snapshots/aa/test/1.json".into(),
        region: "us-east".into(),
        host_id: "replica".into(),
        archive_url: String::new(),
        expires_at_ms: u64::MAX,
    };
    let url = access.url("http://replica", &grant)?;
    let token = url.split("token=").nth(1).unwrap();
    access.verify(token, "GET")?;
    assert!(access.verify(token, "PUT").is_err());
    assert!(
        ReplicaAccess::new("other-secret", Arc::new(crate::clock::SystemClock))
            .verify(token, "GET")
            .is_err()
    );
    for invalid in [
        ReplicaGrant {
            expires_at_ms: 0,
            ..grant.clone()
        },
        ReplicaGrant {
            object: "outside/runtime.json".into(),
            ..grant.clone()
        },
        ReplicaGrant {
            stream: Some(super::super::ReplicaStream {
                prefix: "another-object".into(),
                session: "little-actors/v2/hosts/host/sessions/one/".into(),
                owner_epoch: 1,
                base_version: 0,
            }),
            ..grant.clone()
        },
    ] {
        let url = access.url("http://replica", &invalid)?;
        assert!(
            access
                .verify(url.split("token=").nth(1).unwrap(), "GET")
                .is_err()
        );
    }
    Ok(())
}
