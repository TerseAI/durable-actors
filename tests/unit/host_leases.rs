use super::*;

#[test]
fn lease_status_uses_the_store_clock() {
    let lease = HostLease {
        id: HostId::new("node-a"),
        session_id: "session-a".into(),
        route: "node-a".into(),
        expires_at_ms: 1_000,
    };

    let live = HostLeaseStatus {
        lease: Some(lease.clone()),
        store_now_ms: 999,
    };
    let expired = HostLeaseStatus {
        lease: Some(lease),
        store_now_ms: 1_000,
    };
    let absent = HostLeaseStatus {
        lease: None,
        store_now_ms: 0,
    };

    assert!(live.is_active());
    assert!(!expired.is_active());
    assert!(!absent.is_active());
    let encoded = serde_json::to_value(live).expect("serialize lease status");
    assert_eq!(encoded["registry_now_ms"], 999);
    assert!(encoded.get("store_now_ms").is_none());
}
