use super::*;

#[test]
fn customer_pod_uses_managed_gvisor_without_ambient_credentials() -> Result<()> {
    let request = CreateSpareRequest { control_plane_url: Some("http://control:7100".into()), kind: SpareKind::Actor, name: "warm-one".into(), image_ref: "runtime@sha256:abc".into(), canonical_region: "north-america-west".into(), resources: ResourceLimits { cpu_millis: 2000, memory_mib: 4096 } };
    let pod = spare_pod(&request, "us-west4-a", "test-token")?;
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
    assert_eq!(spec.automount_service_account_token, Some(false));
    assert_eq!(spec.node_selector.unwrap()["topology.kubernetes.io/zone"], "us-west4-a");
    assert_eq!(spec.containers[0].resources.as_ref().unwrap().limits.as_ref().unwrap()["memory"].0, "4096Mi");
    assert_eq!(spec.containers[0].security_context.as_ref().unwrap().allow_privilege_escalation, Some(false));
    Ok(())
}

#[test]
fn resource_identity_requires_namespace_name_and_uid() {
    assert_eq!(resource_identity("sandboxes/warm/uid").unwrap(), ("sandboxes", "warm", "uid"));
    for bad in ["", "warm", "sandboxes/warm", "sandboxes/warm/", "sandboxes/warm/uid/extra"] { assert!(resource_identity(bad).is_err()); }
}
