use super::*;
use anyhow::Result;

#[test]
fn hybrid_selection_uses_resolved_cpu_and_memory() -> Result<()> {
    let defaults = ResourceLimits {
        cpu_millis: 250,
        memory_mib: 128,
    };
    let options = serde_json::from_value(serde_json::json!({
        "Explicit": {"cpu": 0.25, "memoryMiB": 128},
        "Idle": {"idleTimeoutMs": 60000},
        "Region": {"regions": ["north-america-west"]},
        "Cpu": {"cpu": 1},
        "Memory": {"memoryMiB": 512}
    }))?;
    let plan = RuntimePlan::resolve(defaults.clone(), &options, true);
    for name in ["Implicit", "Explicit", "Idle", "Region"] {
        assert_eq!(plan.backend(name), RuntimeBackend::Gke);
    }
    for name in ["Cpu", "Memory"] {
        assert_eq!(plan.backend(name), RuntimeBackend::Substrate);
    }
    let restored: RuntimePlan = serde_json::from_slice(&serde_json::to_vec(&plan)?)?;
    assert_eq!(restored, plan);
    assert_eq!(restored.default_resources, defaults);
    Ok(())
}

#[test]
fn gke_only_installations_keep_custom_shapes_on_gke() -> Result<()> {
    let options = serde_json::from_value(serde_json::json!({"Large": {"cpu": 4}}))?;
    let plan = RuntimePlan::resolve(ResourceLimits::default(), &options, false);
    assert_eq!(plan.backend("Large"), RuntimeBackend::Gke);
    Ok(())
}

#[test]
fn readiness_dispatch_uses_the_backend_recorded_in_the_host_identity() {
    let host = RuntimeBackend::Substrate.host_id("cfg.test");
    assert_eq!(RuntimeBackend::for_host(&host), RuntimeBackend::Substrate);
    let pod = RuntimeBackend::Gke.host_id("cfg.test");
    assert_eq!(RuntimeBackend::for_host(&pod), RuntimeBackend::Gke);
    assert!(host.as_str().starts_with("host.v3.cfg.test."));
}
