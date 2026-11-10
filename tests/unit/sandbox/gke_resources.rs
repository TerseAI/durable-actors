use serde_json::json;

use super::*;

fn pod(status: serde_json::Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "do-actor-one", "namespace": "sandboxes", "uid": "pod-uid"},
        "status": status,
    }))
    .unwrap()
}

fn terminated(reason: &str, exit_code: i32) -> serde_json::Value {
    json!({"phase": "Failed", "containerStatuses": [{
        "name": "runtime", "image": "runtime", "imageID": "", "ready": false, "restartCount": 0,
        "state": {"terminated": {"reason": reason, "exitCode": exit_code,
                                 "finishedAt": "2026-11-10T12:00:00Z"}},
    }]})
}

#[test]
fn finished_pods_become_kills_only_when_the_platform_killed_them() {
    let oom = kill(&pod(terminated("OOMKilled", 137))).unwrap();
    assert_eq!(oom.reason, KillReason::OutOfMemory);
    assert_eq!(oom.sandbox_id, "sandboxes/do-actor-one/pod-uid");
    assert_eq!(oom.killed_at_ms, 1_794_312_000_000);

    let sigkill = kill(&pod(terminated("Error", 137))).unwrap();
    assert_eq!(sigkill.reason, KillReason::Killed);

    let mut deleted = pod(terminated("Error", 137));
    deleted.metadata.deletion_timestamp =
        Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            "2026-11-10T12:00:00Z".parse().unwrap(),
        ));
    assert_eq!(kill(&deleted), None);
    assert_eq!(kill(&pod(terminated("Completed", 0))), None);

    let evicted = kill(&pod(json!({
        "phase": "Failed",
        "reason": "Evicted",
        "message": "Pod ephemeral local storage usage exceeds the total limit of containers 8Gi.",
    })))
    .unwrap();
    assert_eq!(evicted.reason, KillReason::Evicted);
    assert_eq!(
        evicted.message.as_deref(),
        Some("exceeded its temporary storage limit")
    );
}
