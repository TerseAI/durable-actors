use serde_json::json;

use super::*;

#[test]
fn out_of_memory_kills_name_the_annotated_actor() {
    assert_eq!(
        kill(&stopped("OOMKilled", 137)).unwrap(),
        SandboxKill {
            actor: ActorKey {
                project_id: "project".into(),
                actor_name: "Counter".into(),
                actor_id: "counter-1".into(),
            },
            sandbox_id: "sandboxes/actor-pod/actor-uid".into(),
            reason: KillReason::OutOfMemory,
            message: None,
            at_ms: 1_767_225_600_000,
        }
    );
}

#[test]
fn out_of_memory_counts_even_while_the_platform_deletes_the_pod() {
    let mut pod = stopped_json("OOMKilled", 137);
    pod["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:05Z");
    assert_eq!(
        kill(&parse(pod)).map(|kill| kill.reason),
        Some(KillReason::OutOfMemory)
    );
}

#[test]
fn sigkill_only_counts_when_nothing_asked_the_pod_to_stop() {
    assert_eq!(
        kill(&stopped("Error", 137)).map(|kill| kill.reason),
        Some(KillReason::Killed)
    );
    let mut retired = stopped_json("Error", 137);
    retired["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:05Z");
    assert_eq!(kill(&parse(retired)), None);
    assert_eq!(kill(&stopped("Completed", 0)), None);
}

#[test]
fn evictions_explain_a_full_temporary_disk() {
    let mut pod = annotated(json!({
        "phase": "Failed",
        "reason": "Evicted",
        "message": "Pod ephemeral local storage usage exceeds the total limit of containers 8Gi.",
        "conditions": [
            {"type": "Ready", "status": "False", "lastTransitionTime": "2026-01-01T00:00:03Z"},
            {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2025-12-31T23:00:00Z"},
        ],
    }));
    let evicted = kill(&parse(pod.clone())).unwrap();
    assert_eq!(evicted.reason, KillReason::Evicted);
    assert_eq!(
        evicted.message.as_deref(),
        Some("exceeded its temporary storage limit")
    );
    assert_eq!(evicted.at_ms, 1_767_225_603_000);
    pod["status"]["message"] = json!("The node was low on resource: memory.");
    assert_eq!(kill(&parse(pod)).unwrap().message, None);
}

#[test]
fn pods_without_an_actor_report_nothing() {
    let mut pod = stopped_json("OOMKilled", 137);
    pod["metadata"]["annotations"]
        .as_object_mut()
        .unwrap()
        .remove("terse.ai/actor-id");
    assert_eq!(kill(&parse(pod)), None);
}

#[test]
fn annotations_round_trip_the_actor() {
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "counter/1 with spaces".into(),
    };
    let mut pod = stopped_json("OOMKilled", 137);
    pod["metadata"]["annotations"] = json!(actor_annotations(&actor));
    assert_eq!(kill(&parse(pod)).unwrap().actor, actor);
}

fn stopped(reason: &str, exit_code: i32) -> Pod {
    parse(stopped_json(reason, exit_code))
}

fn stopped_json(reason: &str, exit_code: i32) -> serde_json::Value {
    annotated(json!({
        "phase": "Failed",
        "containerStatuses": [{
            "name": "runtime",
            "image": "runtime",
            "imageID": "",
            "ready": false,
            "restartCount": 0,
            "state": {"terminated": {
                "exitCode": exit_code,
                "reason": reason,
                "finishedAt": "2026-01-01T00:00:00Z",
            }},
        }],
    }))
}

fn annotated(status: serde_json::Value) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "actor-pod",
            "namespace": "sandboxes",
            "uid": "actor-uid",
            "annotations": {
                "terse.ai/project-id": "project",
                "terse.ai/actor-name": "Counter",
                "terse.ai/actor-id": "counter-1",
            },
        },
        "status": status,
    })
}

fn parse(pod: serde_json::Value) -> Pod {
    serde_json::from_value(pod).unwrap()
}
