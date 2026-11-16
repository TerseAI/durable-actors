use serde_json::json;

use super::*;

#[test]
fn out_of_memory_kills_are_read_from_the_stopped_container() {
    assert_eq!(
        kill(&stopped("OOMKilled", 137)).unwrap(),
        PodKill {
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
    let mut pod = pod_json(json!({
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

struct HungClaims;

#[async_trait::async_trait]
impl ClaimedActors for HungClaims {
    async fn actor_of(&self, _: &str) -> anyhow::Result<Option<ActorKey>> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn a_claim_read_that_hangs_is_given_up_so_the_watch_moves_on() {
    let reports = KillReports::new(
        kube::runtime::reflector::store().0,
        KillReporting {
            claims: Arc::new(HungClaims),
            events: Arc::new(crate::platform_events::NoPlatformEvents),
        },
    );
    tokio::time::timeout(
        CLAIM_READ_TIMEOUT * 2,
        reports.report(&stopped("OOMKilled", 137)),
    )
    .await
    .expect("the report gave up on the claim");
}

fn stopped(reason: &str, exit_code: i32) -> Pod {
    parse(stopped_json(reason, exit_code))
}

fn stopped_json(reason: &str, exit_code: i32) -> serde_json::Value {
    pod_json(json!({
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

fn pod_json(status: serde_json::Value) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "actor-pod",
            "namespace": "sandboxes",
            "uid": "actor-uid",
        },
        "status": status,
    })
}

fn parse(pod: serde_json::Value) -> Pod {
    serde_json::from_value(pod).unwrap()
}
