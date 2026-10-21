use super::*;
use serde_json::json;

fn without_identity(event: serde_json::Value) -> serde_json::Value {
    let mut event = event.as_object().unwrap().clone();
    assert!(
        event
            .remove("eventId")
            .and_then(|id| id.as_str().map(str::to_owned))
            .is_some_and(|id| !id.is_empty())
    );
    event.into()
}

#[tokio::test]
async fn started_events_describe_the_sandbox_reservation() -> Result<()> {
    let (usage, mut events) = testing::recorded();
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Chat".into(),
        actor_id: "room".into(),
    };
    let resources = ResourceLimits {
        cpu_millis: 500,
        memory_mib: 256,
    };
    usage.started(SandboxUsage::new(
        &HostId::new("host"),
        "gke",
        &actor,
        "north-america-west",
        &resources,
    ));
    let (key, event) = events.recv().await.unwrap();
    assert_eq!(key, "host");
    let mut event = without_identity(event);
    assert!(
        event
            .as_object_mut()
            .unwrap()
            .remove("occurredAtMs")
            .and_then(|at| at.as_u64())
            .is_some_and(|at| at > 0)
    );
    assert_eq!(
        event,
        json!({
            "type": "sandbox.started",
            "hostId": "host",
            "provider": "gke",
            "projectId": "project",
            "actorName": "Chat",
            "actorId": "room",
            "region": "north-america-west",
            "cpuMillis": 500,
            "memoryMib": 256,
        })
    );
    Ok(())
}

#[tokio::test]
async fn ended_events_keep_the_reported_finish_time() -> Result<()> {
    let (usage, mut events) = testing::recorded();
    usage.ended("host".into(), Some(1_234));
    let (key, event) = events.recv().await.unwrap();
    assert_eq!(key, "host");
    assert_eq!(
        without_identity(event),
        json!({"type": "sandbox.ended", "hostId": "host", "occurredAtMs": 1_234})
    );
    Ok(())
}

#[tokio::test]
async fn only_pubsub_topics_are_accepted() {
    for url in [
        "sqs://queue",
        "pubsub://topic",
        "pubsub://projects//topics/usage",
        "pubsub://projects/p/topics/usage/extra",
    ] {
        assert!(UsageEvents::connect(url).await.is_err(), "{url}");
    }
}
