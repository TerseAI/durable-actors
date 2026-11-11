use prost::Message;
use serde_json::json;

use super::*;
use crate::{
    actor::ActorKey,
    actor_lifecycle::{LifecycleKind, LifecycleReport},
    platform_events::{PlatformEvent, deployments::DeploymentEvent},
    sockets::events::{SocketClose, SocketEvent},
};

fn payload(event: &impl PlatformEvent) -> Vec<u8> {
    event.to_record().export_request("gateway").encode_to_vec()
}

fn actor() -> ActorKey {
    ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    }
}

fn decoded(payload: &[u8]) -> serde_json::Value {
    let events = decode(Signal::Platform, payload, &LogQuery::default());
    serde_json::to_value(&events).unwrap()
}

#[test]
fn lifecycle_reports_decode_to_the_instance_they_leave() {
    let report = LifecycleEvent {
        organization_id: None,
        event_id: "session:2".into(),
        actor: actor(),
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us-east".into(),
        received_at_ms: 5_000,
        report: LifecycleReport {
            kind: LifecycleKind::Stopped,
            owner_epoch: 1,
            sequence: 2,
            started_at_ms: 1_000,
            occurred_at_ms: 4_900,
            resources: Default::default(),
            resident: Some(true),
            last_active_at_ms: Some(4_000),
            waiting: vec![],
        },
    };
    assert_eq!(
        decoded(&payload(&report)),
        json!([{
            "actorName": "Room", "actorId": "one", "status": "dormant",
            "occurredAtMs": 4_900, "reportedAtMs": 5_000, "staleAtMs": null,
            "lastActiveAtMs": 4_000, "waiting": []
        }])
    );
}

#[test]
fn connection_events_decode_with_their_gateway() {
    let opened = SocketEvent::Opened {
        actor: actor(),
        connection_id: "c1".into(),
        metadata: json!({"user": "u"}),
        at_ms: 1_000,
    };
    let closed = SocketEvent::Closed {
        actor: actor(),
        connection_id: "c1".into(),
        close: SocketClose {
            code: 1000,
            reason: "done".into(),
            clean: true,
            messages_in: 2,
            messages_out: 3,
            bytes_in: 0,
            bytes_out: 0,
            duration_ms: 4_000,
        },
        at_ms: 5_000,
    };
    let alive = SocketEvent::Alive {
        project_id: "default".into(),
        at_ms: 6_000,
    };
    assert_eq!(
        decoded(&payload(&opened)),
        json!([{
            "connectionId": "c1", "actorName": "Room", "actorId": "one",
            "gatewayId": "gateway", "openedAtMs": 1_000, "metadata": {"user": "u"}
        }])
    );
    assert_eq!(
        decoded(&payload(&closed)),
        json!([{
            "connectionId": "c1", "actorName": "Room", "actorId": "one", "gatewayId": "gateway",
            "openedAtMs": 1_000, "closedAtMs": 5_000, "closeCode": 1000, "closeReason": "done",
            "messagesIn": 2, "messagesOut": 3
        }])
    );
    assert_eq!(
        decoded(&payload(&alive)),
        json!([{"gatewayId": "gateway", "atMs": 6_000}])
    );
}

#[test]
fn deployments_decode_to_their_actor_classes() {
    let published = DeploymentEvent::Published {
        project_id: "default".into(),
        contract: Some(
            serde_json::from_str(include_str!(
                "../../../sdk/tests/fixtures/public-contract.json"
            ))
            .unwrap(),
        ),
        at_ms: 1_000,
    };
    let events = decode(Signal::Platform, &payload(&published), &LogQuery::default());
    let [ObserveEvent::Deployment(deployment)] = events.as_slice() else {
        panic!("one deployment: {events:?}");
    };
    assert!(deployment.actors.contains(&DeployedActor {
        actor_name: "ChatRoom".into(),
        methods: vec!["clear".into(), "sendMessage".into()],
    }));
    let removed = DeploymentEvent::Removed {
        project_id: "default".into(),
        at_ms: 2_000,
    };
    assert_eq!(
        decoded(&payload(&removed)),
        json!([{"atMs": 2_000, "actors": []}])
    );
}

#[test]
fn batches_keep_arrival_order_across_types() {
    let alive = |at_ms| {
        ObserveEvent::GatewayAlive(GatewayAlive {
            gateway_id: "gateway".into(),
            at_ms,
        })
    };
    let deployment = || {
        ObserveEvent::Deployment(Deployment {
            at_ms: 0,
            actors: vec![],
        })
    };
    let names: Vec<_> = batches(vec![alive(1), alive(2), deployment(), alive(3)])
        .into_iter()
        .map(|batch| (batch.name, batch.events.len()))
        .collect();
    assert_eq!(
        names,
        vec![("gatewayAlive", 2), ("deployment", 1), ("gatewayAlive", 1)]
    );
}

#[test]
fn signals_name_the_live_channels() {
    let query = |signals: &str| EventsQuery {
        signals: signals.into(),
        ..Default::default()
    };
    assert_eq!(query("requests, platform").signals().unwrap().len(), 2);
    assert!(query("lifecycle").signals().is_err());
    assert!(query("").signals().is_err());
}
