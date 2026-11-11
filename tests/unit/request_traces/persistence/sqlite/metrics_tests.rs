use super::fixtures::event;
use super::*;
use crate::request_traces::{
    RequestOutcome,
    metrics::{QueueWaitQuery, TimeRange},
};

#[tokio::test]
async fn overview_counts_reroutes_but_percentiles_and_success_use_attempts() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut events = Vec::new();
    for i in 1..=21 {
        let mut record = event(&i.to_string());
        record.trace.actor_name = "Room".into();
        record.trace.started_at_ms = 5000;
        record.trace.duration_ms = i as f64;
        record.trace.queue_wait_ms = Some(i as f64 * 2.0);
        events.push(record);
    }
    for (id, name, outcome, duration, queue, time) in [
        (
            "failed",
            "Room",
            RequestOutcome::Failed,
            100.0,
            Some(5.0),
            5000,
        ),
        (
            "reroute",
            "Room",
            RequestOutcome::Rerouted,
            900.0,
            Some(900.0),
            5000,
        ),
        (
            "rejected",
            "Room",
            RequestOutcome::Rejected,
            3.0,
            None,
            5000,
        ),
        (
            "counter",
            "Counter",
            RequestOutcome::Completed,
            7.0,
            Some(1.0),
            5000,
        ),
        (
            "counter-reroute",
            "Counter",
            RequestOutcome::Rerouted,
            8.0,
            Some(1.0),
            5000,
        ),
        (
            "idle",
            "Idle",
            RequestOutcome::Rerouted,
            1.0,
            Some(1.0),
            5000,
        ),
        (
            "old",
            "Room",
            RequestOutcome::Completed,
            5000.0,
            Some(5000.0),
            100,
        ),
    ] {
        let mut record = event(id);
        record.trace.actor_name = name.into();
        record.trace.outcome = outcome;
        record.trace.duration_ms = duration;
        record.trace.queue_wait_ms = queue;
        record.trace.started_at_ms = time;
        events.push(record);
    }
    store.append(&events).await?;
    let metrics = store
        .metrics(
            "default",
            &TimeRange {
                from_ms: Some(1000),
                to_ms: Some(5000),
            },
        )
        .await?;
    assert_eq!(metrics.total.count, 27);
    assert_eq!(metrics.total.success, Some(100.0 * 22.0 / 24.0));
    let room = metrics
        .classes
        .iter()
        .find(|row| row.actor_name == "Room")
        .unwrap();
    assert_eq!(room.count, 24);
    assert_eq!(room.p95, Some(21.0));
    assert_eq!(room.queue_p95, Some(40.0));
    let idle = metrics
        .classes
        .iter()
        .find(|row| row.actor_name == "Idle")
        .unwrap();
    assert_eq!(idle.count, 1);
    assert_eq!((idle.success, idle.p95, idle.queue_p95), (None, None, None));
    let old = store
        .metrics(
            "default",
            &TimeRange {
                from_ms: Some(0),
                to_ms: Some(999),
            },
        )
        .await?;
    assert_eq!(old.total.count, 1);
    assert_eq!(
        store
            .metrics("default", &TimeRange::default())
            .await?
            .total
            .count,
        28
    );
    Ok(())
}

#[tokio::test]
async fn queue_waits_group_admitted_attempts_and_filter_actor_and_time() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let mut events = Vec::new();
    for (id, actor, wait, outcome, time) in [
        ("one", "a", Some(10.0), RequestOutcome::Completed, 1000),
        ("two", "a", Some(30.0), RequestOutcome::Failed, 2000),
        ("three", "b", Some(90.0), RequestOutcome::Completed, 1500),
        ("reroute", "a", Some(999.0), RequestOutcome::Rerouted, 1500),
        ("rejected", "a", None, RequestOutcome::Rejected, 1500),
        ("old", "a", Some(999.0), RequestOutcome::Completed, 999),
    ] {
        let mut record = event(id);
        record.trace.actor_name = "Room".into();
        record.trace.actor_id = actor.into();
        record.trace.queue_wait_ms = wait;
        record.trace.outcome = outcome;
        record.trace.started_at_ms = time;
        events.push(record);
    }
    store.append(&events).await?;
    let query = QueueWaitQuery {
        from_ms: Some(1000),
        to_ms: Some(2000),
        actor_name: Some("Room".into()),
    };
    let rows = store.queue_waits("default", &query).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].actor_id, "a");
    assert_eq!(rows[0].admitted, 2);
    assert_eq!(rows[0].average_ms, 20.0);
    assert_eq!(rows[0].max_ms, 30.0);
    assert!(
        store
            .queue_waits(
                "default",
                &QueueWaitQuery {
                    actor_name: Some("Other".into()),
                    ..query
                }
            )
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn socket_events_assemble_connections_and_the_latest_heartbeat() -> Result<()> {
    use crate::sockets::events::{SocketClose, SocketEvent};
    let store = SqliteTracePersistence::in_memory();
    let actor = crate::actor::ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    let opened = |connection_id: &str, at_ms| SocketEvent::Opened {
        actor: actor.clone(),
        connection_id: connection_id.into(),
        metadata: serde_json::json!({"userId": connection_id}),
        at_ms,
    };
    store
        .append_socket_events(
            "gateway".into(),
            vec![
                opened("closed", 1000),
                opened("open", 2000),
                SocketEvent::Closed {
                    actor: actor.clone(),
                    connection_id: "closed".into(),
                    close: SocketClose {
                        code: 1001,
                        reason: "going away".into(),
                        clean: true,
                        messages_in: 2,
                        messages_out: 3,
                        bytes_in: 20,
                        bytes_out: 30,
                        duration_ms: 4000,
                    },
                    at_ms: 5000,
                },
                SocketEvent::Alive {
                    project_id: "default".into(),
                    at_ms: 6000,
                },
                SocketEvent::Alive {
                    project_id: "default".into(),
                    at_ms: 7000,
                },
                SocketEvent::Alive {
                    project_id: "other".into(),
                    at_ms: 8000,
                },
            ],
        )
        .await?;
    let records = store.socket_records("default", u64::MAX, false, 10).await?;
    let ids: Vec<_> = records
        .iter()
        .map(|record| record.connection_id.as_str())
        .collect();
    assert_eq!(ids, ["open", "closed"]);
    assert_eq!(
        records[0].metadata,
        Some(serde_json::json!({"userId": "open"}))
    );
    assert_eq!(
        records[1]
            .closed
            .as_ref()
            .map(|(at, close)| (*at, close.code)),
        Some((5000, 1001))
    );
    let open = store.socket_records("default", u64::MAX, true, 10).await?;
    assert_eq!(open.len(), 1);
    assert!(
        store
            .socket_records("default", 1500, false, 10)
            .await?
            .len()
            == 1
    );
    assert!(
        store
            .socket_records("other", u64::MAX, false, 10)
            .await?
            .is_empty()
    );
    assert_eq!(
        store.gateway_heartbeats("default").await?,
        std::collections::HashMap::from([("gateway".to_owned(), 7000)])
    );
    assert_eq!(
        store.gateway_heartbeats("other").await?,
        std::collections::HashMap::from([("gateway".to_owned(), 8000)])
    );
    Ok(())
}
