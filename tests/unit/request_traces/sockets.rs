use super::*;

fn record(connection_id: &str, gateway_id: &str, closed: Option<u64>) -> SocketRecord {
    SocketRecord {
        connection_id: connection_id.into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
        gateway_id: gateway_id.into(),
        opened_at_ms: 1_000,
        metadata: Some(serde_json::json!({"user": connection_id})),
        closed: closed.map(|at_ms| {
            (
                at_ms,
                SocketClose {
                    code: 1000,
                    reason: "done".into(),
                    clean: true,
                    messages_in: 3,
                    messages_out: 5,
                    bytes_in: 30,
                    bytes_out: 50,
                    duration_ms: at_ms - 1_000,
                },
            )
        }),
    }
}

#[test]
fn unclosed_connections_are_open_only_while_their_gateway_reports() {
    let heartbeats = HashMap::from([("live".to_owned(), 99_000), ("stopped".to_owned(), 40_000)]);
    let mut just_opened = record("just-opened", "first-tick-pending", None);
    just_opened.opened_at_ms = 95_000;
    let found = sessions(
        vec![
            record("closed", "live", Some(2_000)),
            record("open", "live", None),
            record("lost", "stopped", None),
            record("never-reported", "unknown", None),
            just_opened,
        ],
        &heartbeats,
        100_000,
    );
    let statuses: Vec<_> = found
        .iter()
        .map(|session| (session.status, session.closed_at_ms))
        .collect();
    assert_eq!(
        statuses,
        vec![
            (SocketStatus::Closed, Some(2_000)),
            (SocketStatus::Open, None),
            (SocketStatus::Lost, Some(40_000)),
            (SocketStatus::Lost, Some(1_000)),
            (SocketStatus::Open, None),
        ]
    );
    assert_eq!(
        (
            found[0].close_code,
            found[0].messages_in,
            found[0].messages_out
        ),
        (Some(1000), Some(3), Some(5))
    );
    assert!(overlaps(&found[0], 0, 5_000) && !overlaps(&found[0], 3_000, 5_000));

    let open = open_connections("alpha", found, &heartbeats);
    assert_eq!(open.rooms.len(), 1);
    let (actor, connections) = &open.rooms[0];
    assert_eq!(actor.project_id, "alpha");
    assert_eq!(
        connections
            .iter()
            .map(|connection| (connection.id.as_str(), connection.gateway_id.as_str()))
            .collect::<Vec<_>>(),
        vec![("just-opened", "first-tick-pending"), ("open", "live")]
    );
    assert_eq!(connections[1].metadata, serde_json::json!({"user": "open"}));
    assert_eq!(
        open.gateways,
        BTreeMap::from([
            ("first-tick-pending".to_owned(), 95_000),
            ("live".to_owned(), 99_000)
        ])
    );
}
