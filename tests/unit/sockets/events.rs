use super::*;

#[test]
fn records_carry_their_project_and_the_resource_its_gateway() {
    let event = SocketEvent::Opened {
        actor: ActorKey {
            project_id: "alpha".into(),
            actor_name: "Room".into(),
            actor_id: "one".into(),
        },
        connection_id: "connection-1".into(),
        metadata: serde_json::json!({"user": "one"}),
        at_ms: 1_000,
    };
    let request = event.export_request("gateway-1");
    let resource = &request.resource_logs[0];
    let gateway = &resource.resource.as_ref().unwrap().attributes[0];
    assert_eq!(gateway.key, otlp::GATEWAY_ID);
    let record = &resource.scope_logs[0].log_records[0];
    assert_eq!(record.event_name, OPENED);
    assert_eq!(record.time_unix_nano, 1_000_000_000);
    let project = record
        .attributes
        .iter()
        .find(|attribute| attribute.key == otlp::PROJECT_ID)
        .and_then(|attribute| attribute.value.as_ref()?.value.clone());
    assert_eq!(project, Some(ProtoKind::StringValue("alpha".into())));
}
