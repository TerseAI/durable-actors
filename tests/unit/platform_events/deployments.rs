use super::*;
use opentelemetry_proto::tonic::common::v1::any_value::Value as ProtoKind;

#[test]
fn published_contracts_travel_in_the_body_under_their_project() {
    let event = DeploymentEvent::Published {
        project_id: "alpha".into(),
        contract: Some(serde_json::json!({"version": 1, "actors": []})),
        at_ms: 1_000,
    };
    let request = event.to_record().export_request("gateway-1");
    let resource = &request.resource_logs[0];
    assert_eq!(
        resource.resource.as_ref().unwrap().attributes[0].key,
        otlp::GATEWAY_ID
    );
    let record = &resource.scope_logs[0].log_records[0];
    assert_eq!(record.event_name, PUBLISHED);
    assert_eq!(record.time_unix_nano, 1_000_000_000);
    assert_eq!(record.attributes[0].key, otlp::PROJECT_ID);
    let Some(ProtoKind::StringValue(body)) =
        record.body.as_ref().and_then(|body| body.value.clone())
    else {
        panic!("the contract is the body");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        serde_json::json!({"version": 1, "actors": []})
    );
    let removed = DeploymentEvent::Removed {
        project_id: "alpha".into(),
        at_ms: 2_000,
    };
    assert!(removed.to_record().body.is_none());
}
