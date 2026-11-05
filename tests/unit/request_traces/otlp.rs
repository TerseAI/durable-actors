use opentelemetry_proto::tonic::{
    logs::v1::{ResourceLogs, ScopeLogs},
    trace::v1::{ResourceSpans, ScopeSpans},
};

use super::*;
use crate::request_traces::{RequestKind, RequestOutcome};

#[test]
fn stamping_replaces_every_tenant_attribute_from_the_payload() {
    let mut resource = Some(Resource {
        attributes: vec![
            string_attribute(PROJECT_ID, "victim"),
            string_attribute("durable_actors.anything", "forged"),
            string_attribute("service.name", "durable-actors-host"),
        ],
        ..Default::default()
    });
    identity().stamp(&mut resource);
    let attributes = resource.unwrap().attributes;
    assert_eq!(string(&attributes, PROJECT_ID).as_deref(), Some("default"));
    assert_eq!(string(&attributes, HOST_ID).as_deref(), Some("host"));
    assert_eq!(string(&attributes, "durable_actors.anything"), None);
    assert_eq!(
        string(&attributes, "service.name").as_deref(),
        Some("durable-actors-host")
    );
}

#[test]
fn spans_decode_into_request_traces() {
    let span = Span {
        span_id: vec![0, 240, 103, 170, 11, 169, 2, 183],
        name: "increment".into(),
        start_time_unix_nano: 1_000_000_000,
        end_time_unix_nano: 1_012_500_000,
        attributes: vec![
            string_attribute(REQUEST_ID, "request-1"),
            string_attribute(KIND, "method"),
            string_attribute(OUTCOME, "failed"),
            string_attribute(HOST_STATE, "cold"),
            number_attribute(ROUTING_MS, 1.5),
            number_attribute(QUEUE_WAIT_MS, 3.0),
            int_attribute(STATE_VERSION, 4),
        ],
        ..Default::default()
    };
    let mut resource = None;
    identity().stamp(&mut resource);
    let events = trace_events(ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource,
            scope_spans: vec![ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.event_id, "00f067aa0ba902b7");
    assert_eq!(
        (event.host_id.as_str(), event.session_id.as_str()),
        ("host", "session")
    );
    let trace = &event.trace;
    assert_eq!(trace.project_id, "default");
    assert_eq!(
        (trace.actor_name.as_str(), trace.actor_id.as_str()),
        ("Counter", "one")
    );
    assert_eq!(trace.kind, RequestKind::Method);
    assert_eq!(trace.outcome, RequestOutcome::Failed);
    assert_eq!(trace.started_at_ms, 1000);
    assert_eq!(trace.duration_ms, 12.5);
    assert_eq!(trace.queue_wait_ms, Some(3.0));
    assert_eq!(trace.routing_ms, 1.5);
    assert_eq!(trace.state_version, Some(4));
}

#[test]
fn logs_decode_with_their_request_and_skip_records_without_ids() {
    let log = |id: Option<&str>, request: Option<&str>| LogRecord {
        time_unix_nano: 2_000_000_000,
        severity_text: "WARN".into(),
        body: Some(AnyValue {
            value: Some(Value::StringValue("careful".into())),
        }),
        attributes: id
            .map(|id| string_attribute(LOG_ID, id))
            .into_iter()
            .chain(request.map(|request| string_attribute(REQUEST_ID, request)))
            .collect(),
        ..Default::default()
    };
    let mut resource = None;
    identity().stamp(&mut resource);
    let logs = actor_logs(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource,
            scope_logs: vec![ScopeLogs {
                log_records: vec![
                    log(Some("a"), Some("request-1")),
                    log(Some("b"), None),
                    log(None, Some("request-1")),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    assert_eq!(
        logs.iter()
            .map(|log| (log.log_id.as_str(), log.request_id.as_deref()))
            .collect::<Vec<_>>(),
        [("a", Some("request-1")), ("b", None)]
    );
    assert_eq!(logs[0].timestamp_ms, 2000);
    assert_eq!(logs[0].level, "warn");
    assert_eq!(logs[0].message, "careful");
    assert_eq!(logs[0].project_id, "default");
}

fn identity() -> Identity {
    Identity {
        organization_id: Some("org".into()),
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
        host_id: "host".into(),
        session_id: "session".into(),
    }
}

fn number_attribute(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
        ..Default::default()
    }
}

fn int_attribute(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
        ..Default::default()
    }
}
