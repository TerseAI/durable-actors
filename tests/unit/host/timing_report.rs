use super::*;

#[test]
fn recorded_milestones_are_reported_once_as_server_timing() {
    let reports = TimingReports::new(8);
    reports.record(
        "request-1",
        [
            ("queue_admitted", Some(1.25)),
            ("execution_completed", Some(4.0)),
            ("unused", None),
        ],
    );
    reports.describe("request-1", "durability_proof", "replicas");
    assert_eq!(
        reports.take("request-1").as_deref(),
        Some(
            "queue_admitted;dur=1.25, execution_completed;dur=4.00, durability_proof;desc=replicas"
        )
    );
    assert!(reports.take("request-1").is_none());
}

#[test]
fn reports_are_bounded_by_evicting_the_oldest_request() {
    let reports = TimingReports::new(2);
    for request in ["one", "two", "three"] {
        reports.record(request, [("completed", Some(1.0))]);
    }
    assert!(reports.take("one").is_none());
    assert!(reports.take("three").is_some());
}
