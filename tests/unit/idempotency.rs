use super::*;
use serde_json::json;

fn identity(key: &str, subject: &str, args: Vec<Value>) -> InvocationIdentity {
    InvocationIdentity::new(key, subject, "increment", &args).unwrap()
}

#[test]
fn receipts_replay_older_results_and_bind_caller_and_payload() {
    let first = identity("1000.first", "alice", vec![json!(1)]);
    let second = identity("1001.second", "alice", vec![json!(2)]);
    let mut receipts = InvocationReceipts::default();
    assert!(receipts.admit(&first, 1001).is_none());
    receipts.complete(&first, ReceiptOutcome::Completed(json!(1)), 1001);
    assert!(receipts.admit(&second, 1001).is_none());
    receipts.complete(&second, ReceiptOutcome::Completed(json!(3)), 1001);
    assert_eq!(receipts.admit(&first, 1002), Some(completed(json!(1))));
    assert!(
        receipts
            .admit(&identity("1000.first", "bob", vec![json!(1)]), 1002)
            .is_none()
    );
    assert_eq!(
        failure_code(receipts.admit(&identity("1000.first", "alice", vec![json!(9)]), 1002)),
        "idempotency_conflict"
    );
}

#[test]
fn recovered_pending_receipts_never_reexecute_partial_interleaved_work() {
    let first = identity("1000.first", "alice", vec![]);
    let mut receipts = InvocationReceipts::default();
    assert!(receipts.admit(&first, 1000).is_none());
    let mut recovered: InvocationReceipts =
        serde_json::from_slice(&serde_json::to_vec(&receipts).unwrap()).unwrap();
    assert_eq!(
        failure_code(recovered.admit(&first, 1001)),
        "outcome_unknown"
    );
}

#[test]
fn expiry_and_capacity_eviction_fence_old_keys_instead_of_executing_again() {
    let first = identity("1000.first", "alice", vec![]);
    let mut receipts = InvocationReceipts::default();
    receipts.admit(&first, 1000);
    receipts.complete(&first, ReceiptOutcome::Completed(json!(1)), 1000);
    for i in 1..=MAX_RECEIPTS {
        let next = identity(&format!("{}.next", 1000 + i), "alice", vec![]);
        receipts.admit(&next, 2000);
        receipts.complete(&next, ReceiptOutcome::Completed(json!(i)), 2000);
    }
    assert_eq!(
        failure_code(receipts.admit(&first, 2000)),
        "idempotency_expired"
    );
    let next = identity("2000.last", "alice", vec![]);
    assert_eq!(
        failure_code(receipts.admit(&next, 2000 + RETENTION_MS)),
        "idempotency_expired"
    );
    assert_eq!(
        failure_code(receipts.admit(&identity("9000.future", "alice", vec![]), 2000)),
        "idempotency_expired"
    );
}

#[test]
fn large_results_are_not_reexecuted_when_the_receipt_cannot_retain_the_payload() {
    let first = identity("1000.first", "alice", vec![]);
    let mut receipts = InvocationReceipts::default();
    receipts.admit(&first, 1000);
    receipts.complete(
        &first,
        ReceiptOutcome::Completed(json!("x".repeat(MAX_RESULT_BYTES + 1))),
        1000,
    );
    assert_eq!(
        failure_code(receipts.admit(&first, 1001)),
        "idempotency_result_unavailable"
    );
}

#[test]
fn total_receipt_bytes_are_bounded_and_eviction_fences_survive_serialization() {
    let first = identity("1000.first", "alice", vec![]);
    let mut receipts = InvocationReceipts::default();
    receipts.admit(&first, 1000);
    receipts.complete(
        &first,
        ReceiptOutcome::Completed(json!("x".repeat(60_000))),
        1000,
    );
    for time in 1001..1030 {
        let next = identity(&format!("{time}.next"), "alice", vec![]);
        receipts.admit(&next, 1030);
        receipts.complete(
            &next,
            ReceiptOutcome::Completed(json!("x".repeat(60_000))),
            1030,
        );
    }
    assert!(encoded_size(&receipts) <= MAX_RECEIPT_BYTES);
    let mut recovered: InvocationReceipts =
        serde_json::from_slice(&serde_json::to_vec(&receipts).unwrap()).unwrap();
    recovered.validate().unwrap();
    assert_eq!(
        failure_code(recovered.admit(&first, 1030)),
        "idempotency_expired"
    );
}

#[test]
fn fingerprints_ignore_object_key_order_and_keys_have_a_strict_wire_format() {
    let left = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
    let right = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
    assert_eq!(
        identity("1000.key", "alice", vec![left]),
        identity("1000.key", "alice", vec![right])
    );
    for key in ["", "1", "01.a", "-1.a", "1.", "1.a b", "1.a.b"] {
        assert!(InvocationIdentity::new(key, "alice", "increment", &[]).is_err());
    }
}

fn failure_code(result: Option<ActorExecutionResult>) -> String {
    match result.unwrap() {
        ActorExecutionResult::Failed { failure } => failure.code,
        result => panic!("expected failure, got {result:?}"),
    }
}
