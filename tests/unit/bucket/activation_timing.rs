use super::*;
use crate::{state_log::StateSnapshot, state_transport::SnapshotWriter};
use tracing::instrument::WithSubscriber;

#[tokio::test]
async fn activation_logs_correlated_storage_phases_for_new_and_restored_actors() -> Result<()> {
    let f = Fixture::new()?;
    let output = tempfile::NamedTempFile::new()?;
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_writer(output.reopen()?)
        .finish();
    async {
        let first = f
            .runtime
            .register_activation(&f.actor, &request("first"), "us-east", true)
            .await?;
        let plan = f
            .runtime
            .prepare_actor_write(&f.actor, &first.placement.lease, 1, 1)
            .await?;
        let bytes = StateSnapshot::new(
            1,
            1,
            "private-request".into(),
            serde_json::json!({"secret": "private-state"}),
            serde_json::json!("private-result"),
        )?
        .encode()?;
        f.runtime.write_snapshot(&plan, bytes).await?;
        f.clock.0.store(11_000, Ordering::SeqCst);
        f.runtime
            .register_activation(&f.actor, &request("next"), "us-east", false)
            .await?;
        anyhow::Ok(())
    }
    .with_subscriber(subscriber)
    .await?;
    let events = phase_events(&output)?;
    for session in ["first", "next"] {
        let phases: Vec<_> = events
            .iter()
            .filter(|event| event["span"]["session_id"] == session)
            .collect();
        assert!(!phases.is_empty(), "missing activation phases: {events:?}");
        for event in &phases {
            assert_eq!(event["span"]["project_id"], "default");
            assert_eq!(event["span"]["actor_name"], "Counter");
            assert_eq!(event["span"]["actor_id"], "one");
            assert_eq!(event["span"]["host_id"], format!("host-{session}"));
            assert_eq!(event["outcome"], "completed");
            assert!(event["duration_ms"].as_f64().unwrap() >= 0.0);
        }
        assert!(
            phases
                .iter()
                .any(|event| event["phase"] == "ownership_write")
        );
        assert_eq!(phases.last().unwrap()["phase"], "activation_total");
    }
    for phase in [
        "ownership_read",
        "session_recovery",
        "recovery_session_read",
        "recovery_session_claim",
        "latest_snapshot",
        "snapshot_list",
        "snapshot_read",
        "replica_members_read",
    ] {
        assert!(
            events
                .iter()
                .any(|event| event["span"]["session_id"] == "next" && event["phase"] == phase),
            "missing {phase}: {events:?}"
        );
    }
    let logs = std::fs::read_to_string(output.path())?;
    for payload in ["private-state", "private-result", "private-request"] {
        assert!(!logs.contains(payload));
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_activation_records_waiting_write_and_preserves_correlation() -> Result<()> {
    let f = Fixture::new()?;
    let output = tempfile::NamedTempFile::new()?;
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_writer(output.reopen()?)
        .finish();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), resume));
    let request = request("cancelled");
    let mut activation = Box::pin(
        f.runtime
            .register_activation(&f.actor, &request, "us-east", true)
            .with_subscriber(subscriber),
    );
    tokio::select! {
        result = &mut activation => panic!("write should wait: {}", result.is_ok()),
        result = entered.acquire() => result?.forget(),
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(activation);
    let events = phase_events(&output)?;
    for phase in ["ownership_write", "activation_total"] {
        let event = events
            .iter()
            .find(|event| event["phase"] == phase)
            .expect("cancelled phase");
        assert_eq!(event["outcome"], "cancelled");
        assert_eq!(event["span"]["session_id"], "cancelled");
        assert!(event["duration_ms"].as_f64().unwrap() >= 10.0);
    }
    Ok(())
}

#[tokio::test]
async fn activation_failure_logs_the_failed_phase_without_error_payloads() -> Result<()> {
    let f = Fixture::new()?;
    let output = tempfile::NamedTempFile::new()?;
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_writer(output.reopen()?)
        .finish();
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    resume.close();
    *f.bucket.delay_write.lock().unwrap() =
        Some((Arc::new(tokio::sync::Semaphore::new(0)), resume));
    assert!(
        f.runtime
            .register_activation(&f.actor, &request("first"), "us-east", true)
            .with_subscriber(subscriber)
            .await
            .is_err()
    );
    let events = phase_events(&output)?;
    for phase in ["ownership_write", "activation_total"] {
        let event = events
            .iter()
            .find(|event| event["phase"] == phase)
            .expect("failed phase");
        assert_eq!(event["outcome"], "failed");
        assert!(event.get("error").is_none());
    }
    Ok(())
}

fn phase_events(output: &tempfile::NamedTempFile) -> Result<Vec<serde_json::Value>> {
    Ok(std::fs::read_to_string(output.path())?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|event| event["event"] == "actor_activation_phase")
        .collect())
}
