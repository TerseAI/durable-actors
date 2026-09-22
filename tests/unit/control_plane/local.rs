use super::*;

#[test]
fn startup_message_has_clear_hierarchy_and_next_step() {
    let message = local_ready_message(
        "http://127.0.0.1:7100",
        Path::new("/projects/chat/.little-actors"),
    );

    assert_eq!(
        message,
        "little actors / local\n\n  Ready  http://127.0.0.1:7100\n  State  /projects/chat/.little-actors\n  Next   npx little-actors generate --url http://127.0.0.1:7100\n\n  State persists between restarts. Delete the state directory to start fresh."
    );
}

#[tokio::test]
async fn relative_state_directories_are_absolute_in_host_configuration() -> Result<()> {
    let cwd = std::env::current_dir()?;
    let directory = tempfile::tempdir_in(&cwd)?;
    let relative = directory.path().strip_prefix(&cwd)?;
    let options = DevOptions {
        project_id: "default".into(),
        api_key: Some("test-key".into()),
        contract: None,
        project: cwd.clone(),
        port: 0,
        data_dir: Some(relative.into()),
        entrypoint: "actors.ts".into(),
        storage: DevStorage::Local,
        ready_fd: None,
        sdk_host: None,
    };
    let state = local_storage(&options, relative, "http://localhost:7100").await?;
    let config: crate::bucket::access::HostStorageConfig =
        serde_json::from_str(&state.access.bootstrap(&state.region).await?)?;
    let BucketLocation::File {
        directory: configured,
    } = config.bucket
    else {
        panic!("expected file bucket")
    };
    assert_eq!(configured, directory.path().canonicalize()?.join("objects"));
    state
        .traces
        .record(
            "host",
            "session",
            vec![crate::request_traces::RequestTrace {
                request_id: "request".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
                kind: crate::request_traces::RequestKind::Method,
                operation: "increment".into(),
                connection_id: None,
                started_at_ms: 1,
                duration_ms: 1.0,
                queue_wait_ms: None,
                outcome: crate::request_traces::RequestOutcome::Completed,
                metadata: None,
            }],
            0,
        )
        .await?;
    let event_id = state.traces.replay(&Default::default()).await?.records[0]
        .event
        .event_id
        .clone();
    drop(state);
    let restored = local_storage(&options, relative, "http://localhost:7100").await?;
    assert_eq!(
        restored.traces.replay(&Default::default()).await?.records[0]
            .event
            .event_id,
        event_id
    );
    Ok(())
}
