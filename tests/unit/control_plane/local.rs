use super::*;

#[test]
fn startup_message_has_clear_hierarchy_and_next_step() {
    let message = local_ready_message(
        "http://127.0.0.1:7100",
        Path::new("/projects/chat/.durable-actors"),
        "local",
    );

    assert!(message.starts_with("durable actors / local\n\n  Ready"));
    assert!(message.contains("Connect your application"));
    assert!(message.contains("DURABLE_ACTORS_PROJECT_ID=local"));
    assert!(message.contains("DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100"));
    assert!(message.contains("DURABLE_ACTORS_SECRET='generated-secret'"));
    assert!(
        message.find("1. Configure your client").unwrap()
            < message.find("2. Generate your client").unwrap()
    );
    assert!(!message.contains("cat --"));
    assert!(!message.contains("API key"));
    assert!(!message.contains("export "));
}

#[test]
fn startup_command_uses_the_configured_project_and_port() {
    let message = local_ready_message(
        "http://127.0.0.1:8123",
        Path::new("/projects/chat/.durable-actors"),
        "my-project",
    );
    assert!(message.contains("DURABLE_ACTORS_PROJECT_ID=my-project"));
    assert!(message.contains("DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:8123"));
    assert!(message.contains("     durable-actors generate --remote\n"));
}

#[test]
fn credentials_print_a_dotenv_shared_secret() {
    let message = local_credentials_instructions("Sam's-$secret #1", LocalReadyStyles::default());
    assert!(message.contains("DURABLE_ACTORS_SECRET=\"Sam's-$secret #1\""));
    assert!(message.contains("Update the secret after restarting this actor server."));
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
