use super::*;

#[tokio::test]
async fn admitted_requests_prevent_idle_host_shutdown() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (requests, mut activity) = tokio::sync::watch::channel(ActorActivity {
        active: 1,
        ..Default::default()
    });
    let (_stopped_sender, mut actor_stopped) = tokio::sync::watch::channel(false);
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        (&mut activity, &mut actor_stopped),
        Duration::from_millis(100),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), stopped.as_mut())
            .await
            .is_err()
    );
    requests.send_modify(|activity| {
        activity.active = 0;
        activity.last_active = tokio::time::Instant::now();
    });
    tokio::time::timeout(Duration::from_secs(1), stopped).await??;
    Ok(())
}

#[tokio::test]
async fn failed_activation_stops_a_busy_host() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (_activity_sender, mut activity) = tokio::sync::watch::channel(ActorActivity {
        active: 1,
        ..Default::default()
    });
    let (stopped_sender, mut actor_stopped) = tokio::sync::watch::channel(false);
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        (&mut activity, &mut actor_stopped),
        Duration::from_millis(10),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stopped.as_mut())
            .await
            .is_err()
    );
    stopped_sender.send_replace(true);
    let error = tokio::time::timeout(Duration::from_secs(1), stopped.as_mut())
        .await?
        .unwrap_err();
    assert!(error.to_string().contains("activation stopped"));
    drop(stopped);
    javascript.kill().await?;
    Ok(())
}
