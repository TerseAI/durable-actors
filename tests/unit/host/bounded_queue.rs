use super::*;

struct StalledExecutor {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    executed: AtomicUsize,
}

#[async_trait]
impl ActorExecutor for StalledExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        _: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        self.executed.fetch_add(1, Ordering::SeqCst);
        if invocation.request_id == "stall" {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(ActorMethodOutcome::Completed {
            result: Value::Null,
            effects: vec![],
        })
    }
}

async fn stalled_host() -> (Arc<ActorHost>, Arc<StalledExecutor>) {
    let executor = Arc::new(StalledExecutor {
        started: Default::default(),
        release: Default::default(),
        executed: AtomicUsize::new(0),
    });
    let host = Arc::new(create_host(
        HostEndpoint {
            id: super::super::super::HostId::new("burst"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
        replication().await,
    ));
    (host, executor)
}

#[tokio::test]
async fn stalled_actor_burst_is_bounded_and_drains() -> Result<()> {
    let (host, executor) = stalled_host().await;
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "stall").await });
    tokio::time::timeout(Duration::from_secs(5), executor.started.notified()).await?;
    let mut calls = Vec::new();
    for n in 0..1023 {
        let caller = host.clone();
        calls.push(tokio::spawn(async move {
            invoke(&caller, &format!("burst-{n}")).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting = host.status().borrow().waiting.len();
    let rejected = calls.iter().filter(|call| call.is_finished()).count();
    executor.release.notify_one();
    assert!(matches!(
        first.await??,
        ActorExecutionResult::Completed { .. }
    ));
    let mut completed = 1;
    for call in calls {
        match tokio::time::timeout(Duration::from_secs(10), call).await??? {
            ActorExecutionResult::Completed { .. } => completed += 1,
            ActorExecutionResult::Failed { failure } => {
                assert_eq!(failure.code, "actor_overloaded")
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
    host.drain(Duration::from_secs(5)).await?;
    eprintln!(
        "burst: attempted=1024 admitted={completed} rejected={rejected} stalled_inventory={waiting} executed={} drained_inventory={}",
        executor.executed.load(Ordering::SeqCst),
        host.status().borrow().waiting.len()
    );
    assert!(waiting <= 127, "stalled inventory grew to {waiting}");
    assert_eq!(completed, 128);
    assert_eq!(rejected, 896);
    assert_eq!(executor.executed.load(Ordering::SeqCst), completed);
    assert!(host.status().borrow().waiting.is_empty());
    Ok(())
}

#[tokio::test]
async fn cancelling_queued_requests_releases_inventory_before_actor_resumes() -> Result<()> {
    let (host, executor) = stalled_host().await;
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "stall").await });
    tokio::time::timeout(Duration::from_secs(5), executor.started.notified()).await?;
    let mut queued = Vec::new();
    for n in 0..127 {
        let caller = host.clone();
        queued.push(tokio::spawn(async move {
            invoke(&caller, &format!("cancel-{n}")).await
        }));
    }
    let mut status = host.status();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| s.waiting.len() == 127),
    )
    .await??;
    for call in queued {
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        status.wait_for(|s| s.waiting.is_empty()),
    )
    .await??;
    assert_eq!(executor.executed.load(Ordering::SeqCst), 1);
    let caller = host.clone();
    let replacement = tokio::spawn(async move { invoke(&caller, "replacement").await });
    tokio::time::timeout(
        Duration::from_secs(2),
        status.wait_for(|s| s.waiting.len() == 1),
    )
    .await??;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    executor.release.notify_one();
    assert!(matches!(
        replacement.await??,
        ActorExecutionResult::Completed { .. }
    ));
    host.drain(Duration::from_secs(5)).await?;
    assert_eq!(executor.executed.load(Ordering::SeqCst), 2);
    Ok(())
}
