use super::*;
use tokio::{sync::Semaphore, task::JoinSet};

const LIMIT: usize = 128;

struct StalledExecutor {
    started: AtomicUsize,
    finished: AtomicUsize,
    release: Semaphore,
    admission: Option<watch::Sender<()>>,
}

#[async_trait]
impl ActorExecutor for StalledExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }

    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        self.admission.as_ref().map(watch::Sender::subscribe)
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        _: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        self.started.fetch_add(1, Ordering::SeqCst);
        if let Some(admission) = &self.admission {
            admission.send_replace(());
            self.release.acquire().await?.forget();
        } else if invocation.request_id == "stalled" {
            self.release.acquire().await?.forget();
        }
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(if self.admission.is_some() {
            ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "actor_method_failed".into(),
                message: "controlled executor finished without effects".into(),
            })
        } else {
            ActorMethodOutcome::Completed {
                result: Value::Null,
                effects: vec![],
            }
        })
    }
}

async fn stalled_host(interleaved: bool) -> (Arc<ActorHost>, Arc<StalledExecutor>) {
    stalled_host_for(interleaved, counter()).await
}

async fn stalled_host_for(
    interleaved: bool,
    actor: ActorKey,
) -> (Arc<ActorHost>, Arc<StalledExecutor>) {
    let executor = Arc::new(StalledExecutor {
        started: AtomicUsize::new(0),
        finished: AtomicUsize::new(0),
        release: Semaphore::new(0),
        admission: interleaved.then(|| watch::channel(()).0),
    });
    let endpoint = HostEndpoint {
        id: super::super::super::HostId::new("overload-host"),
        route: "http://host.invalid".into(),
    };
    let database = HostDatabase::new(
        endpoint.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        replication().await,
    );
    let host = Arc::new(ActorHost::new(
        endpoint,
        actor,
        executor.clone(),
        database,
        Arc::new(EmptySocketPublisher),
    ));
    (host, executor)
}

async fn request(host: &ActorHost, id: &str) -> Result<ActorExecutionResult> {
    host.invoke_actor(
        ActorInvocation {
            actor: host.actor().clone(),
            request_id: id.into(),
            method: "increment".into(),
            args: vec![json!(format!("{id}:{}", "x".repeat(32 * 1024)))],
        },
        1,
        HostState::Warm,
        0.0,
    )
    .await
    .0
}

async fn waiting(host: &ActorHost, count: usize) -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(2),
        host.status()
            .wait_for(|status| status.waiting.len() == count),
    )
    .await??;
    Ok(())
}

async fn started(executor: &StalledExecutor, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while executor.started.load(Ordering::SeqCst) != count {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

fn rss_kib() -> Option<usize> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

#[tokio::test]
async fn stalled_actor_burst_is_bounded_cancellation_frees_waiting_and_resume_drains() -> Result<()>
{
    let (host, executor) = stalled_host(false).await;
    let mut accepted = JoinSet::new();
    let caller = host.clone();
    accepted.spawn(async move { request(&caller, "stalled").await });
    started(&executor, 1).await?;
    let mut abort = Vec::new();
    for index in 1..LIMIT {
        let caller = host.clone();
        abort.push(
            accepted.spawn(async move { request(&caller, &format!("queued-{index}")).await }),
        );
    }
    waiting(&host, LIMIT - 1).await?;
    let mut other_actor = counter();
    other_actor.project_id = "another-project".into();
    let (other_host, other_executor) = stalled_host_for(false, other_actor).await;
    let independent = request(&other_host, "independent-capacity").await?;
    assert!(matches!(
        independent,
        ActorExecutionResult::Completed { .. }
    ));
    assert_eq!(other_executor.started.load(Ordering::SeqCst), 1);
    other_host.drain(Duration::from_secs(2)).await?;
    let rss_before = rss_kib();
    let mut rss_samples = Vec::new();
    let mut maximum_waiting = 0;
    for index in 0..10_000 {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            request(&host, &format!("overload-{index}")),
        )
        .await??;
        assert!(
            matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "actor_overloaded" && failure.message.contains("before execution"))
        );
        maximum_waiting = maximum_waiting.max(host.status().borrow().waiting.len());
        if index % 1_000 == 999 {
            rss_samples.push(rss_kib());
        }
    }
    assert_eq!(executor.started.load(Ordering::SeqCst), 1);
    assert_eq!(maximum_waiting, LIMIT - 1);
    for handle in abort.into_iter().take(64) {
        handle.abort();
    }
    waiting(&host, LIMIT - 1 - 64).await?;
    let after_cancel = host.status().borrow().waiting.len();
    for index in 0..64 {
        let caller = host.clone();
        accepted.spawn(async move { request(&caller, &format!("replacement-{index}")).await });
    }
    waiting(&host, LIMIT - 1).await?;
    executor.release.add_permits(1);
    let mut completed = 0;
    let mut cancelled = 0;
    while let Some(result) = accepted.join_next().await {
        match result {
            Ok(outcome) => {
                assert!(matches!(outcome?, ActorExecutionResult::Completed { .. }));
                completed += 1;
            }
            Err(error) if error.is_cancelled() => cancelled += 1,
            Err(error) => return Err(error.into()),
        }
    }
    host.drain(Duration::from_secs(2)).await?;
    assert_eq!(cancelled, 64);
    assert_eq!(completed, LIMIT);
    assert_eq!(executor.started.load(Ordering::SeqCst), LIMIT);
    assert_eq!(executor.finished.load(Ordering::SeqCst), LIMIT);
    assert!(host.status().borrow().waiting.is_empty());
    println!(
        "bounded_actor_load limit={LIMIT} attempted={} accepted={} rejected=10000 cancelled={cancelled} executed={completed} max_waiting={maximum_waiting} after_cancel_waiting={after_cancel} after_drain_waiting=0 payload_bytes=32768 rss_before_kib={rss_before:?} rss_samples_kib={rss_samples:?}",
        LIMIT + 64 + 10_000,
        LIMIT + 64
    );
    Ok(())
}

#[tokio::test]
async fn interleaved_running_requests_and_cancelled_callers_still_hold_capacity() -> Result<()> {
    let (host, executor) = stalled_host(true).await;
    let mut accepted = JoinSet::new();
    for index in 0..LIMIT {
        let caller = host.clone();
        accepted.spawn(async move { request(&caller, &format!("running-{index}")).await });
        started(&executor, index + 1).await?;
    }
    accepted.abort_all();
    while accepted.join_next().await.is_some() {}
    let result = tokio::time::timeout(Duration::from_secs(1), request(&host, "overflow")).await??;
    assert!(
        matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "actor_overloaded")
    );
    assert_eq!(executor.started.load(Ordering::SeqCst), LIMIT);
    let mut status = host.status();
    let (drained, resumed) = tokio::join!(host.drain(Duration::from_secs(2)), async {
        tokio::time::timeout(
            Duration::from_secs(1),
            status.wait_for(|status| status.phase == ActorPhase::Draining),
        )
        .await??;
        executor.release.add_permits(LIMIT);
        Ok::<_, anyhow::Error>(())
    });
    resumed?;
    drained?;
    assert_eq!(executor.finished.load(Ordering::SeqCst), LIMIT);
    assert!(host.status().borrow().waiting.is_empty());
    Ok(())
}

#[tokio::test]
async fn mailbox_and_waiting_inventory_share_one_outstanding_limit() -> Result<()> {
    let (host, executor) = stalled_host(false).await;
    let caller = host.clone();
    let first = tokio::spawn(async move { request(&caller, "stalled").await });
    started(&executor, 1).await?;
    let mut queued = Vec::new();
    for index in 1..LIMIT {
        let caller = host.clone();
        let mut call = Box::pin(async move { request(&caller, &format!("mailbox-{index}")).await });
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        queued.push(call);
    }
    let mailbox = host.mailbox.max_capacity() - host.mailbox.capacity();
    assert_eq!(mailbox, LIMIT - 1);
    assert_eq!(host.capacity.available_permits(), 0);
    assert!(host.status().borrow().waiting.is_empty());
    assert!(
        matches!(request(&host, "overflow").await?, ActorExecutionResult::Failed { failure } if failure.code == "actor_overloaded")
    );
    waiting(&host, LIMIT - 1).await?;
    assert_eq!(host.mailbox.capacity(), host.mailbox.max_capacity());
    assert_eq!(host.capacity.available_permits(), 0);
    drop(queued);
    waiting(&host, 0).await?;
    assert_eq!(host.capacity.available_permits(), LIMIT - 1);
    assert_eq!(executor.started.load(Ordering::SeqCst), 1);
    executor.release.add_permits(1);
    first.await??;
    host.drain(Duration::from_secs(2)).await?;
    assert_eq!(host.capacity.available_permits(), LIMIT);
    println!(
        "mailbox_transfer attempted=129 admitted=128 rejected=1 max_outstanding=128 max_mailbox={mailbox} max_inventory=127 cancelled=127 executed=1 drained_mailbox=0 drained_inventory=0"
    );
    Ok(())
}
