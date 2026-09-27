use super::*;
use crate::idempotency::InvocationIdentity;

#[tokio::test]
async fn durable_receipts_replay_older_requests_after_host_recovery() -> Result<()> {
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let writes = Arc::new(FakeStateTransport::default());
    let host = test_host(executor.clone(), writes.clone(), None);
    let first = invocation("first", "alice", vec![]);
    let second = invocation("second", "alice", vec![]);
    assert_eq!(host.invoke_actor(first.clone(), 1).await?, completed(1));
    assert_eq!(host.invoke_actor(second, 1).await?, completed(2));
    let recovered = test_host(
        executor.clone(),
        writes.clone(),
        writes.writes.lock().unwrap().last().cloned(),
    );
    assert_eq!(recovered.invoke_actor(first, 2).await?, completed(1));
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);
    assert_eq!(writes.writes.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn durable_receipts_isolate_callers_and_reject_key_payload_conflicts() -> Result<()> {
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let host = test_host(
        executor.clone(),
        Arc::new(FakeStateTransport::default()),
        None,
    );
    let first = invocation("first", "alice", vec![json!(1)]);
    assert_eq!(host.invoke_actor(first.clone(), 1).await?, completed(1));
    let conflict = invocation("first", "alice", vec![json!(2)]);
    assert!(
        matches!(host.invoke_actor(conflict, 1).await?, ActorExecutionResult::Failed { failure } if failure.code == "idempotency_conflict")
    );
    assert_eq!(
        host.invoke_actor(invocation("first", "bob", vec![json!(1)]), 1)
            .await?,
        completed(2)
    );
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn unchanged_state_results_are_durable_and_replayed_without_execution() -> Result<()> {
    struct Reader(AtomicU64);
    #[async_trait]
    impl ActorExecutor for Reader {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            Ok(ActorMethodOutcome::Completed {
                result: json!(self.0.fetch_add(1, Ordering::Relaxed)),
                state: json!({}),
                effects: vec![],
            })
        }
    }
    let executor = Arc::new(Reader(AtomicU64::new(0)));
    let writes = Arc::new(FakeStateTransport::default());
    let host = test_host(executor.clone(), writes.clone(), None);
    host.invoke_actor(invocation("one", "alice", vec![]), 1)
        .await?;
    let second = invocation("two", "alice", vec![]);
    assert_eq!(host.invoke_actor(second.clone(), 1).await?, completed(1));
    let recovered = test_host(
        executor.clone(),
        writes.clone(),
        writes.writes.lock().unwrap().last().cloned(),
    );
    assert_eq!(recovered.invoke_actor(second, 2).await?, completed(1));
    assert_eq!(executor.0.load(Ordering::Relaxed), 2);
    assert_eq!(writes.writes.lock().unwrap().len(), 2);
    Ok(())
}

fn invocation(key: &str, subject: &str, args: Vec<Value>) -> ActorInvocation {
    // A fixed timestamp per test run keeps explicit retry identities stable.
    static START: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    let now = START.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    });
    ActorInvocation {
        request_id: key.into(),
        idempotency: Some(
            InvocationIdentity::new(&format!("{now}.{key}"), subject, "increment", &args).unwrap(),
        ),
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        method: "increment".into(),
        args,
    }
}

#[tokio::test]
async fn concurrent_duplicates_join_one_reentrant_execution() -> Result<()> {
    struct Executor {
        calls: AtomicU64,
        admission: watch::Sender<()>,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.admission.send_replace(());
            self.started.notify_one();
            self.release.notified().await;
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence: 1,
                    result: json!(1),
                    state: json!({"count":1}),
                    effects: vec![],
                },
            ))
        }
    }
    let executor = Arc::new(Executor {
        calls: AtomicU64::new(0),
        admission: watch::channel(()).0,
        started: Default::default(),
        release: Default::default(),
    });
    let host = Arc::new(test_host(
        executor.clone(),
        Arc::new(FakeStateTransport::default()),
        None,
    ));
    let request = invocation("first", "alice", vec![]);
    let first = tokio::spawn({
        let host = host.clone();
        let request = request.clone();
        async move { host.invoke_actor(request, 1).await }
    });
    executor.started.notified().await;
    let wrong_epoch = tokio::time::timeout(
        Duration::from_millis(30),
        host.invoke_actor(request.clone(), 2),
    )
    .await;
    let mut duplicate = tokio::spawn({
        let host = host.clone();
        async move {
            host.invoke_actor(
                ActorInvocation {
                    request_id: "retry".into(),
                    ..request
                },
                1,
            )
            .await
        }
    });
    let waiting = tokio::time::timeout(Duration::from_millis(30), &mut duplicate).await;
    executor.release.notify_one();
    assert!(
        matches!(wrong_epoch, Ok(Ok(ActorExecutionResult::Reroute))),
        "a duplicate cannot join another ownership epoch"
    );
    assert!(
        waiting.is_err(),
        "a duplicate should await the original execution"
    );
    assert_eq!(first.await??, completed(1));
    assert_eq!(duplicate.await??, completed(1));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn committed_receipt_survives_a_lost_storage_acknowledgement() -> Result<()> {
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let writes = Arc::new(FakeStateTransport::default());
    writes.failures.store(1, Ordering::SeqCst);
    let host = test_host(executor.clone(), writes.clone(), None);
    let request = invocation("lost-ack", "alice", vec![]);
    assert!(
        matches!(host.invoke_actor(request.clone(), 1).await?, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
    );
    let snapshot = writes.writes.lock().unwrap().last().cloned();
    let recovered = test_host(executor.clone(), writes, snapshot);
    assert_eq!(recovered.invoke_actor(request, 2).await?, completed(1));
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn recovery_refuses_to_reexecute_partially_committed_reentrant_work() -> Result<()> {
    struct Executor {
        calls: AtomicU64,
        admission: watch::Sender<()>,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            self.admission.send_replace(());
            if first {
                self.started.notify_one();
                self.release.notified().await;
            }
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence: if first { 2 } else { 1 },
                    result: json!(2),
                    state: json!({"count":2}),
                    effects: vec![],
                },
            ))
        }
    }
    let executor = Arc::new(Executor {
        calls: AtomicU64::new(0),
        admission: watch::channel(()).0,
        started: Default::default(),
        release: Default::default(),
    });
    let writes = Arc::new(FakeStateTransport::default());
    let host = Arc::new(test_host(executor.clone(), writes.clone(), None));
    let pending = invocation("pending", "alice", vec![]);
    let running = tokio::spawn({
        let host = host.clone();
        let pending = pending.clone();
        async move { host.invoke_actor(pending, 1).await }
    });
    executor.started.notified().await;
    let second = invocation("committed", "alice", vec![]);
    assert_eq!(host.invoke_actor(second.clone(), 1).await?, completed(2));
    let snapshot = writes.writes.lock().unwrap().last().cloned();
    let replacement = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let recovered = test_host(replacement.clone(), writes, snapshot);
    assert!(
        matches!(recovered.invoke_actor(pending, 2).await?, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
    );
    assert_eq!(recovered.invoke_actor(second, 2).await?, completed(2));
    assert_eq!(replacement.invocations.load(Ordering::Relaxed), 0);
    executor.release.notify_one();
    running.await??;
    Ok(())
}

#[tokio::test]
async fn a_failed_first_invocation_does_not_invent_an_initial_actor_state() -> Result<()> {
    struct Executor;
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            assert!(
                state.is_none(),
                "a failure must not replace constructor defaults with an empty object"
            );
            anyhow::bail!("customer process disconnected")
        }
    }
    let writes = Arc::new(FakeStateTransport::default());
    let host = test_host(Arc::new(Executor), writes.clone(), None);
    host.invoke_actor(invocation("fail", "alice", vec![]), 1)
        .await?;
    assert!(writes.writes.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn failure_receipts_survive_recovery_with_existing_actor_state() -> Result<()> {
    struct Executor(AtomicU64);
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            _: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                code: "actor_method_failed".into(),
                message: "declined".into(),
            }))
        }
    }
    let executor = Arc::new(Executor(AtomicU64::new(0)));
    let writes = Arc::new(FakeStateTransport::default());
    let initial =
        StateSnapshot::new(1, 1, "initial".into(), json!({"count":7}), Value::Null)?.encode()?;
    let host = test_host(executor.clone(), writes.clone(), Some(initial));
    let request = invocation("declined", "alice", vec![]);
    let failure = host.invoke_actor(request.clone(), 1).await?;
    assert!(
        matches!(&failure, ActorExecutionResult::Failed { failure } if failure.code == "actor_error")
    );
    let snapshot = writes.writes.lock().unwrap().last().cloned();
    let recovered = test_host(executor.clone(), writes, snapshot);
    assert_eq!(recovered.invoke_actor(request, 2).await?, failure);
    assert_eq!(executor.0.load(Ordering::Relaxed), 1);
    Ok(())
}

fn test_host(
    executor: Arc<dyn ActorExecutor>,
    writes: Arc<FakeStateTransport>,
    snapshot: Option<Vec<u8>>,
) -> ActorHost {
    let initial_state = snapshot.map(|bytes| {
        (
            StateSnapshot::decode(&bytes).unwrap().state_version,
            bytes.into(),
        )
    });
    ActorHost::new(
        HostEndpoint {
            id: crate::host::HostId::new("host"),
            route: "http://host.invalid".into(),
        },
        executor,
        Arc::new(FakeAuthority {
            initial_state,
            ..Default::default()
        }),
        writes,
        Arc::new(EmptySocketPublisher),
    )
}
