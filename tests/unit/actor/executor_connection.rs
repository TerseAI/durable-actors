use super::*;

#[tokio::test]
async fn generic_executor_connects_before_code_and_hydrates_after_assignment() -> Result<()> {
    let root = tempfile::tempdir_in("/tmp")?;
    let path = root.path().join("executor.sock");
    let listener = ActorExecutorListener::bind(&path).await?;
    let peer = tokio::spawn(async move {
        let mut socket = BufReader::new(tokio::net::UnixStream::connect(path).await?);
        write_json_line(&mut socket, &json!({"type":"warm","protocol":24})).await?;
        let load = read_json_line(&mut socket).await?;
        assert_eq!(load["entrypoint"], "/customer/actors.mjs");
        assert_eq!(load["environment"]["CUSTOMER_KEY"], "value");
        write_json_line(
            &mut socket,
            &json!({"type":"attach","protocol":24,"actor_names":["counter"]}),
        )
        .await?;
        assert_eq!(read_json_line(&mut socket).await?["type"], "attached");
        let hydrate = read_json_line(&mut socket).await?;
        assert_eq!(hydrate["command"]["type"], "hydrate");
        assert_eq!(hydrate["command"]["sqlite"]["txid"], 41);
        write_json_line(
            &mut socket,
            &json!({"type":"reply","message_id":hydrate["message_id"],"reply":{"type":"hydrated"}}),
        )
        .await?;
        anyhow::Ok(())
    });
    let warm = listener.accept_warm().await?;
    let connection = warm
        .load(
            "/customer/actors.mjs",
            &HashMap::from([("CUSTOMER_KEY".into(), "value".into())]),
        )
        .await?;
    connection
        .mark_ready(
            None,
            None,
            Arc::new(crate::litestream::Litestream::default()),
        )
        .await?;
    connection
        .executor()
        .hydrate(
            ActorKey {
                project_id: "default".into(),
                actor_name: "counter".into(),
                actor_id: "one".into(),
            },
            Some(Arc::new(ActorState {
                sqlite: crate::actor::SqliteState::position(41),
            })),
        )
        .await?;
    peer.await??;
    Ok(())
}
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Duration, timeout},
};

#[tokio::test]
async fn residency_reports_are_separate_from_invocation_cache_hints() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("residency.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let mut peer = BufReader::new(UnixStream::connect(&socket).await?);
    write_json_line(
        &mut peer,
        &json!({"type":"attach", "protocol":24, "actor_names":["Room"]}),
    )
    .await?;
    let connection = listener.accept().await?;
    connection
        .mark_ready(
            None,
            None,
            Arc::new(crate::litestream::Litestream::default()),
        )
        .await?;
    let executor = connection.executor();
    assert!(executor.resident_actors().is_none());
    let mut changes = executor
        .residency_changes()
        .expect("residency notifications");
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    for actors in [vec![actor], vec![]] {
        write_json_line(&mut peer, &json!({"type":"residency", "actors":actors})).await?;
        timeout(Duration::from_secs(1), changes.changed()).await??;
        timeout(Duration::from_secs(1), async {
            while executor.resident_actors() != Some(actors.clone()) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn connection_lookup_is_scoped_to_its_invocation_and_does_not_block_other_actors()
-> Result<()> {
    struct Source {
        requested: mpsc::UnboundedSender<ActorKey>,
        release: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait]
    impl ActorSocketSource for Source {
        async fn query(
            &self,
            actor: &ActorKey,
            query: crate::actor::SocketQuery,
        ) -> Result<crate::actor::SocketLookup> {
            self.requested.send(actor.clone())?;
            self.release.acquire().await?.forget();
            if query.count_only {
                return Ok(crate::actor::SocketLookup::Count(1));
            }
            Ok(crate::actor::SocketLookup::Connections(vec![
                ActorSocketConnection {
                    id: actor.actor_id.clone(),
                    metadata: json!({}),
                    tags: vec![],
                },
            ]))
        }
    }
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let (requested, mut requests) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    executor
        .mark_ready(
            None,
            Some(Arc::new(Source {
                requested,
                release: release.clone(),
            })),
            Arc::new(crate::litestream::Litestream::default()),
        )
        .await?;
    let peer = async {
        let mut customer = BufReader::new(customer);
        assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
        let slow = read_json_line(&mut customer).await?;
        write_json_line(&mut customer, &json!({"type":"get_connections", "message_id":slow["message_id"], "actor": {"project_id": "default", "actor_name":"forged","actor_id":"forged"}})).await?;
        let fast = read_json_line(&mut customer).await?;
        assert_eq!(fast["command"]["actor"]["actor_id"], "fast");
        write_json_line(&mut customer, &json!({"type":"reply", "message_id":fast["message_id"], "reply":{"type":"invoked", "result":42, "sqlite":{"txid":1}}})).await?;
        let loaded = read_json_line(&mut customer).await?;
        assert_eq!(loaded["type"], "socket_connections");
        assert_eq!(loaded["message_id"], slow["message_id"]);
        assert_eq!(loaded["connections"][0]["id"], "slow");
        write_json_line(&mut customer, &json!({"type":"reply", "message_id":slow["message_id"], "reply":{"type":"invoked", "result":1, "sqlite":{"txid":1}}})).await?;
        anyhow::Ok(())
    };
    let invoke = |id: &str| {
        executor.invoke(
            ActorMethodInvocation {
                request_id: id.into(),
                actor: ActorKey {
                    project_id: "default".into(),
                    actor_name: "counter".into(),
                    actor_id: id.into(),
                },
                method: "run".into(),
                args: vec![],
            },
            None,
        )
    };
    let callers = async {
        let fast = async {
            let actor = requests.recv().await.context("no lookup")?;
            assert_eq!(actor.actor_name, "counter");
            assert_eq!(actor.actor_id, "slow");
            assert!(
                matches!(invoke("fast").await?, ActorMethodOutcome::Completed { result, .. } if result == json!(42))
            );
            release.add_permits(1);
            anyhow::Ok(())
        };
        let (slow, ()) = tokio::try_join!(invoke("slow"), fast)?;
        assert!(matches!(slow, ActorMethodOutcome::Completed { result, .. } if result == json!(1)));
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(2), async {
        tokio::try_join!(peer, callers)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

#[tokio::test]
async fn admission_is_a_single_permission_from_an_active_invocation() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let mut admission = executor.invocation_admission().unwrap();
    assert!(!admission.has_changed()?);
    let caller = tokio::spawn(async move {
        executor
            .invoke(
                ActorMethodInvocation {
                    request_id: "request".into(),
                    actor: ActorKey {
                        project_id: "default".into(),
                        actor_name: "counter".into(),
                        actor_id: "one".into(),
                    },
                    method: "hold".into(),
                    args: vec![],
                },
                None,
            )
            .await
    });
    let mut customer = BufReader::new(customer);
    let command = timeout(Duration::from_secs(1), read_json_line(&mut customer)).await??;
    assert!(!admission.has_changed()?);
    let permission = json!({"type": "ready_for_invocation", "message_id": command["message_id"]});
    write_json_line(&mut customer, &permission).await?;
    timeout(Duration::from_secs(1), admission.changed()).await??;
    write_json_line(&mut customer, &permission).await?;
    let error = timeout(Duration::from_secs(1), running)
        .await??
        .unwrap_err();
    assert!(error.to_string().contains("already granted"));
    assert!(caller.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn admission_without_an_active_invocation_is_rejected() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (_executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let mut customer = BufReader::new(customer);
    write_json_line(
        &mut customer,
        &json!({"type":"ready_for_invocation", "message_id": 99}),
    )
    .await?;
    let error = timeout(Duration::from_secs(1), running)
        .await??
        .unwrap_err();
    assert!(error.to_string().contains("no active call"));
    Ok(())
}

#[tokio::test]
async fn multiplexes_out_of_order_replies_before_peer_disconnect() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let peer = tokio::spawn(async move {
        let mut customer = BufReader::new(customer);
        let first = read_json_line(&mut customer).await?;
        let second = read_json_line(&mut customer).await?;
        for command in [second, first] {
            write_json_line(&mut customer, &json!({
                    "type": "reply", "message_id": command["message_id"],
                    "reply": {"type": "invoked", "result": command["command"]["request_id"], "sqlite": {"txid": 1}}
                })).await?;
        }
        anyhow::Ok(())
    });
    let invoke = |id: &str| {
        executor.invoke(
            ActorMethodInvocation {
                request_id: id.into(),
                actor: ActorKey {
                    project_id: "default".into(),
                    actor_name: "counter".into(),
                    actor_id: id.into(),
                },
                method: "get".into(),
                args: Vec::new(),
            },
            None,
        )
    };
    let replies = timeout(Duration::from_secs(2), async {
        tokio::try_join!(invoke("first"), invoke("second"))
    })
    .await?;
    peer.await??;
    let _ = running.await;
    let (first, second) = replies?;
    assert!(
        matches!(first, ActorMethodOutcome::Completed { result, .. } if result == json!("first"))
    );
    assert!(
        matches!(second, ActorMethodOutcome::Completed { result, .. } if result == json!("second"))
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_does_not_wait_for_a_peer_that_stopped_reading() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("executor.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let customer = tokio::spawn(async move {
        let stream = UnixStream::connect(socket).await?;
        let mut stream = BufReader::new(stream);
        write_json_line(
            &mut stream,
            &json!({"type":"attach", "protocol":24, "actor_names":["counter"]}),
        )
        .await?;
        let _ = read_json_line(&mut stream).await?;
        std::future::pending::<Result<()>>().await
    });
    let connection = listener.accept().await?;
    connection
        .mark_ready(
            None,
            None,
            Arc::new(crate::litestream::Litestream::default()),
        )
        .await?;
    let executor = connection.executor();
    let shutdown = CancellationToken::new();
    let mut running = tokio::spawn(connection.run(shutdown.clone()));
    let mut call = tokio::spawn(async move {
        executor
            .invoke(
                ActorMethodInvocation {
                    request_id: "blocked-write".into(),
                    actor: ActorKey {
                        project_id: "default".into(),
                        actor_name: "counter".into(),
                        actor_id: "one".into(),
                    },
                    method: "accept".into(),
                    args: vec![json!("x".repeat(8 * 1024 * 1024))],
                },
                None,
            )
            .await
    });
    assert!(timeout(Duration::from_millis(30), &mut call).await.is_err());
    shutdown.cancel();
    let stopped = timeout(Duration::from_millis(200), &mut running).await;
    running.abort();
    call.abort();
    customer.abort();
    stopped.context("executor shutdown waited for a blocked socket writer")???;
    Ok(())
}

#[tokio::test]
async fn one_javascript_executor_runs_until_host_shutdown() -> Result<()> {
    let root = TempDir::new_in("/tmp")?;
    let socket = root.path().join("actor-executor.sock");
    let host = ActorExecutorListener::bind(&socket).await?;
    let customer = tokio::spawn(run_incrementing_customer(socket.clone()));
    let connection = host.accept().await?;
    let executor = connection.executor();
    connection
        .mark_ready(
            None,
            None,
            Arc::new(crate::litestream::Litestream::default()),
        )
        .await?;
    assert!(executor.supports("counter"));

    let shutdown = CancellationToken::new();
    let connection_task = tokio::spawn(connection.run(shutdown.clone()));
    let outcome = executor
        .invoke(
            ActorMethodInvocation {
                request_id: "request-1".into(),
                actor: ActorKey {
                    project_id: "default".into(),
                    actor_name: "counter".into(),
                    actor_id: "counter-1".into(),
                },
                method: "increment".into(),
                args: vec![json!(2)],
            },
            None,
        )
        .await?;
    assert_eq!(
        outcome,
        ActorMethodOutcome::Completed {
            result: json!(2),
            state: ActorState {
                sqlite: crate::actor::SqliteState::position(2)
            },
            effects: Vec::new(),
        }
    );
    let socket_outcome = executor
        .handle_socket(
            ActorSocketInvocation {
                request_id: "socket-request-1".into(),
                actor: ActorKey {
                    project_id: "default".into(),
                    actor_name: "counter".into(),
                    actor_id: "counter-1".into(),
                },
                event: ActorSocketEvent::Connect {
                    connection: ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    },
                },
                connections: vec![ActorSocketConnection {
                    id: "socket-1".into(),
                    metadata: json!({ "userId": "user-1" }),
                    tags: Vec::new(),
                }],
            },
            Some(&ActorState {
                sqlite: crate::actor::SqliteState::position(2),
            }),
        )
        .await?;
    assert_eq!(
        socket_outcome,
        ActorSocketOutcome::Handled {
            state: ActorState {
                sqlite: crate::actor::SqliteState::position(3)
            },
            effects: vec![ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: ActorSocketMessage::Text {
                    data: "ready".into()
                },
            }],
        }
    );
    shutdown.cancel();
    connection_task.await??;
    customer.await??;
    Ok(())
}

#[tokio::test]
async fn resident_commands_omit_state_and_retry_only_an_explicit_hydration_request() -> Result<()> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let mut reader = BufReader::new(customer);
    let customer = async {
        let first = read_json_line(&mut reader).await?;
        assert_eq!(first["command"]["sqlite"], json!({"txid": 9}));
        write_json_line(&mut reader, &json!({"type":"reply", "message_id":first["message_id"], "reply":json!({"type":"invoked", "result":10,"sqlite": {"txid": 10}})})).await?;
        let warm = read_json_line(&mut reader).await?;
        assert!(warm["command"].get("sqlite").is_none());
        assert_eq!(warm["command"]["resident_only"], true);
        write_json_line(&mut reader, &json!({"type":"reply", "message_id":warm["message_id"], "reply":json!({"type":"state_required"})})).await?;
        let retry = read_json_line(&mut reader).await?;
        assert_eq!(
            retry["command"]["request_id"],
            warm["command"]["request_id"]
        );
        assert_eq!(retry["command"]["sqlite"], json!({"txid": 10}));
        assert_eq!(retry["command"]["resident_only"], false);
        write_json_line(&mut reader, &json!({"type":"reply", "message_id":retry["message_id"], "reply":json!({"type":"invoked", "result":11,"sqlite": {"txid": 11}})})).await?;
        anyhow::Ok(())
    };
    let invoke = async {
        for count in [9, 10] {
            let outcome = executor
                .invoke(
                    ActorMethodInvocation {
                        request_id: format!("request-{count}"),
                        actor: ActorKey {
                            project_id: "default".into(),
                            actor_name: "counter".into(),
                            actor_id: "one".into(),
                        },
                        method: "increment".into(),
                        args: vec![],
                    },
                    Some(&ActorState {
                        sqlite: crate::actor::SqliteState::position(count),
                    }),
                )
                .await?;
            assert!(
                matches!(outcome, ActorMethodOutcome::Completed {result, ..} if result == json!(count + 1))
            );
        }
        anyhow::Ok(())
    };
    tokio::try_join!(customer, invoke)?;
    running.abort();
    Ok(())
}

#[tokio::test]
async fn executor_receives_the_shared_database_path_for_host_managed_commits() -> Result<()> {
    let state = ActorState {
        sqlite: crate::actor::SqliteState {
            txid: 1,
            path: Some("/tmp/actor.sqlite".into()),
        },
    };
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    let customer = async {
        let mut reader = BufReader::new(customer);
        let message = read_json_line(&mut reader).await?;
        assert_eq!(
            message["command"]["sqlite"],
            serde_json::to_value(&state.sqlite)?
        );
        write_json_line(
            &mut reader,
            &json!({
                "type": "reply", "message_id": message["message_id"],
                "reply": {"type": "invoked", "result": 1, "sqlite": state.sqlite}
            }),
        )
        .await
    };
    let invoke = async {
        let outcome = executor
            .invoke(
                ActorMethodInvocation {
                    request_id: "large".into(),
                    actor: ActorKey {
                        project_id: "default".into(),
                        actor_name: "counter".into(),
                        actor_id: "one".into(),
                    },
                    method: "read".into(),
                    args: vec![],
                },
                Some(&state),
            )
            .await?;
        assert_eq!(
            outcome,
            ActorMethodOutcome::Completed {
                result: json!(1),
                state: state.clone(),
                effects: vec![],
            }
        );
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(30), async {
        tokio::try_join!(customer, invoke)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

async fn run_incrementing_customer(socket: PathBuf) -> Result<()> {
    let stream = UnixStream::connect(socket).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer
        .write_all(b"{\"type\":\"attach\",\"protocol\":24,\"actor_names\":[\"counter\"]}\n")
        .await?;
    ensure!(
        read_json_line(&mut reader).await?
            == json!({ "type": "attached", "protocol": 24, "supports_residency": true })
    );

    let invocation = read_json_line(&mut reader).await?;
    let invocation_id = invocation["message_id"]
        .as_u64()
        .context("invocation message ID")?;
    ensure!(invocation["command"]["type"] == "invoke");
    ensure!(invocation["command"].get("timeout_ms").is_none());
    write_json_line(
        &mut writer,
        &json!({
            "type": "reply",
            "message_id": invocation_id,
            "reply": {
                "type": "invoked",
                "result": 2,
                "sqlite": {"txid": 2}
            }
        }),
    )
    .await?;

    let socket_event = read_json_line(&mut reader).await?;
    let socket_event_id = socket_event["message_id"]
        .as_u64()
        .context("socket event message ID")?;
    ensure!(socket_event["command"]["type"] == "websocket_event");
    ensure!(socket_event["command"]["event"]["type"] == "connect");
    write_json_line(
        &mut writer,
        &json!({
            "type": "reply",
            "message_id": socket_event_id,
            "reply": {
                "type": "websocket_handled",
                "sqlite": {"txid": 3},
                "effects": [{
                    "type": "send",
                    "connection_id": "socket-1",
                    "message": { "type": "text", "data": "ready" }
                }]
            }
        }),
    )
    .await?;

    let mut trailing = String::new();
    ensure!(
        reader.read_line(&mut trailing).await? == 0,
        "expected Rust host to close the actor executor"
    );
    Ok(())
}

async fn read_json_line<R>(reader: &mut R) -> Result<Value>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = String::new();
    ensure!(reader.read_line(&mut line).await? > 0, "expected JSON line");
    Ok(serde_json::from_str(line.trim_end())?)
}

async fn write_json_line<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    writer
        .write_all(serde_json::to_string(value)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    Ok(())
}

#[tokio::test]
async fn commit_acknowledges_capture_for_the_invocations_registered_database() -> Result<()> {
    use crate::litestream::{Litestream, Replicator, storage::SqliteCapture};
    let replication = Arc::new(Litestream::default());
    let capture = SqliteCapture::new(replication.clone()).await?;
    let state = ActorState {
        sqlite: capture.state(),
    };
    let database = rusqlite::Connection::open(capture.path())?;
    database.execute_batch("CREATE TABLE entries(value); INSERT INTO entries VALUES(7)")?;
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    executor.mark_ready(None, None, replication.clone()).await?;
    let peer = async {
        let mut customer = BufReader::new(customer);
        read_json_line(&mut customer).await?;
        let invocation = read_json_line(&mut customer).await?;
        let id = &invocation["message_id"];
        write_json_line(
            &mut customer,
            &json!({"type":"commit_sqlite", "message_id":id, "path":"/not-authorized.sqlite"}),
        )
        .await?;
        let committed = read_json_line(&mut customer).await?;
        assert_eq!(committed["type"], "sqlite_committed");
        assert_eq!(committed["message_id"], *id);
        assert!(committed["txid"].as_u64().unwrap() > state.sqlite.txid);
        write_json_line(&mut customer, &json!({"type":"reply", "message_id":id, "reply":{"type":"invoked","result":7,"sqlite":{"txid":committed["txid"]}}})).await?;
        anyhow::Ok(())
    };
    let caller = async {
        let result = executor
            .invoke(
                ActorMethodInvocation {
                    request_id: "commit".into(),
                    actor: ActorKey {
                        project_id: "default".into(),
                        actor_name: "counter".into(),
                        actor_id: "one".into(),
                    },
                    method: "write".into(),
                    args: vec![],
                },
                Some(&state),
            )
            .await?;
        let ActorMethodOutcome::Completed { state, .. } = result else {
            anyhow::bail!("unexpected outcome")
        };
        assert_eq!(replication.sync(&capture.path()).await?, state.sqlite.txid);
        anyhow::Ok(())
    };
    let result = timeout(Duration::from_secs(3), async {
        tokio::try_join!(peer, caller)
    })
    .await;
    running.abort();
    result??;
    Ok(())
}

#[tokio::test]
async fn sqlite_capture_waits_without_blocking_executor_admission() -> Result<()> {
    let capture = Arc::new(ControlledCapture::new(false));
    let (executor, running, mut peer, caller) = commit_fixture(capture.clone()).await?;
    let mut admission = executor.invocation_admission().unwrap();
    write_json_line(&mut peer, &json!({"type":"commit_sqlite", "message_id":1})).await?;
    timeout(Duration::from_secs(1), capture.started.notified()).await?;
    write_json_line(
        &mut peer,
        &json!({"type":"ready_for_invocation", "message_id":1}),
    )
    .await?;
    timeout(Duration::from_secs(1), admission.changed()).await??;
    assert!(
        timeout(Duration::from_millis(20), read_json_line(&mut peer))
            .await
            .is_err()
    );
    capture.release.add_permits(1);
    assert_eq!(
        read_json_line(&mut peer).await?,
        json!({"type":"sqlite_committed", "message_id":1, "txid":2})
    );
    write_json_line(&mut peer, &commit_reply(2)).await?;
    assert!(matches!(
        caller.await??,
        ActorMethodOutcome::Completed { .. }
    ));
    running.abort();
    Ok(())
}

#[tokio::test]
async fn sqlite_capture_failure_is_acknowledged_and_cannot_be_reported_as_success() -> Result<()> {
    for success in [false, true] {
        let capture = Arc::new(ControlledCapture::new(true));
        let (_executor, running, mut peer, caller) = commit_fixture(capture.clone()).await?;
        write_json_line(&mut peer, &json!({"type":"commit_sqlite", "message_id":1})).await?;
        capture.release.add_permits(1);
        let reply = read_json_line(&mut peer).await?;
        assert_eq!(reply["type"], "sqlite_committed");
        assert_eq!(reply["message_id"], 1);
        assert!(reply["error"].as_str().unwrap().contains("capture failed"));
        assert!(reply.get("txid").is_none());
        if success {
            write_json_line(&mut peer, &commit_reply(2)).await?;
            let error = timeout(Duration::from_secs(1), running)
                .await??
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("success after SQLite capture failed")
            );
            assert!(caller.await?.is_err());
        } else {
            write_json_line(&mut peer, &json!({"type":"reply", "message_id":1, "reply":{"type":"failed", "code":"sqlite_capture_failed", "message":"capture failed"}})).await?;
            assert!(matches!(caller.await??, ActorMethodOutcome::Failed(_)));
            running.abort();
        }
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_capture_rejects_unknown_duplicate_and_premature_messages() -> Result<()> {
    for violation in ["unknown", "duplicate", "premature", "wrong_position"] {
        let capture = Arc::new(ControlledCapture::new(false));
        let (_executor, running, mut peer, caller) = commit_fixture(capture.clone()).await?;
        if violation != "unknown" {
            write_json_line(&mut peer, &json!({"type":"commit_sqlite", "message_id":1})).await?;
            timeout(Duration::from_secs(1), capture.started.notified()).await?;
        }
        let (message, expected) = match violation {
            "unknown" => (
                json!({"type":"commit_sqlite", "message_id":99}),
                "no active invocation",
            ),
            "duplicate" => (
                json!({"type":"commit_sqlite", "message_id":1}),
                "already requested",
            ),
            "premature" => (commit_reply(2), "before SQLite capture completed"),
            _ => {
                capture.release.add_permits(1);
                assert_eq!(read_json_line(&mut peer).await?["txid"], 2);
                (commit_reply(3), "differs from its captured SQLite position")
            }
        };
        write_json_line(&mut peer, &message).await?;
        let error = timeout(Duration::from_secs(1), running)
            .await??
            .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{violation}: {error:#}"
        );
        assert!(caller.await?.is_err());
    }
    Ok(())
}

fn commit_reply(txid: u64) -> Value {
    json!({"type":"reply", "message_id":1, "reply":{"type":"invoked", "result":7, "sqlite":{"txid":txid}}})
}

async fn commit_fixture(
    capture: Arc<ControlledCapture>,
) -> Result<(
    Arc<JsActorExecutor>,
    JoinHandle<Result<()>>,
    BufReader<UnixStream>,
    JoinHandle<Result<ActorMethodOutcome>>,
)> {
    let (host, customer) = UnixStream::pair()?;
    let (reader, writer) = host.into_split();
    let (executor, running) =
        JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
    executor.mark_ready(None, None, capture).await?;
    let mut peer = BufReader::new(customer);
    assert_eq!(read_json_line(&mut peer).await?["type"], "attached");
    let caller = tokio::spawn({
        let executor = executor.clone();
        async move {
            executor
                .invoke(
                    ActorMethodInvocation {
                        request_id: "commit".into(),
                        actor: ActorKey {
                            project_id: "default".into(),
                            actor_name: "counter".into(),
                            actor_id: "one".into(),
                        },
                        method: "write".into(),
                        args: vec![],
                    },
                    Some(&ActorState {
                        sqlite: crate::actor::SqliteState {
                            txid: 1,
                            path: Some("/host/actor.sqlite".into()),
                        },
                    }),
                )
                .await
        }
    });
    assert_eq!(read_json_line(&mut peer).await?["message_id"], 1);
    Ok((executor, running, peer, caller))
}

struct ControlledCapture {
    started: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    fail: bool,
}

impl ControlledCapture {
    fn new(fail: bool) -> Self {
        Self {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            fail,
        }
    }
}

#[async_trait]
impl crate::litestream::Replicator for ControlledCapture {
    async fn register(&self, _: &Path, _: &Path) -> Result<()> {
        anyhow::bail!("unexpected registration")
    }
    async fn sync(&self, path: &Path) -> Result<u64> {
        assert_eq!(path, Path::new("/host/actor.sqlite"));
        self.started.notify_one();
        self.release.acquire().await?.forget();
        ensure!(!self.fail, "capture failed");
        Ok(2)
    }
    async fn unregister(&self, _: &Path) -> Result<()> {
        anyhow::bail!("unexpected unregistration")
    }
}

#[async_trait]
impl crate::litestream::DatabaseRestore for ControlledCapture {
    async fn restore(&self, _: &Path, _: &Path, _: u64) -> Result<()> {
        anyhow::bail!("unexpected restore")
    }
}
