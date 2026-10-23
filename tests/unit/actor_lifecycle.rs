use super::*;

#[tokio::test]
async fn inventory_distinguishes_resident_idle_stopped_and_stale_actors() -> Result<()> {
    let store = LifecycleStore::default();
    let now = SystemClock.now_ms()?;
    for (id, kind, resident, received_at_ms) in [
        ("resident", LifecycleKind::Heartbeat, Some(true), now),
        ("idle", LifecycleKind::Heartbeat, Some(false), now),
        (
            "stopped",
            LifecycleKind::Stopped,
            Some(false),
            now - STALE_AFTER_MS - 1,
        ),
        (
            "stale",
            LifecycleKind::Heartbeat,
            Some(true),
            now - STALE_AFTER_MS - 1,
        ),
    ] {
        store
            .record(LifecycleEvent {
                event_id: id.into(),
                actor: ActorKey {
                    project_id: "project".into(),
                    actor_name: "Counter".into(),
                    actor_id: id.into(),
                },
                host_id: id.into(),
                session_id: id.into(),
                region: "us-east".into(),
                received_at_ms,
                report: LifecycleReport {
                    kind,
                    owner_epoch: 1,
                    sequence: 1,
                    started_at_ms: 1000,
                    occurred_at_ms: 1000,
                    resources: Default::default(),
                    resident,
                    waiting: vec![WaitingOperation {
                        id: "request".into(),
                        operation: "increment".into(),
                    }],
                },
            })
            .await?;
    }
    let snapshot = store.actor_inventory("project").await?;
    let actor = &snapshot.actors[0];
    assert_eq!((actor.live, actor.dormant, actor.unknown), (1, 2, 1));
    for instance in &actor.instances {
        match instance.actor_id.as_str() {
            "resident" | "idle" => {
                assert_eq!(instance.waiting.as_ref().unwrap()[0].operation, "increment")
            }
            "stopped" => assert!(instance.waiting.as_ref().unwrap().is_empty()),
            "stale" => assert!(instance.waiting.is_none()),
            _ => unreachable!(),
        }
    }
    assert!(
        store
            .actor_inventory("another-project")
            .await?
            .actors
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn latest_report_follows_activation_shutdown_and_reactivation_per_project() -> Result<()> {
    let persistence = Arc::new(MemoryLifecyclePersistence::default());
    let store = LifecycleStore::new(persistence.clone());
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let mut event = LifecycleEvent {
        event_id: "session:1".into(),
        actor: actor.clone(),
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us-east".into(),
        received_at_ms: 1000,
        report: LifecycleReport {
            kind: LifecycleKind::Started,
            owner_epoch: 1,
            sequence: 1,
            started_at_ms: 1000,
            occurred_at_ms: 1000,
            resources: Default::default(),
            resident: Some(true),
            waiting: Vec::new(),
        },
    };
    store.record(event.clone()).await?;
    event.actor.project_id = "other-project".into();
    store.record(event.clone()).await?;
    let other_actor = event.actor.clone();
    event.actor = actor.clone();
    event.event_id = "session:2".into();
    event.report.kind = LifecycleKind::Stopped;
    event.report.sequence = 2;
    event.report.occurred_at_ms = 2000;
    event.received_at_ms = 2000;
    store.record(event.clone()).await?;
    assert_eq!(
        persistence.0.get(&actor).await.unwrap().report.kind,
        LifecycleKind::Stopped
    );
    assert_eq!(
        persistence.0.get(&other_actor).await.unwrap().report.kind,
        LifecycleKind::Started
    );
    let stopped = event.clone();
    event.event_id = "next-session:1".into();
    event.host_id = "next-host".into();
    event.session_id = "next-session".into();
    event.report.kind = LifecycleKind::Started;
    event.report.owner_epoch = 2;
    event.report.sequence = 1;
    event.report.started_at_ms = 3000;
    event.report.occurred_at_ms = 3000;
    event.received_at_ms = 3000;
    store.record(event.clone()).await?;
    store.record(stopped).await?;
    assert_eq!(
        persistence.0.get(&actor).await.unwrap().event_id,
        event.event_id
    );
    Ok(())
}
