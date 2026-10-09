use super::*;
use crate::{postgres::testing::with_postgres, sandbox::*};

const DONE_STOPPED_AT_MS: i64 = 1_791_574_800_250;
const RETIRED_STOPPED_AT_MS: i64 = 1_791_574_830_500;

#[derive(Default)]
struct Provider {
    fail: std::sync::atomic::AtomicBool,
    pause: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    stopping: std::sync::Mutex<Vec<String>>,
    released: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl SandboxProvider for Provider {
    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<StoppedSpare>> {
        anyhow::ensure!(
            !self.fail.load(std::sync::atomic::Ordering::SeqCst),
            "API unavailable"
        );
        let pause = self.pause.lock().unwrap().take();
        if let Some((observed, resume)) = pause {
            let _ = observed.send(());
            resume.await?;
        }
        let stopping = self.stopping.lock().unwrap().clone();
        Ok(spares
            .iter()
            .filter_map(|spare| {
                let stopped_at_ms = match spare.name.as_str() {
                    "do-actor-done" => Some(DONE_STOPPED_AT_MS),
                    "do-actor-evicted" => None,
                    name if stopping.iter().any(|stopped| stopped == name) => {
                        Some(RETIRED_STOPPED_AT_MS)
                    }
                    _ => return None,
                };
                Some(StoppedSpare {
                    resource_id: spare.resource_id.clone(),
                    stopped_at_ms,
                })
            })
            .collect())
    }
    async fn stop_spare(&self, spare: &SpareHandle) -> Result<()> {
        self.stopping.lock().unwrap().push(spare.name.clone());
        Ok(())
    }
    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        self.released.lock().unwrap().push(spare.name.clone());
        Ok(())
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("unexpected assignment")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}

#[tokio::test]
async fn completed_hosts_are_removed_but_live_hosts_and_uncertain_observations_are_retained()
-> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(Provider {
            fail: true.into(),
            ..Default::default()
        });
        let pool = SparePool::new(
            PostgresDatabase::connect(&fixture.url).await?,
            provider.clone(),
            PoolConfig {
                control_plane_url: None,
                kind: SpareKind::Actor,
                idle: 0,
                fleet_maximum: 0,
                max_starting: 1,
                idle_ttl_seconds: 600,
                regions: vec![],
                resources: ResourceLimits::default(),
            },
            true,
        );
        for name in ["done", "live"] {
            pool.reserve_host(name, name, "revision").await?;
            let spare = SpareHandle {
                name: format!("do-actor-{name}"),
                resource_id: format!("sandboxes/do-actor-{name}/{name}-uid"),
                route: "http://host:7101".into(),
                canonical_region: "region".into(),
                control_route: "http://host:7102".into(),
                control_token: "token".into(),
            };
            pool.remember(name, "revision", &spare, &usage_assignment(name, &spare))
                .await?;
        }
        pool.reserve_host("starting", "starting", "revision")
            .await?;
        assert!(pool.forget_stopped().await.is_err());
        assert!(pool.host("done").await?.is_some());
        assert!(pool.host("live").await?.is_some());
        provider
            .fail
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let stop = CancellationToken::new();
        pool.start(
            Arc::new(crate::control_plane::admin::LocalAdminRegistry::default()),
            stop.clone(),
        );
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            while pool.host("done").await?.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            anyhow::Ok(())
        })
        .await;
        stop.cancel();
        result??;
        pool.forget_stopped().await?;
        assert!(pool.host("done").await?.is_none());
        assert!(pool.host("live").await?.is_some());
        assert_eq!(
            pool.store
                .0
                .query_one("SELECT count(*) FROM durable_actors_spares", &[])
                .await?
                .get::<_, i64>(0),
            2
        );
        let events = crate::usage::UsageOutbox::new(pool.store.0.clone())
            .pending()
            .await?;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == crate::usage::UsageEventType::Started)
                .count(),
            2
        );
        let stopped = events
            .iter()
            .find(|event| event.event_type == crate::usage::UsageEventType::Stopped)
            .unwrap();
        assert_eq!(stopped.assignment.session_id, "done");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn retired_hosts_record_their_stop_when_the_container_exits() -> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(Provider::default());
        let pool = SparePool::new(
            PostgresDatabase::connect(&fixture.url).await?,
            provider.clone(),
            PoolConfig {
                control_plane_url: None,
                kind: SpareKind::Actor,
                idle: 0,
                fleet_maximum: 0,
                max_starting: 1,
                idle_ttl_seconds: 600,
                regions: vec![],
                resources: ResourceLimits::default(),
            },
            true,
        );
        pool.reserve_host("retired", "host", "revision").await?;
        let spare = SpareHandle {
            name: "do-actor-retired".into(),
            resource_id: "sandboxes/do-actor-retired/uid".into(),
            route: "http://host:7101".into(),
            canonical_region: "region".into(),
            control_route: "http://host:7102".into(),
            control_token: "token".into(),
        };
        pool.remember(
            "host",
            "revision",
            &spare,
            &usage_assignment("retired", &spare),
        )
        .await?;

        assert_eq!(
            pool.retire_config("revision").await?,
            vec![spare.resource_id]
        );
        let outbox = crate::usage::UsageOutbox::new(pool.store.0.clone());
        assert_eq!(outbox.pending().await?.len(), 1);
        assert_eq!(*provider.stopping.lock().unwrap(), ["do-actor-retired"]);
        assert!(provider.released.lock().unwrap().is_empty());

        pool.forget_stopped().await?;
        let events = outbox.pending().await?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type, crate::usage::UsageEventType::Stopped);
        assert_eq!(events[1].assignment.session_id, "retired");
        assert_eq!(events[1].observed_at_ms, RETIRED_STOPPED_AT_MS);
        assert_eq!(*provider.released.lock().unwrap(), ["do-actor-retired"]);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn stop_events_use_the_runtime_termination_time_when_it_is_still_recorded() -> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(Provider::default());
        let pool = SparePool::new(
            PostgresDatabase::connect(&fixture.url).await?,
            provider.clone(),
            PoolConfig {
                control_plane_url: None,
                kind: SpareKind::Actor,
                idle: 0,
                fleet_maximum: 0,
                max_starting: 1,
                idle_ttl_seconds: 600,
                regions: vec![],
                resources: ResourceLimits::default(),
            },
            true,
        );
        for name in ["done", "evicted"] {
            pool.reserve_host(name, name, "revision").await?;
            let spare = SpareHandle {
                name: format!("do-actor-{name}"),
                resource_id: format!("sandboxes/do-actor-{name}/{name}-uid"),
                route: "http://host:7101".into(),
                canonical_region: "region".into(),
                control_route: "http://host:7102".into(),
                control_token: "token".into(),
            };
            pool.remember(name, "revision", &spare, &usage_assignment(name, &spare))
                .await?;
        }

        let detecting_from_ms = now_ms()?;
        pool.forget_stopped().await?;
        let detected_by_ms = now_ms()?;
        let stops: std::collections::HashMap<_, _> =
            crate::usage::UsageOutbox::new(pool.store.0.clone())
                .pending()
                .await?
                .into_iter()
                .filter(|event| event.event_type == crate::usage::UsageEventType::Stopped)
                .map(|event| (event.assignment.session_id, event.observed_at_ms))
                .collect();
        assert_eq!(stops.len(), 2);
        assert_eq!(stops["done"], DONE_STOPPED_AT_MS);
        assert!((detecting_from_ms..=detected_by_ms).contains(&stops["evicted"]));
        let mut released = provider.released.lock().unwrap().clone();
        released.sort();
        assert_eq!(released, ["do-actor-done", "do-actor-evicted"]);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn stale_cleanup_observation_cannot_remove_a_replacement_identity() -> Result<()> {
    with_postgres(async |fixture| {
        let (observed, captured) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let provider = Arc::new(Provider {
            pause: Some((observed, paused)).into(),
            ..Default::default()
        });
        let pool = SparePool::new(
            PostgresDatabase::connect(&fixture.url).await?,
            provider.clone(),
            PoolConfig {
                control_plane_url: None,
                kind: SpareKind::Actor,
                idle: 0,
                fleet_maximum: 0,
                max_starting: 1,
                idle_ttl_seconds: 600,
                regions: vec![],
                resources: ResourceLimits::default(),
            },
            false,
        );
        let mut spare = SpareHandle {
            name: "do-actor-done".into(),
            resource_id: "sandboxes/do-actor-done/old-uid".into(),
            route: "http://host:7101".into(),
            canonical_region: "region".into(),
            control_route: "http://host:7102".into(),
            control_token: "token".into(),
        };
        pool.reserve_host("done", "old", "revision").await?;
        pool.remember("old", "revision", &spare, &usage_assignment("old", &spare))
            .await?;
        let cleanup = pool.forget_stopped();
        tokio::pin!(cleanup);
        tokio::select! {
            result = &mut cleanup => panic!("cleanup did not wait for observation: {result:?}"),
            result = captured => result?,
        }
        pool.store
            .0
            .execute(
                "DELETE FROM durable_actors_spares WHERE host_id = 'old'",
                &[],
            )
            .await?;
        pool.reserve_host("done", "new", "revision").await?;
        spare.resource_id = "sandboxes/do-actor-done/new-uid".into();
        pool.remember("new", "revision", &spare, &usage_assignment("new", &spare))
            .await?;
        let _ = resume.send(());
        cleanup.await?;
        assert_eq!(pool.host("new").await?, Some(spare));
        assert!(provider.released.lock().unwrap().is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reconciliation_excludes_evicted_spares_from_subsequent_claims() -> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(Provider::default());
        let pool = SparePool::new(PostgresDatabase::connect(&fixture.url).await?, provider, PoolConfig {
            control_plane_url: None, kind: SpareKind::Actor, idle: 2, fleet_maximum: 4,
            max_starting: 2, idle_ttl_seconds: 600, regions: vec![], resources: ResourceLimits::default(),
        }, false);
        for name in ["evicted", "warm"] {
            let spare = SpareHandle {
                name: format!("do-actor-{name}"),
                resource_id: format!("sandboxes/do-actor-{name}/{name}-uid"),
                route: "http://host:7101".into(),
                canonical_region: "region".into(),
                control_route: "http://host:7102".into(),
                control_token: "token".into(),
            };
            pool.store.0.execute("INSERT INTO durable_actors_spares (name, pool_key, status, handle, expires_at) VALUES ($1, 'warm', 'ready', $2, clock_timestamp() + interval '10 minutes')", &[&spare.name, &serde_json::to_string(&spare)?]).await?;
        }
        pool.forget_stopped().await?;
        assert_eq!(pool.store.claim("warm", "next", "revision").await?.unwrap().name, "do-actor-warm");
        assert!(pool.store.claim("warm", "another", "revision").await?.is_none());
        Ok(())
    }).await
}

fn now_ms() -> Result<i64> {
    Ok(crate::clock::Clock::now_ms(&crate::clock::SystemClock)?.try_into()?)
}

fn usage_assignment(session: &str, spare: &SpareHandle) -> crate::usage::UsageAssignment {
    crate::usage::UsageAssignment {
        billing_account_id: None,
        project_id: "project".into(),
        session_id: session.into(),
        resource_id: spare.resource_id.clone(),
        region: spare.canonical_region.clone(),
        cpu_millis: 250,
        memory_mib: 256,
    }
}
