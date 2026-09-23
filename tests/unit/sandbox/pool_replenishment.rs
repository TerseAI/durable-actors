use super::*;
use crate::sandbox::{
    ActorHostHandle, BuildCodeRequest, BuiltActorCode, CreateSpareRequest, EnsureHostRequest,
    HostTermination, SocketCredentials, SocketCredentialsRequest, TerminateHostsRequest,
};

fn handle(name: String) -> SpareHandle {
    SpareHandle {
        resource_id: format!("sb-{name}"),
        name,
        route: "https://host.test".into(),
        canonical_region: "region".into(),
        control_route: "https://assign.test".into(),
        control_token: "secret".into(),
    }
}

#[tokio::test]
async fn misses_are_counted_once_and_shared_by_controllers() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let first = PoolStore(database.clone(), SpareKind::Actor);
        let second = PoolStore(database.clone(), SpareKind::Actor);
        for index in 0..20 {
            for _ in 0..2 {
                assert!(
                    first
                        .claim("busy", &format!("host-{index}"), "revision")
                        .await?
                        .is_none()
                );
            }
        }
        let count: i64 = database
            .query_one(
                "SELECT count(*) FROM durable_actors_pool_events WHERE event_kind = 'acquire'",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(count, 20);
        let mut config = config(5);
        config.maximum = 32;
        assert_eq!(second.replenish("busy", &config, 32).await?.len(), 8);
        assert!(first.replenish("busy", &config, 32).await?.is_empty());
        let target: i32 = database
            .query_one(
                "SELECT target FROM durable_actors_pool_targets WHERE pool_key = 'busy'",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(target, 32);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn fleet_capacity_and_build_limits_are_atomic_across_roles() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let actor = PoolStore(database.clone(), SpareKind::Actor);
        let replica = PoolStore(database.clone(), SpareKind::Replica);
        let mut config = config(3);
        config.fleet_maximum = 3;
        config.max_starting = 2;
        let (actors, replicas) = tokio::try_join!(
            actor.replenish("actors", &config, 8),
            replica.replenish("replicas", &config, 8)
        )?;
        assert_eq!(actors.len() + replicas.len(), 2);
        let (store, key, name) = match actors.first() {
            Some(name) => (&actor, "actors", name),
            None => (&replica, "replicas", &replicas[0]),
        };
        assert!(store.publish(key, &handle(name.clone()), 600).await?);
        let (actors, replicas) = tokio::try_join!(
            actor.replenish("actors", &config, 8),
            replica.replenish("replicas", &config, 8)
        )?;
        assert_eq!(actors.len() + replicas.len(), 1);
        assert!(actor.replenish("actors", &config, 8).await?.is_empty());
        assert!(replica.replenish("replicas", &config, 8).await?.is_empty());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn expiring_spares_stay_claimable_until_a_replacement_is_ready() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let store = PoolStore(database.clone(), SpareKind::Actor);
        let mut config = config(1);
        config.maximum = 2;
        let old = store.replenish("pool", &config, 8).await?.remove(0);
        store.publish("pool", &handle(old.clone()), 600).await?;
        database.execute("UPDATE durable_actors_spares SET expires_at = clock_timestamp() + interval '1 second' WHERE name = $1", &[&old]).await?;
        let new = store.replenish("pool", &config, 8).await?.remove(0);
        assert_eq!(database.query_one("SELECT status FROM durable_actors_spares WHERE name = $1", &[&old]).await?.get::<_, &str>(0), "ready");
        assert!(store.replenish("pool", &config, 8).await?.is_empty());
        store.publish("pool", &handle(new.clone()), 600).await?;
        store.replenish("pool", &config, 8).await?;
        assert_eq!(database.query_one("SELECT status FROM durable_actors_spares WHERE name = $1", &[&old]).await?.get::<_, &str>(0), "retiring");
        assert_eq!(store.claim("pool", "host", "rev").await?.unwrap().name, new);
        Ok(())
    }).await
}

#[tokio::test]
async fn failed_builds_back_off_without_blocking_other_pools() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let store = PoolStore(database.clone(), SpareKind::Actor);
        let config = config(1);
        let name = store.replenish("failed", &config, 8).await?.remove(0);
        store.build_failed("failed", &name).await?;
        assert!(store.replenish("failed", &config, 8).await?.is_empty());
        assert_eq!(store.replenish("healthy", &config, 8).await?.len(), 1);
        database.execute("UPDATE durable_actors_pool_targets SET retry_after = clock_timestamp() - interval '1 second' WHERE pool_key = 'failed'", &[]).await?;
        assert_eq!(store.replenish("failed", &config, 8).await?.len(), 1);
        Ok(())
    }).await
}

#[tokio::test]
async fn retiring_spares_hold_their_budget_until_termination() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let store = PoolStore(database.clone(), SpareKind::Actor);
        let mut config = config(1);
        config.fleet_maximum = 1;
        let name = store.replenish("old", &config, 8).await?.remove(0);
        store.publish("old", &handle(name), 600).await?;
        store.retire_unwanted(&["new".into()], true).await?;
        assert!(store.replenish("new", &config, 8).await?.is_empty());
        database
            .execute(
                "DELETE FROM durable_actors_spares WHERE status = 'retiring'",
                &[],
            )
            .await?;
        assert_eq!(store.replenish("new", &config, 8).await?.len(), 1);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn target_shrink_deadlines_survive_controller_changes() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let first = PoolStore(database.clone(), SpareKind::Actor);
        let second = PoolStore(database.clone(), SpareKind::Actor);
        let mut config = config(5);
        config.maximum = 32;
        first.replenish("pool", &config, 0).await?;
        database.execute("UPDATE durable_actors_pool_targets SET target = 20, shrink_at_ms = (extract(epoch FROM clock_timestamp()) * 1000)::bigint - 1 WHERE pool_key = 'pool'", &[]).await?;
        second.replenish("pool", &config, 0).await?;
        let row = database.query_one("SELECT target, shrink_at_ms FROM durable_actors_pool_targets WHERE pool_key = 'pool'", &[]).await?;
        assert_eq!(row.get::<_, i32>(0), 18);
        let deadline: i64 = row.get(1);
        first.replenish("pool", &config, 0).await?;
        let row = database.query_one("SELECT target, shrink_at_ms FROM durable_actors_pool_targets WHERE pool_key = 'pool'", &[]).await?;
        assert_eq!(row.get::<_, i32>(0), 18);
        assert_eq!(row.get::<_, i64>(1), deadline);
        Ok(())
    }).await
}

#[tokio::test]
async fn shrinking_cannot_retire_a_spare_claimed_while_it_waits_for_a_lock() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let store = Arc::new(PoolStore(database.clone(), SpareKind::Actor));
        let names = store.replenish("pool", &config(2), 8).await?;
        for name in &names { store.publish("pool", &handle(name.clone()), 600).await?; }
        database.execute("UPDATE durable_actors_spares SET expires_at = clock_timestamp() + interval '590 seconds' WHERE name = $1", &[&names[0]]).await?;
        let mut client = database.connection().await?;
        let tx = client.transaction().await?;
        let pid: i32 = tx.query_one("SELECT pg_backend_pid() FROM durable_actors_spares WHERE name = $1 FOR UPDATE", &[&names[0]]).await?.get(0);
        let trimming = { let store = store.clone(); tokio::spawn(async move { store.replenish("pool", &config(1), 8).await }) };
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let waiting: bool = database.query_one("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))", &[&pid]).await?.get(0);
                if waiting { return anyhow::Ok(()); }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        tx.execute("UPDATE durable_actors_spares SET status = 'claimed', host_id = 'claimed-host' WHERE name = $1", &[&names[0]]).await?;
        tx.commit().await?;
        tokio::time::timeout(Duration::from_secs(3), trimming).await???;
        assert_eq!(database.query_one("SELECT status FROM durable_actors_spares WHERE name = $1", &[&names[0]]).await?.get::<_, &str>(0), "claimed");
        Ok(())
    }).await
}

#[tokio::test]
async fn a_stalled_region_does_not_block_claim_replenishment_in_another_region() -> Result<()> {
    use crate::control_plane::admin::{HostLaunchSpec, LocalAdminRegistry};
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let spec = HostLaunchSpec {
            project_id: "project".into(), source: None, image_ref: "im-runtime".into(),
            code_snapshot: Some("im-code".into()), working_directory: "/customer".into(),
            actor_entrypoint: Some("actors.mjs".into()), secret_refs: vec![],
        };
        registry.register_test_deployment(&spec).await?;
        let mut config = config(1);
        config.regions = vec!["slow".into(), "fast".into()];
        let pool = SparePool::new(database.clone(), Arc::new(RegionalProvider), config);
        let stop = CancellationToken::new();
        let task = tokio::spawn(pool.clone().run(registry, stop.clone()));
        let guard = stop.clone().drop_guard();
        let key = pool.key("im-runtime", "fast");
        wait_for_ready(&database, &key).await?;
        assert!(pool.claim(&spec, "fast", "host").await?.is_some());
        wait_for_ready(&database, &key).await?;
        assert_eq!(database.query_one("SELECT count(*) FROM durable_actors_spares WHERE pool_key = $1 AND status = 'starting'", &[&pool.key("im-runtime", "slow")]).await?.get::<_, i64>(0), 1);
        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), task).await??;
        Ok(())
    }).await
}

async fn wait_for_ready(database: &PostgresDatabase, key: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready: i64 = database.query_one("SELECT count(*) FROM durable_actors_spares WHERE pool_key = $1 AND status = 'ready'", &[&key]).await?.get(0);
            if ready > 0 { return anyhow::Ok(()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await?
}

struct RegionalProvider;

#[async_trait::async_trait]
impl SandboxProvider for RegionalProvider {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        if request.canonical_region == "slow" {
            std::future::pending::<()>().await;
        }
        Ok(SpareHandle {
            canonical_region: request.canonical_region.clone(),
            ..handle(request.name.clone())
        })
    }
    async fn retire_spare(&self, _: &SpareHandle) -> Result<()> {
        Ok(())
    }
    async fn build_code(&self, _: &BuildCodeRequest) -> Result<BuiltActorCode> {
        anyhow::bail!("unexpected build")
    }
    async fn socket_credentials(&self, _: &SocketCredentialsRequest) -> Result<SocketCredentials> {
        anyhow::bail!("unexpected credentials")
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("unexpected assignment")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}
