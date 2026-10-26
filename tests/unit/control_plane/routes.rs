use super::*;

#[tokio::test(start_paused = true)]
async fn routes_survive_time_and_only_matching_invalidations_remove_them() {
    let cache = RouteCache::default();
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let old = route(&actor, 1);
    cache
        .remember(&actor, old.clone(), cache.generation())
        .await;
    tokio::time::advance(std::time::Duration::from_secs(86_400)).await;
    assert_eq!(cache.get(&actor).await, Some(old.clone()));
    let current = route(&actor, 2);
    cache
        .remember(&actor, current.clone(), cache.generation())
        .await;
    cache
        .invalidate(RouteIdentity::new(&actor, &old.placement))
        .await;
    assert_eq!(cache.get(&actor).await, Some(current.clone()));
    let generation = cache.generation();
    cache
        .invalidate(RouteIdentity::new(&actor, &current.placement))
        .await;
    cache.remember(&actor, current, generation).await;
    assert_eq!(cache.get(&actor).await, None);
}

#[tokio::test]
async fn invalidations_reach_other_gateway_caches() -> anyhow::Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let redis = Redis::connect(&crate::redis_client::RedisConfig { url, ca_cert: None })?;
    let stop = CancellationToken::new();
    let channel = format!("test-routes:{}", uuid::Uuid::new_v4());
    let a = RouteCache::default().with_redis(redis.clone(), &channel, stop.clone());
    let b = RouteCache::default().with_redis(redis, &channel, stop.clone());
    wait_for(|| a.connected.load(Ordering::SeqCst) && b.connected.load(Ordering::SeqCst)).await;
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let target = route(&actor, 1);
    b.remember(&actor, target.clone(), b.generation()).await;
    let generation = b.generation();
    a.invalidate(RouteIdentity::new(&actor, &target.placement))
        .await;
    wait_for(|| b.generation() > generation).await;
    assert_eq!(b.get(&actor).await, None);
    b.remember(&actor, target, b.generation()).await;
    let generation = b.generation();
    a.invalidate_project(&actor.project_id).await;
    wait_for(|| b.generation() > generation).await;
    assert_eq!(b.get(&actor).await, None);
    stop.cancel();
    Ok(())
}

async fn wait_for(predicate: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

fn route(actor: &ActorKey, epoch: u64) -> CachedRoute {
    let host = HostId::new(format!("host-{epoch}"));
    CachedRoute {
        placement: ObjectPlacement {
            lease: crate::host_leases::HostLease {
                id: host.clone(),
                session_id: format!("session-{epoch}"),
                route: "http://127.0.0.1:8000".into(),
                expires_at_ms: 30_000,
            },
            object: actor.storage_key(),
            owner: host,
            owner_epoch: epoch,
            home_region: "north-america-east".into(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        },
        host_config_key: "config".into(),
        overrides_regions: false,
    }
}
