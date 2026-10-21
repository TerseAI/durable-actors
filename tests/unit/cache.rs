use super::*;
use std::sync::atomic::AtomicUsize;

async fn counted<V>(loads: &AtomicUsize, value: V) -> Result<V> {
    loads.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(value)
}

#[tokio::test]
async fn memory_caches_insert_read_and_invalidate() {
    let cache = Caches::memory().shared::<String>("test", 10, Duration::from_secs(60));
    assert_eq!(cache.get("a").await, None);
    cache.insert("a", "one".into()).await;
    assert_eq!(cache.get("a").await.as_deref(), Some("one"));
    cache.invalidate("a").await;
    assert_eq!(cache.get("a").await, None);
}

#[tokio::test]
async fn concurrent_misses_share_one_load() -> Result<()> {
    let cache = Caches::memory().shared::<u32>("test", 10, Duration::from_secs(60));
    let loads = AtomicUsize::new(0);
    let values = futures_util::future::try_join_all(
        (0..20).map(|_| cache.get_or_try_init("key", counted(&loads, 7))),
    )
    .await?;
    assert!(values.iter().all(|value| *value == 7));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(cache.get_or_try_init("key", counted(&loads, 8)).await?, 7);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(cache.inner.stats.joined.load(Ordering::Relaxed), 19);
    Ok(())
}

#[tokio::test]
async fn failed_loads_reach_every_waiter_and_are_not_cached() -> Result<()> {
    let cache = Caches::memory().shared::<u32>("test", 10, Duration::from_secs(60));
    let failing = || async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Err::<u32, _>(anyhow!("source missing"))
    };
    let (first, second) = tokio::join!(
        cache.get_or_try_init("key", failing()),
        cache.get_or_try_init("key", failing()),
    );
    assert!(format!("{:#}", first.unwrap_err()).contains("source missing"));
    assert!(format!("{:#}", second.unwrap_err()).contains("source missing"));
    assert_eq!(cache.get("key").await, None);
    assert_eq!(cache.get_or_try_init("key", async { Ok(3) }).await?, 3);
    Ok(())
}

#[tokio::test]
async fn a_waiter_takes_over_a_cancelled_load() -> Result<()> {
    let cache = Caches::memory().shared::<u32>("test", 10, Duration::from_secs(60));
    let leader = tokio::spawn({
        let cache = cache.clone();
        async move { cache.get_or_try_init("key", std::future::pending()).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let waiter = tokio::spawn({
        let cache = cache.clone();
        async move { cache.get_or_try_init("key", async { Ok(5) }).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    leader.abort();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), waiter).await???,
        5
    );
    assert_eq!(cache.get("key").await, Some(5));
    Ok(())
}

#[tokio::test]
async fn local_caches_hold_values_that_cannot_leave_the_process() {
    type Lock = Arc<tokio::sync::Mutex<()>>;
    let cache = Caches::memory().local::<Lock>("test", 10, Duration::from_secs(60));
    let first = cache
        .get_or_try_init("actor", async { Ok(Lock::default()) })
        .await
        .unwrap();
    let second = cache
        .get_or_try_init("actor", async { Ok(Lock::default()) })
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn unreachable_redis_misses_instead_of_failing() -> Result<()> {
    let caches = Caches::redis(&RedisConfig {
        url: "redis://127.0.0.1:1".into(),
        ca_cert: None,
    })?;
    let cache = caches.shared::<u32>("test", 10, Duration::from_secs(60));
    let started = std::time::Instant::now();
    assert_eq!(cache.get("key").await, None);
    assert_eq!(cache.get_or_try_init("key", async { Ok(4) }).await?, 4);
    cache.invalidate("key").await;
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(cache.inner.stats.errors.load(Ordering::Relaxed) >= 3);
    let last_error = cache.inner.stats.last_error.lock().unwrap().clone();
    assert!(
        last_error
            .as_deref()
            .unwrap_or_default()
            .contains("skipped"),
        "{last_error:?}"
    );
    caches.check().await;
    Ok(())
}

#[test]
fn slow_replies_keep_redis_while_it_still_answers() {
    let failure = |message: &str, slow| redis::Failure {
        error: anyhow!("{message}"),
        slow,
    };
    let busy = redis::Liveness::new(std::time::Instant::now());
    busy.answered();
    assert!(!busy.down(&failure("timed out", true)));
    assert!(busy.down(&failure("connection refused", false)));

    let silent = redis::Liveness::new(std::time::Instant::now() - Duration::from_secs(10));
    assert!(silent.down(&failure("timed out", true)));
}

#[test]
fn redis_rejects_a_malformed_url() {
    for url in ["http://127.0.0.1:6379", "not a url"] {
        assert!(
            Caches::redis(&RedisConfig {
                url: url.into(),
                ca_cert: None,
            })
            .is_err(),
            "{url}"
        );
    }
}

#[tokio::test]
async fn redis_caches_are_shared_between_replicas() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let config = RedisConfig { url, ca_cert: None };
    let (first, second) = (Caches::redis(&config)?, Caches::redis(&config)?);
    let ttl = Duration::from_secs(60);
    let (one, two) = (
        first.shared::<Vec<String>>("test", 10, ttl),
        second.shared::<Vec<String>>("test", 10, ttl),
    );
    let key = uuid::Uuid::new_v4().to_string();
    assert_eq!(two.get(&key).await, None);
    let loads = AtomicUsize::new(0);
    let values = futures_util::future::try_join_all(
        (0..20).map(|_| one.get_or_try_init(&key, counted(&loads, vec!["a".to_owned()]))),
    )
    .await?;
    assert!(values.iter().all(|value| value == &["a"]));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(two.get(&key).await, Some(vec!["a".to_owned()]));
    two.invalidate(&key).await;
    assert_eq!(one.get(&key).await, None);

    let brief = first.shared::<u32>("test", 10, Duration::from_millis(100));
    brief.insert(&key, 1).await;
    assert_eq!(brief.get(&key).await, Some(1));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(brief.get(&key).await, None);
    for cache in [&one.inner.stats, &two.inner.stats, &brief.inner.stats] {
        assert_eq!(cache.errors.load(Ordering::Relaxed), 0);
    }
    Ok(())
}
