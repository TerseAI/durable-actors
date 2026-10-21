//! Caches the control plane's replicas share, or that one process keeps.
//!
//! [`Caches`] hands out every cache. Without Redis (locally, in tests) every
//! cache lives in this process's memory. With `DURABLE_ACTORS_REDIS_URL` set,
//! shared caches live only in Redis: every replica sees what any one of them
//! loaded or invalidated, and no in-process copy sits in front to go stale.
//! Values that only mean something inside one process, such as a lock, use
//! [`Caches::local`] and always stay in memory.
//!
//! A cache never fails its caller. A Redis error counts as a miss, or a skipped
//! write, so an unreachable Redis costs latency, not availability.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

mod redis;
pub use self::redis::RedisConfig;

/// Where a cache keeps its entries.
#[async_trait]
pub(crate) trait Store<V>: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<V>>;
    async fn insert(&self, key: &str, value: &V) -> Result<()>;
    async fn invalidate(&self, key: &str) -> Result<()>;
}

/// Entries in this process's memory, evicted by age and capacity.
struct MemoryStore<V>(moka::future::Cache<String, V>);

#[async_trait]
impl<V: Clone + Send + Sync + 'static> Store<V> for MemoryStore<V> {
    async fn get(&self, key: &str) -> Result<Option<V>> {
        Ok(self.0.get(key).await)
    }
    async fn insert(&self, key: &str, value: &V) -> Result<()> {
        self.0.insert(key.to_owned(), value.clone()).await;
        Ok(())
    }
    async fn invalidate(&self, key: &str) -> Result<()> {
        self.0.invalidate(key).await;
        Ok(())
    }
}

/// Hands out caches: in memory, or shared through Redis when configured.
#[derive(Clone, Default)]
pub(crate) struct Caches {
    redis: Option<redis::Redis>,
    stats: Arc<Mutex<Vec<Arc<Stats>>>>,
}

impl Caches {
    /// Every cache in this process's memory.
    pub fn memory() -> Self {
        Self::default()
    }

    /// Shared caches in Redis. Connects lazily: the control plane starts while
    /// Redis is unreachable and its caches miss until Redis answers.
    pub fn redis(config: &RedisConfig) -> Result<Self> {
        Ok(Self {
            redis: Some(redis::Redis::connect(config)?),
            ..Self::default()
        })
    }

    /// A cache every replica shares when Redis is configured. Entries expire
    /// `ttl` after they are written. `capacity` bounds the in-memory cache;
    /// Redis evicts by its own `maxmemory` policy.
    pub fn shared<V>(&self, name: &'static str, capacity: u64, ttl: Duration) -> Cache<V>
    where
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        match &self.redis {
            Some(redis) => self.cache(name, "redis", Arc::new(redis.store(name, ttl))),
            None => self.cache(
                name,
                "memory",
                Arc::new(MemoryStore(
                    moka::future::Cache::builder()
                        .max_capacity(capacity)
                        .time_to_live(ttl)
                        .build(),
                )),
            ),
        }
    }

    /// A cache for values that only mean something in this process. Always in
    /// memory; entries expire `idle` after they were last read or written.
    pub fn local<V>(&self, name: &'static str, capacity: u64, idle: Duration) -> Cache<V>
    where
        V: Clone + Send + Sync + 'static,
    {
        self.cache(
            name,
            "memory",
            Arc::new(MemoryStore(
                moka::future::Cache::builder()
                    .max_capacity(capacity)
                    .time_to_idle(idle)
                    .build(),
            )),
        )
    }

    fn cache<V>(
        &self,
        name: &'static str,
        backend: &'static str,
        store: Arc<dyn Store<V>>,
    ) -> Cache<V> {
        let stats = Arc::new(Stats {
            name,
            backend,
            ..Stats::default()
        });
        self.stats.lock().unwrap().push(stats.clone());
        Cache {
            inner: Arc::new(Inner {
                store,
                flights: Mutex::default(),
                stats,
            }),
        }
    }

    /// Logs a `cache_stats` event per active cache every `every`: hits, misses,
    /// loads other callers shared, and store errors since the last report.
    pub fn report(&self, every: Duration, stop: CancellationToken) {
        let stats = self.stats.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(every) => {} }
                let caches = stats.lock().unwrap().clone();
                for cache in caches {
                    cache.report();
                }
            }
        });
    }

    /// Checks Redis once, so a misconfigured URL or credential shows up at
    /// startup rather than as a quiet miss rate.
    pub async fn check(&self) {
        if let Some(redis) = &self.redis {
            match redis.ping().await {
                Ok(()) => tracing::info!("Redis cache connected"),
                Err(error) => tracing::warn!(error = %format!("{error:#}"),
                    "Redis cache unreachable; caches miss until it answers"),
            }
        }
    }
}

/// A cache of `V` by string key. Cloning shares it.
pub(crate) struct Cache<V> {
    inner: Arc<Inner<V>>,
}

impl<V> Clone for Cache<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<V> {
    store: Arc<dyn Store<V>>,
    /// Loads in progress in this process, so concurrent misses on a key load it once.
    flights: Mutex<HashMap<String, Flight<V>>>,
    stats: Arc<Stats>,
}

/// A load's outcome, once it lands. Callers that joined it get the error as text.
type Flight<V> = watch::Receiver<Option<Result<V, String>>>;

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub async fn get(&self, key: &str) -> Option<V> {
        let value = self.read(key).await;
        self.inner.stats.count(if value.is_some() {
            &self.inner.stats.hits
        } else {
            &self.inner.stats.misses
        });
        value
    }

    pub async fn insert(&self, key: &str, value: V) {
        if let Err(error) = self.inner.store.insert(key, &value).await {
            self.inner.stats.failed(error);
        }
    }

    pub async fn invalidate(&self, key: &str) {
        if let Err(error) = self.inner.store.invalidate(key).await {
            self.inner.stats.failed(error);
        }
    }

    /// The cached value, or `init`'s, which is then cached. Concurrent callers
    /// in this process share one `init`; a failed `init` is not cached.
    pub async fn get_or_try_init<F>(&self, key: &str, init: F) -> Result<V>
    where
        F: Future<Output = Result<V>>,
    {
        let stats = &self.inner.stats;
        if let Some(value) = self.read(key).await {
            stats.count(&stats.hits);
            return Ok(value);
        }
        let sender = loop {
            let mut flight = {
                let mut flights = self.inner.flights.lock().unwrap();
                match flights.get(key) {
                    Some(flight) => flight.clone(),
                    None => {
                        let (sender, flight) = watch::channel(None);
                        flights.insert(key.to_owned(), flight);
                        break sender;
                    }
                }
            };
            if let Ok(outcome) = flight.wait_for(Option::is_some).await {
                stats.count(&stats.joined);
                return outcome
                    .clone()
                    .expect("landed")
                    .map_err(|error| anyhow!(error));
            }
            // The caller loading it was cancelled; load it here instead.
        };
        let _land = Land {
            flights: &self.inner.flights,
            key,
            flight: sender.subscribe(),
        };
        // A load may have landed between the read above and this one starting.
        if let Some(value) = self.read(key).await {
            stats.count(&stats.hits);
            sender.send_replace(Some(Ok(value.clone())));
            return Ok(value);
        }
        stats.count(&stats.misses);
        let result = init.await;
        if let Ok(value) = &result {
            self.insert(key, value.clone()).await;
        }
        sender.send_replace(Some(match &result {
            Ok(value) => Ok(value.clone()),
            Err(error) => Err(format!("{error:#}")),
        }));
        result
    }

    async fn read(&self, key: &str) -> Option<V> {
        self.inner.store.get(key).await.unwrap_or_else(|error| {
            self.inner.stats.failed(error);
            None
        })
    }
}

/// Ends a load's flight when the load finishes or is cancelled.
struct Land<'a, V> {
    flights: &'a Mutex<HashMap<String, Flight<V>>>,
    key: &'a str,
    flight: Flight<V>,
}

impl<V> Drop for Land<'_, V> {
    fn drop(&mut self) {
        let mut flights = self.flights.lock().unwrap();
        if flights
            .get(self.key)
            .is_some_and(|flight| flight.same_channel(&self.flight))
        {
            flights.remove(self.key);
        }
    }
}

#[derive(Default)]
struct Stats {
    name: &'static str,
    backend: &'static str,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Callers that waited on another caller's load instead of loading.
    joined: AtomicU64,
    errors: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Stats {
    fn count(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn failed(&self, error: anyhow::Error) {
        self.count(&self.errors);
        *self.last_error.lock().unwrap() = Some(format!("{error:#}"));
    }

    fn report(&self) {
        let take = |counter: &AtomicU64| counter.swap(0, Ordering::Relaxed);
        let (hits, misses, joined, errors) = (
            take(&self.hits),
            take(&self.misses),
            take(&self.joined),
            take(&self.errors),
        );
        if hits + misses + joined + errors == 0 {
            return;
        }
        let last_error = self.last_error.lock().unwrap().take();
        if errors > 0 {
            tracing::warn!(
                event = "cache_stats",
                cache = self.name,
                backend = self.backend,
                hits,
                misses,
                joined,
                errors,
                last_error = last_error.as_deref(),
                "cache store errors; those reads counted as misses"
            );
        } else {
            tracing::info!(
                event = "cache_stats",
                cache = self.name,
                backend = self.backend,
                hits,
                misses,
                joined,
                errors,
                "cache activity"
            );
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/cache.rs"]
mod tests;
