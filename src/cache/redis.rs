use super::Store;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use failsafe::{
    backoff::{self, Constant},
    failure_policy::{ConsecutiveFailures, consecutive_failures},
    futures::CircuitBreaker,
};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub struct RedisConfig {
    pub url: String,
    pub ca_cert: Option<String>,
}

/// Bump the version when a value's encoding changes.
const KEY_PREFIX: &str = "durable-actors:v1";
/// Generous: under a burst the control plane, not Redis, is slow, and a late reply beats a miss.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(1);
const BACKOFF: Duration = Duration::from_secs(1);
/// A slow reply while others still arrive is load, not an outage.
const SILENCE: Duration = Duration::from_secs(2);

type Breaker = failsafe::StateMachine<ConsecutiveFailures<Constant>, ()>;

#[derive(Clone)]
pub(crate) struct Redis {
    client: redis::Client,
    connection: ConnectionManager,
    breaker: Breaker,
    liveness: Arc<Liveness>,
}

impl Redis {
    pub(super) fn connect(config: &RedisConfig) -> Result<Self> {
        let client = match &config.ca_cert {
            Some(ca) => redis::Client::build_with_tls(
                config.url.as_str(),
                redis::TlsCertificates {
                    client_tls: None,
                    root_cert: Some(ca.as_bytes().to_vec()),
                },
            ),
            None => redis::Client::open(config.url.as_str()),
        }
        .context("configure Redis client")?;
        let connection = ConnectionManager::new_lazy_with_config(
            client.clone(),
            ConnectionManagerConfig::new()
                .set_connection_timeout(Some(Duration::from_secs(1)))
                .set_response_timeout(Some(COMMAND_TIMEOUT))
                .set_number_of_retries(1),
        )
        .context("configure Redis connection")?;
        Ok(Self {
            client,
            connection,
            breaker: failsafe::Config::new()
                .failure_policy(consecutive_failures(1, backoff::constant(BACKOFF)))
                .build(),
            liveness: Arc::new(Liveness::new(Instant::now())),
        })
    }

    pub(super) fn store<V>(&self, name: &str, ttl: Duration) -> RedisStore<V> {
        RedisStore {
            redis: self.clone(),
            prefix: format!("{KEY_PREFIX}:{name}:"),
            ttl_ms: ttl.as_millis().max(1) as u64,
            value: PhantomData,
        }
    }

    pub(super) async fn ping(&self) -> Result<()> {
        self.run::<String>(redis::cmd("PING")).await.map(drop)
    }

    pub(crate) async fn pubsub(&self) -> Result<redis::aio::PubSub> {
        tokio::time::timeout(Duration::from_secs(5), self.client.get_async_pubsub())
            .await
            .context("Redis pub/sub connection timed out")?
            .context("open Redis pub/sub connection")
    }

    pub(crate) async fn run<T: redis::FromRedisValue>(&self, command: redis::Cmd) -> Result<T> {
        let mut connection = self.connection.clone();
        let attempt = async {
            match tokio::time::timeout(COMMAND_TIMEOUT, command.query_async(&mut connection)).await
            {
                Ok(Ok(value)) => {
                    self.liveness.answered();
                    Ok(value)
                }
                Ok(Err(error)) => Err(Failure {
                    slow: error.is_timeout(),
                    error: error.into(),
                }),
                Err(_) => Err(Failure {
                    slow: true,
                    error: anyhow!("Redis did not answer within {COMMAND_TIMEOUT:?}"),
                }),
            }
        };
        self.breaker
            .call_with(|failure: &Failure| self.liveness.down(failure), attempt)
            .await
            .map_err(|error| match error {
                failsafe::Error::Inner(failure) => failure.error,
                failsafe::Error::Rejected => anyhow!("Redis skipped after a recent outage"),
            })
    }
}

pub(super) struct Failure {
    pub error: anyhow::Error,
    pub slow: bool,
}

pub(super) struct Liveness {
    started: Instant,
    answered_ms: AtomicU64,
}

impl Liveness {
    pub(super) fn new(started: Instant) -> Self {
        Self {
            started,
            answered_ms: AtomicU64::new(0),
        }
    }

    pub(super) fn answered(&self) {
        self.answered_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    pub(super) fn down(&self, failure: &Failure) -> bool {
        let silent_ms = (self.started.elapsed().as_millis() as u64)
            .saturating_sub(self.answered_ms.load(Ordering::Relaxed));
        !failure.slow || silent_ms >= SILENCE.as_millis() as u64
    }
}

pub(super) struct RedisStore<V> {
    redis: Redis,
    prefix: String,
    ttl_ms: u64,
    value: PhantomData<fn() -> V>,
}

impl<V> RedisStore<V> {
    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

#[async_trait]
impl<V> Store<V> for RedisStore<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&self, key: &str) -> Result<Option<V>> {
        let value: Option<Vec<u8>> = self
            .redis
            .run(redis::cmd("GET").arg(self.key(key)).clone())
            .await?;
        value
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .context("decode cached value")
    }

    async fn insert(&self, key: &str, value: &V) -> Result<()> {
        let mut command = redis::cmd("SET");
        command
            .arg(self.key(key))
            .arg(serde_json::to_vec(value)?)
            .arg("PX")
            .arg(self.ttl_ms);
        self.redis.run(command).await
    }

    async fn invalidate(&self, key: &str) -> Result<()> {
        self.redis
            .run::<i64>(redis::cmd("DEL").arg(self.key(key)).clone())
            .await
            .map(drop)
    }
}
