use super::*;

#[test]
fn sandbox_credentials_cover_ownership_and_read_only_customer_code() {
    let config = crate::bucket::PersistenceConfig::Replicated {
        durability: crate::bucket::Durability::Zonal,
        replicas: vec![],
    };
    let value = boundary("authority", &config, Some("code"));
    let rules = value["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(
        rules[0]["availableResource"],
        "//storage.googleapis.com/projects/_/buckets/authority"
    );
    assert!(
        rules[0]["availabilityCondition"]["expression"]
            .as_str()
            .unwrap()
            .contains("/owners/")
    );
    assert_eq!(
        rules[1]["availableResource"],
        "//storage.googleapis.com/projects/_/buckets/code"
    );
    assert_eq!(
        rules[1]["availablePermissions"],
        json!(["inRole:roles/storage.objectViewer"])
    );
}

#[tokio::test(start_paused = true)]
async fn refreshes_idle_tokens_before_expiry_without_blocking_readers() -> Result<()> {
    let (tokens, mut exchange, clock) = token_fixture();
    complete_exchange(&mut exchange, Ok(storage_token(&clock, "first", 600))).await;

    tokio::time::advance(Duration::from_secs(480)).await;
    let refresh = next_exchange(&mut exchange).await;
    assert_eq!(ready_token(&tokens).await?.access_token, "first");
    refresh
        .send(Ok(storage_token(&clock, "second", 600)))
        .unwrap();
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(121)).await;
    assert_eq!(ready_token(&tokens).await?.access_token, "second");
    assert!(exchange.try_recv().is_err());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retries_failed_refreshes_while_preserving_a_usable_token() -> Result<()> {
    let (tokens, mut exchange, clock) = token_fixture();
    complete_exchange(&mut exchange, Ok(storage_token(&clock, "first", 300))).await;

    tokio::time::advance(Duration::from_secs(180)).await;
    complete_exchange(&mut exchange, Err(anyhow::anyhow!("STS unavailable"))).await;
    assert_eq!(ready_token(&tokens).await?.access_token, "first");
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(exchange.try_recv().is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    complete_exchange(&mut exchange, Ok(storage_token(&clock, "second", 300))).await;
    assert_eq!(ready_token(&tokens).await?.access_token, "second");
    Ok(())
}

type ExchangeRequest = tokio::sync::oneshot::Sender<Result<StorageToken>>;

struct TestTokenSource(tokio::sync::mpsc::UnboundedSender<ExchangeRequest>);

#[async_trait::async_trait]
impl StorageTokenSource for TestTokenSource {
    async fn exchange(&self) -> Result<StorageToken> {
        let (send, receive) = tokio::sync::oneshot::channel();
        self.0.send(send)?;
        receive.await?
    }
}

struct TestClock(tokio::time::Instant);

impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1_000_000 + self.0.elapsed().as_millis() as u64)
    }
}

fn token_fixture() -> (
    StorageTokens,
    tokio::sync::mpsc::UnboundedReceiver<ExchangeRequest>,
    Arc<TestClock>,
) {
    let (send, receive) = tokio::sync::mpsc::unbounded_channel();
    let clock = Arc::new(TestClock(tokio::time::Instant::now()));
    let tokens = StorageTokens::new(Arc::new(TestTokenSource(send)), clock.clone());
    (tokens, receive, clock)
}

fn storage_token(clock: &TestClock, name: &str, lifetime_secs: u64) -> StorageToken {
    StorageToken {
        access_token: name.into(),
        expires_at_ms: clock.now_ms().unwrap() + lifetime_secs * 1_000,
    }
}

async fn complete_exchange(
    exchange: &mut tokio::sync::mpsc::UnboundedReceiver<ExchangeRequest>,
    token: Result<StorageToken>,
) {
    next_exchange(exchange).await.send(token).unwrap();
    tokio::task::yield_now().await;
}

async fn next_exchange(
    exchange: &mut tokio::sync::mpsc::UnboundedReceiver<ExchangeRequest>,
) -> ExchangeRequest {
    tokio::time::timeout(Duration::from_secs(1), exchange.recv())
        .await
        .expect("token exchange should start promptly")
        .unwrap()
}

async fn ready_token(tokens: &StorageTokens) -> Result<StorageToken> {
    tokio::time::timeout(Duration::from_secs(1), tokens.issue()).await?
}
