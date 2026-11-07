use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::*;
use crate::{bucket::FileBucket, clock::SystemClock};

const KEY: &str = "durable-actors/v3/owners/actor.json";

async fn with_store(
    test: impl AsyncFnOnce(&PostgresOwnership, &FileBucket) -> Result<()>,
) -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        let directory = tempfile::tempdir()?;
        let legacy = Arc::new(FileBucket::new(directory.path().into())?);
        let store = PostgresOwnership::new(
            PostgresDatabase::lazy(&database.url)?,
            legacy.clone(),
            Arc::new(SystemClock),
        );
        test(&store, &legacy).await
    })
    .await
}

fn record(expires_at_ms: u64) -> Bytes {
    serde_json::to_vec(&serde_json::json!({"lease": {"expires_at_ms": expires_at_ms}, "mutation": uuid::Uuid::new_v4().to_string()}))
        .unwrap()
        .into()
}

#[tokio::test]
async fn replaces_only_at_the_generation_it_read() -> Result<()> {
    with_store(async |store, _| {
        assert!(store.get(KEY).await?.is_none());
        assert!(store.replace(KEY, None, record(0)).await?);
        assert!(!store.replace(KEY, None, record(0)).await?);
        let created = store.get(KEY).await?.unwrap();
        let next = record(0);
        assert!(
            store
                .replace(KEY, Some(created.generation), next.clone())
                .await?
        );
        assert!(
            !store
                .replace(KEY, Some(created.generation), record(0))
                .await?
        );
        let current = store.get(KEY).await?.unwrap();
        assert_eq!(current.bytes, next);
        assert!(current.generation > created.generation);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn adopts_a_released_bucket_record_and_marks_it_moved() -> Result<()> {
    with_store(async |store, legacy| {
        let released = record(0);
        assert!(legacy.compare_and_swap(KEY, None, released.clone()).await?);
        let original = legacy.get(KEY).await?.unwrap();
        let adopted = store.get(KEY).await?.unwrap();
        assert_eq!(adopted.bytes, released);
        assert!(adopted.generation > original.generation);
        let marker = legacy.get(KEY).await?.unwrap();
        assert!(serde_json::from_slice::<Moved>(&marker.bytes).is_ok());
        assert!(
            !store
                .replace(KEY, Some(original.generation), record(0))
                .await?
        );
        assert!(
            store
                .replace(KEY, Some(adopted.generation), record(0))
                .await?
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn leaves_a_leased_bucket_record_with_its_host() -> Result<()> {
    with_store(async |store, legacy| {
        let leased = record(u64::MAX);
        assert!(legacy.compare_and_swap(KEY, None, leased.clone()).await?);
        let original = legacy.get(KEY).await?.unwrap();
        let read = store.get(KEY).await?.unwrap();
        assert_eq!(
            (read.generation, read.bytes),
            (original.generation, leased.clone())
        );
        assert_eq!(legacy.get(KEY).await?.unwrap().bytes, leased);
        assert!(
            !store
                .replace(KEY, Some(original.generation), record(0))
                .await?
        );
        Ok(())
    })
    .await
}
