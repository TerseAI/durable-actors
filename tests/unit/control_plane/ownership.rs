use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use crate::payload::Payload;
use anyhow::Result;

use super::*;
use crate::{
    bucket::{
        ActivationAuthority, FileBucket, RuntimeStorage, RuntimeStorageReader, SessionCheckpoint,
    },
    clock::SystemClock,
    host::HostId,
    host_leases::HostLeaseRequest,
    placement::ObjectPlacementStore,
};

struct TestClock(AtomicU64);

impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

/// Loses the response of a compare-and-swap after applying it, when asked to.
struct LossyBucket {
    inner: FileBucket,
    lose_reply: AtomicBool,
}

#[async_trait]
impl Bucket for LossyBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.inner.get(key).await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        self.inner.range(key, start, length).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Payload,
    ) -> Result<bool> {
        let written = self.inner.compare_and_swap(key, generation, bytes).await?;
        anyhow::ensure!(
            !self.lose_reply.swap(false, Ordering::SeqCst),
            "CAS response lost"
        );
        Ok(written)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
}

struct Harness {
    _directory: tempfile::TempDir,
    bucket: Arc<LossyBucket>,
    clock: Arc<TestClock>,
    url: String,
}

impl Harness {
    fn store(&self) -> Result<PostgresOwnership> {
        Ok(PostgresOwnership::new(
            PostgresDatabase::lazy(&self.url)?,
            self.bucket.clone(),
            self.clock.clone(),
        ))
    }

    fn control_plane(&self) -> Result<RuntimeStorageReader> {
        Ok(RuntimeStorageReader::new(
            self.bucket.clone(),
            self.clock.clone(),
            Arc::new(crate::litestream::EmbeddedRestore),
        )?
        .with_ownership(Arc::new(self.store()?)))
    }

    /// A host or control plane that keeps ownership in the bucket.
    fn legacy(&self) -> Result<RuntimeStorage> {
        RuntimeStorage::new(self.bucket.clone(), self.clock.clone())
    }

    async fn rows(&self) -> Result<i64> {
        Ok(self
            .store()?
            .database
            .query_one("SELECT count(*) FROM actor_ownership", &[])
            .await?
            .get(0))
    }

    async fn sealed(&self, actor: &ActorKey) -> Result<bool> {
        let record = self
            .store()?
            .select(&owner(actor))
            .await?
            .context("no PostgreSQL record")?;
        Ok(
            serde_json::from_reader::<_, serde_json::Value>(record.bytes.reader())?["sealed"]
                == true,
        )
    }

    fn advance(&self, ms: u64) {
        self.clock.0.fetch_add(ms, Ordering::SeqCst);
    }
}

async fn with_harness(test: impl AsyncFnOnce(&Harness) -> Result<()>) -> Result<()> {
    crate::postgres::testing::with_postgres(async |database| {
        let directory = tempfile::tempdir()?;
        let harness = Harness {
            bucket: Arc::new(LossyBucket {
                inner: FileBucket::new(directory.path().into())?,
                lose_reply: AtomicBool::new(false),
            }),
            _directory: directory,
            clock: Arc::new(TestClock(AtomicU64::new(1_000))),
            url: database.url.clone(),
        };
        test(&harness).await
    })
    .await
}

fn actor_named(id: &str) -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: id.into(),
    }
}

fn owner(actor: &ActorKey) -> String {
    crate::storage_paths::owner(&actor.storage_key()).unwrap()
}

fn key() -> String {
    owner(&actor_named("one"))
}

fn record(expires_at_ms: u64) -> Payload {
    serde_json::to_vec(&serde_json::json!({
        "actor": actor_named("one"),
        "lease": {"id": "legacy-host", "session_id": "legacy", "route": "http://legacy", "expires_at_ms": expires_at_ms},
        "epoch": 1,
        "mutation": uuid::Uuid::new_v4().to_string(),
    }))
    .unwrap()
    .into()
}

fn moved(record: &Payload) -> Payload {
    serde_json::to_vec(&Moved {
        moved_to_postgres: STANDARD.encode(record.to_bytes().unwrap()),
    })
    .unwrap()
    .into()
}

fn request(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new(format!("host-{session}")),
        session_id: session.into(),
        route: format!("http://{session}"),
        duration_ms: 10_000,
    }
}

#[tokio::test]
async fn replaces_only_at_the_generation_it_read() -> Result<()> {
    with_harness(async |h| {
        let store = h.store()?;
        assert!(store.get(&key()).await?.is_none());
        assert!(store.replace(&key(), None, record(0)).await?);
        assert!(!store.replace(&key(), None, record(0)).await?);
        let created = store.get(&key()).await?.unwrap();
        let next = record(0);
        assert!(
            store
                .replace(&key(), Some(created.generation), next.clone())
                .await?
        );
        assert!(
            !store
                .replace(&key(), Some(created.generation), record(0))
                .await?
        );
        let current = store.get(&key()).await?.unwrap();
        assert_eq!(current.bytes, next);
        assert!(current.generation > created.generation);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_creates_and_replacements_each_have_one_winner() -> Result<()> {
    with_harness(async |h| {
        let (store, key) = (h.store()?, key());
        let winners =
            |results: Vec<Result<bool>>| results.iter().filter(|r| matches!(r, Ok(true))).count();
        let created =
            futures_util::future::join_all((0..8).map(|_| store.replace(&key, None, record(0))))
                .await;
        assert_eq!(winners(created), 1);
        let generation = store.get(&key).await?.unwrap().generation;
        let replaced = futures_util::future::join_all(
            (0..8).map(|_| store.replace(&key, Some(generation), record(0))),
        )
        .await;
        assert_eq!(winners(replaced), 1);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn adopts_a_released_bucket_record_and_marks_it_moved() -> Result<()> {
    with_harness(async |h| {
        let store = h.store()?;
        let released = record(0);
        assert!(
            h.bucket
                .compare_and_swap(&key(), None, released.clone())
                .await?
        );
        let original = h.bucket.get(&key()).await?.unwrap();
        let adopted = store.get(&key()).await?.unwrap();
        assert_eq!(adopted.bytes, released);
        assert!(adopted.generation > original.generation);
        let marker = h.bucket.get(&key()).await?.unwrap();
        assert_eq!(marker.bytes, moved(&released));
        assert!(
            !store
                .replace(&key(), Some(original.generation), record(0))
                .await?
        );
        assert!(
            store
                .replace(&key(), Some(adopted.generation), record(0))
                .await?
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn leaves_a_leased_bucket_record_with_its_host() -> Result<()> {
    with_harness(async |h| {
        let store = h.store()?;
        let leased = record(u64::MAX);
        assert!(
            h.bucket
                .compare_and_swap(&key(), None, leased.clone())
                .await?
        );
        let original = h.bucket.get(&key()).await?.unwrap();
        let read = store.get(&key()).await?.unwrap();
        assert_eq!(
            (read.generation, read.bytes),
            (original.generation, leased.clone())
        );
        assert_eq!(h.bucket.get(&key()).await?.unwrap().bytes, leased);
        assert!(
            !store
                .replace(&key(), Some(original.generation), record(0))
                .await?
        );
        assert_eq!(h.rows().await?, 0);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn finishes_a_move_interrupted_after_the_marker() -> Result<()> {
    with_harness(async |h| {
        let original = record(0);
        assert!(
            h.bucket
                .compare_and_swap(&key(), None, moved(&original))
                .await?
        );
        let marker = h.bucket.get(&key()).await?.unwrap();
        let adopted = h.store()?.get(&key()).await?.unwrap();
        assert_eq!(
            (adopted.generation, adopted.bytes),
            (marker.generation + 1, original)
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_lost_marker_response_still_completes_the_move() -> Result<()> {
    with_harness(async |h| {
        let original = record(0);
        assert!(
            h.bucket
                .compare_and_swap(&key(), None, original.clone())
                .await?
        );
        h.bucket.lose_reply.store(true, Ordering::SeqCst);
        assert_eq!(h.store()?.get(&key()).await?.unwrap().bytes, original);
        assert_eq!(h.bucket.get(&key()).await?.unwrap().bytes, moved(&original));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_moves_agree_on_one_record() -> Result<()> {
    with_harness(async |h| {
        let original = record(0);
        assert!(
            h.bucket
                .compare_and_swap(&key(), None, original.clone())
                .await?
        );
        let (first, second, key) = (h.store()?, h.store()?, key());
        let (first, second) = tokio::join!(first.get(&key), second.get(&key));
        let (first, second) = (first?.unwrap(), second?.unwrap());
        assert_eq!(
            (first.generation, &first.bytes),
            (second.generation, &original)
        );
        assert_eq!(second.bytes, original);
        assert_eq!(h.rows().await?, 1);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn peeking_and_listing_never_move_records() -> Result<()> {
    with_harness(async |h| {
        let store = h.store()?;
        let (in_postgres, in_bucket, interrupted) = (
            owner(&actor_named("postgres")),
            owner(&actor_named("bucket")),
            owner(&actor_named("interrupted")),
        );
        assert!(store.replace(&in_postgres, None, record(0)).await?);
        assert!(
            h.bucket
                .compare_and_swap(&in_bucket, None, record(0))
                .await?
        );
        assert!(
            h.bucket
                .compare_and_swap(&interrupted, None, moved(&record(0)))
                .await?
        );
        let mut expected = vec![in_postgres, in_bucket.clone(), interrupted.clone()];
        expected.sort();
        assert_eq!(store.keys().await?, expected);
        let peeked = store.peek(&in_bucket).await?.unwrap();
        assert_eq!(h.bucket.get(&in_bucket).await?.unwrap().bytes, peeked.bytes);
        assert!(store.peek(&interrupted).await?.is_none());
        assert_eq!(h.rows().await?, 1);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_unreachable_database_fails_instead_of_reading_the_bucket() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(FileBucket::new(directory.path().into())?);
    let store = PostgresOwnership::new(
        PostgresDatabase::lazy("postgresql://postgres@127.0.0.1:1/none?sslmode=disable")?,
        bucket.clone(),
        Arc::new(SystemClock),
    );
    let released = record(0);
    assert!(
        bucket
            .compare_and_swap(&key(), None, released.clone())
            .await?
    );
    assert!(store.get(&key()).await.is_err());
    assert!(store.replace(&key(), None, record(0)).await.is_err());
    assert_eq!(bucket.get(&key()).await?.unwrap().bytes, released);
    Ok(())
}

#[tokio::test]
async fn a_legacy_host_renews_and_releases_in_the_bucket_until_its_actor_moves() -> Result<()> {
    with_harness(async |h| {
        let (actor, legacy_host, first, next) = (
            actor_named("one"),
            h.legacy()?,
            request("legacy"),
            request("next"),
        );
        legacy_host
            .register_activation(&actor, &first, "us-east", true, None)
            .await?;
        let control_plane = h.control_plane()?;
        h.advance(5_000);
        legacy_host.renew_activation(&actor, &first).await?;
        assert!(
            control_plane
                .claim_activation(&actor, &next, "us-east", false, None)
                .await
                .is_err()
        );
        assert_eq!(h.rows().await?, 0);
        legacy_host
            .release_activation(&actor, &first.id, &first.session_id)
            .await?;
        // A restarted control plane keeps no ownership state of its own.
        let control_plane = h.control_plane()?;
        control_plane
            .claim_activation(&actor, &next, "us-east", false, None)
            .await?;
        let (current, _) = control_plane
            .get_owner_with_hint(&actor.storage_key())
            .await?;
        let current = current.context("no owner")?;
        assert_eq!((current.owner_epoch, current.owner), (2, next.id));
        assert_eq!(h.rows().await?, 1);
        assert!(
            h.legacy()?
                .get_owner_with_hint(&actor.storage_key())
                .await
                .is_err(),
            "bucket readers fail on the marker instead of reviving the record"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_activation_racing_the_marker_wins_and_stays_in_the_bucket() -> Result<()> {
    with_harness(async |h| {
        let (actor, first) = (actor_named("one"), request("first"));
        let legacy = h.legacy()?;
        legacy
            .register_activation(&actor, &first, "us-east", true, None)
            .await?;
        legacy
            .release_activation(&actor, &first.id, &first.session_id)
            .await?;
        let read = h.bucket.get(&key()).await?.unwrap();
        h.legacy()?
            .register_activation(&actor, &request("second"), "us-east", false, None)
            .await?;
        let store = h.store()?;
        assert!(
            !store
                .mark_moved(&key(), read.generation, &read.bytes.to_bytes()?)
                .await?
        );
        let current = store.get(&key()).await?.unwrap();
        assert_eq!(h.bucket.get(&key()).await?.unwrap().bytes, current.bytes);
        assert_eq!(h.rows().await?, 0);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_stale_bucket_hint_rereads_and_moves_the_record() -> Result<()> {
    with_harness(async |h| {
        let (actor, first) = (actor_named("one"), request("first"));
        let legacy = h.legacy()?;
        legacy
            .register_activation(&actor, &first, "us-east", true, None)
            .await?;
        let sealed = SessionCheckpoint { snapshot: None };
        legacy
            .release_with_checkpoint(&actor, &first.id, &first.session_id, Some(sealed))
            .await?;
        let (_, hint) = h
            .legacy()?
            .get_owner_with_hint(&actor.storage_key())
            .await?;
        let control_plane = h.control_plane()?;
        control_plane
            .claim_activation(&actor, &request("next"), "us-east", false, hint.as_ref())
            .await?;
        let (current, _) = control_plane
            .get_owner_with_hint(&actor.storage_key())
            .await?;
        assert_eq!(current.context("no owner")?.owner_epoch, 2);
        assert_eq!(h.rows().await?, 1);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_claims_on_a_new_actor_have_one_winner() -> Result<()> {
    with_harness(async |h| {
        let (actor, a, b) = (actor_named("one"), request("a"), request("b"));
        let (first, second) = (h.control_plane()?, h.control_plane()?);
        let (first, second) = tokio::join!(
            first.claim_activation(&actor, &a, "us-east", true, None),
            second.claim_activation(&actor, &b, "us-east", true, None),
        );
        assert!(first.is_ok() != second.is_ok());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_expired_claim_is_reclaimed_and_its_late_host_cannot_renew() -> Result<()> {
    with_harness(async |h| {
        let (actor, first) = (actor_named("one"), request("first"));
        let control_plane = h.control_plane()?;
        control_plane
            .claim_activation(&actor, &first, "us-east", true, None)
            .await?;
        h.advance(10_001);
        control_plane
            .claim_activation(&actor, &request("second"), "us-east", false, None)
            .await?;
        assert!(
            control_plane
                .renew_activation(&actor, &first)
                .await
                .is_err()
        );
        let (current, _) = control_plane
            .get_owner_with_hint(&actor.storage_key())
            .await?;
        assert_eq!(current.context("no owner")?.owner_epoch, 2);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn recovery_seals_abandoned_records_from_either_store_and_moves_bucket_ones() -> Result<()> {
    with_harness(async |h| {
        let (in_postgres, in_bucket, moved_actor) = (
            actor_named("postgres"),
            actor_named("bucket"),
            actor_named("moved"),
        );
        let control_plane = h.control_plane()?;
        control_plane
            .claim_activation(&in_postgres, &request("postgres"), "us-east", true, None)
            .await?;
        h.legacy()?
            .register_activation(&in_bucket, &request("bucket"), "us-east", true, None)
            .await?;
        let legacy = h.legacy()?;
        let released = request("moved");
        legacy
            .register_activation(&moved_actor, &released, "us-east", true, None)
            .await?;
        let sealed = SessionCheckpoint { snapshot: None };
        legacy
            .release_with_checkpoint(
                &moved_actor,
                &released.id,
                &released.session_id,
                Some(sealed),
            )
            .await?;
        control_plane
            .get_owner_with_hint(&moved_actor.storage_key())
            .await?;
        h.advance(11 * 60 * 1000);
        control_plane.seal_abandoned().await?;
        assert!(h.sealed(&in_postgres).await?);
        assert!(h.sealed(&in_bucket).await?);
        assert_eq!(h.rows().await?, 3);
        let keys = [owner(&in_bucket), owner(&moved_actor)];
        let markers =
            futures_util::future::try_join_all(keys.iter().map(|key| h.bucket.get(key))).await?;
        // A recovery pass from before the move cannot parse the markers, so it leaves them alone.
        h.legacy()?.seal_abandoned().await?;
        for (key, marker) in keys.iter().zip(markers) {
            assert_eq!(
                h.bucket.get(key).await?.unwrap().bytes,
                marker.unwrap().bytes
            );
        }
        Ok(())
    })
    .await
}
