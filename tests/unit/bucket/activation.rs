use super::*;
use crate::{
    bucket::{FileBucket, SnapshotStore, SnapshotStoreConfig, recovery::CheckpointProgress},
    clock::Clock,
    host_leases::HostLeaseRequest,
    ownership::{ActorClaim, BucketOwnership, Claim, OwnershipHint, Ownerships, SessionCheckpoint},
    placement::ObjectPlacementStore,
    state_log::StateSnapshot,
    state_transport::SnapshotWriter,
    storage::{StateStream, snapshot_object_name},
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Past the lease of a claim made at the fixture's start.
const EXPIRED: u64 = 1000 + crate::host_leases::MAX_HOST_LEASE_DURATION_MS;

#[tokio::test]
async fn state_writes_require_local_ownership_and_a_matching_lease() -> Result<()> {
    let f = Fixture::new()?;
    let active = f.activate("first").await?;
    let lease = active.placement.lease;
    let mut other_host = lease.clone();
    other_host.id = HostId::new("other-host");
    let mut other_session = lease.clone();
    other_session.session_id = "other-session".into();
    for mismatched in [other_host, other_session] {
        let error = f
            .runtime
            .prepare_actor_write(&f.actor, &mismatched, 1, 1)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "actor ownership changed");
    }
    let mut other_actor = f.actor.clone();
    other_actor.actor_id = "not-activated".into();
    let error = f
        .runtime
        .prepare_actor_write(&other_actor, &lease, 1, 1)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "actor is not locally activated");
    Ok(())
}

#[tokio::test]
async fn sqlite_dependencies_must_exist_in_the_snapshot_backend_before_publication() -> Result<()> {
    use crate::litestream::{
        Litestream, Replicator,
        storage::{CaptureDirectory, SqliteCapture},
    };
    use crate::state_log::SqliteSnapshot;

    let directory = tempfile::tempdir()?;
    let snapshots = Arc::new(FileBucket::new(directory.path().join("snapshots"))?);
    let f = Fixture::new()?.with_persistence(
        SnapshotStoreConfig::Local,
        Arc::new(crate::bucket::BucketSnapshots(snapshots.clone())),
    )?;
    let active = f.activate("first").await?;
    let first_plan = f
        .runtime
        .prepare_actor_write(&f.actor, &active.placement.lease, 1, 1)
        .await?;
    let next_plan = f
        .runtime
        .prepare_actor_write(&f.actor, &active.placement.lease, 1, 2)
        .await?;
    let replication = Arc::new(Litestream::default());
    let mut capture =
        SqliteCapture::new(CaptureDirectory::temporary()?, replication.clone(), None).await?;
    let database = rusqlite::Connection::open(capture.path())?;
    database.execute("INSERT INTO __terse_fields VALUES ('seed', '1')", [])?;
    let initial = replication.sync(&capture.path()).await?;
    let first = StateSnapshot::new(
        1,
        1,
        "sql".into(),
        SqliteSnapshot {
            txid: initial,
            parent: None,
            files: capture.capture(initial).await?,
        },
        serde_json::Value::Null,
    )?;
    let first_bytes = first.encode()?;
    database.execute("INSERT INTO __terse_fields VALUES ('count', '1')", [])?;
    let changed = replication.sync(&capture.path()).await?;
    let next = StateSnapshot::new(
        2,
        1,
        "fields".into(),
        SqliteSnapshot {
            txid: changed,
            parent: Some(first_plan.stream.snapshot(&first_bytes)?),
            files: capture.capture(changed).await?,
        },
        serde_json::Value::Null,
    )?;
    assert!(
        f.runtime
            .write_snapshot(&next_plan, next.encode()?)
            .await
            .is_err()
    );
    assert!(snapshots.get(&next_plan.object_name).await?.is_none());
    f.runtime.write_snapshot(&first_plan, first_bytes).await?;
    f.runtime.write_snapshot(&next_plan, next.encode()?).await?;
    assert!(f.bucket.get(&first_plan.object_name).await?.is_none());
    assert_eq!(
        snapshots.get(&next_plan.object_name).await?.unwrap().bytes,
        next.encode()?
    );
    Ok(())
}

#[tokio::test]
async fn ownership_and_state_can_use_independent_backends() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let snapshots = Arc::new(FileBucket::new(directory.path().to_owned())?);
    let f = Fixture::new()?.with_persistence(
        SnapshotStoreConfig::Local,
        Arc::new(crate::bucket::BucketSnapshots(snapshots.clone())),
    )?;
    let active = f.activate("first").await?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &active.placement.lease, 1, 1)
        .await?;
    let bytes = state(1, 1, 42)?;
    f.runtime.write_snapshot(&plan, bytes.clone()).await?;
    assert!(f.bucket.get(&plan.object_name).await?.is_none());
    assert_eq!(
        snapshots.get(&plan.object_name).await?.unwrap().bytes,
        bytes
    );
    f.clock.0.store(EXPIRED, Ordering::SeqCst);
    let resumed = f.activate("next").await?;
    assert_eq!(resumed.state.unwrap().bytes, bytes);
    Ok(())
}

#[tokio::test]
async fn hosts_reload_only_the_session_they_adopted() -> Result<()> {
    let f = Fixture::new()?;
    let lease = HostLease {
        id: request("first").id,
        session_id: "first".into(),
        route: String::new(),
        expires_at_ms: EXPIRED,
    };
    assert!(
        f.runtime
            .activate_actor(&f.actor, &lease, "us-east")
            .await
            .is_err()
    );
    let activation = f.activate("first").await?;
    let writes = f.bucket.writes.load(Ordering::SeqCst);
    let restored = f
        .runtime
        .activate_actor(&f.actor, &activation.placement.lease, "us-east")
        .await?;
    assert_eq!(restored.placement, activation.placement);
    assert_eq!(f.bucket.writes.load(Ordering::SeqCst), writes);
    let mut replacement = activation.placement.lease.clone();
    replacement.session_id = "replacement".into();
    assert!(
        f.runtime
            .activate_actor(&f.actor, &replacement, "us-east")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn same_named_actors_in_different_projects_recover_independent_state() -> Result<()> {
    let mut f = Fixture::new()?;
    let mut committed = Vec::new();
    for (project, count) in [("team-a", 1), ("team-b", 3)] {
        f.actor.project_id = project.into();
        let placement = f.activate(&format!("old-{project}")).await?.placement;
        let plan = f
            .runtime
            .prepare_actor_write(&f.actor, &placement.lease, placement.owner_epoch, 1)
            .await?;
        let bytes = state(1, placement.owner_epoch, count)?;
        f.runtime.write_snapshot(&plan, bytes.clone()).await?;
        committed.push((f.actor.clone(), bytes));
    }
    f.clock.0.store(EXPIRED, Ordering::SeqCst);
    for (actor, bytes) in committed {
        f.actor = actor;
        let restored = f.activate(&format!("new-{}", f.actor.project_id)).await?;
        assert_eq!(restored.placement.state_version, 1);
        assert_eq!(restored.state.unwrap().bytes, bytes);
    }
    Ok(())
}

#[tokio::test]
async fn a_new_actor_is_claimed_with_one_read_and_one_write() -> Result<()> {
    let f = Fixture::new()?;
    assert!(
        f.ownerships
            .get_owner(&f.actor.storage_key())
            .await?
            .is_none()
    );
    let reads = f.bucket.reads.load(Ordering::SeqCst);
    let activation = f.activate("first").await?;
    assert_eq!(activation.placement.owner_epoch, 1);
    assert_eq!(activation.placement.lease.expires_at_ms, EXPIRED);
    assert!(activation.placement.lease.route.is_empty());
    assert!(activation.state.is_none());
    assert_eq!(f.bucket.reads.load(Ordering::SeqCst), reads + 1);
    assert_eq!(f.bucket.writes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn snapshot_writes_need_one_bucket_write_and_no_ownership_read() -> Result<()> {
    use crate::state_transport::StateWrite;
    let f = Fixture::new()?;
    let activation = f.activate("first").await?.placement;
    for version in 1..=3 {
        let plan = f
            .runtime
            .prepare_actor_write(&f.actor, &activation.lease, activation.owner_epoch, version)
            .await?;
        let bytes = state(version, activation.owner_epoch, version)?;
        let reads = f.bucket.reads.load(Ordering::SeqCst);
        let writes = f.bucket.writes.load(Ordering::SeqCst);
        assert_eq!(
            f.runtime.write_snapshot(&plan, bytes.clone()).await?,
            StateWrite::Written
        );
        assert_eq!(f.bucket.reads.load(Ordering::SeqCst), reads);
        assert_eq!(f.bucket.writes.load(Ordering::SeqCst), writes + 1);
        assert_eq!(
            f.bucket.inner.get(&plan.object_name).await?.unwrap().bytes,
            bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_claims_have_one_winner() -> Result<()> {
    let f = Fixture::new()?;
    let (a, b) = tokio::join!(f.claim("left"), f.claim("right"));
    let winners: Vec<_> = [a?, b?]
        .into_iter()
        .filter_map(|claim| match claim {
            Claim::Won(claim) => Some(claim),
            Claim::Held => None,
        })
        .collect();
    assert_eq!(winners.len(), 1);
    assert!(matches!(f.claim("late").await?, Claim::Held));
    assert_eq!(
        f.ownerships
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap(),
        winners[0].record.placement()
    );
    Ok(())
}

#[tokio::test]
async fn renewal_release_and_takeover_share_the_actor_record() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.won("first").await?;
    assert!(matches!(f.claim("second").await?, Claim::Held));
    f.clock.0.store(2000, Ordering::SeqCst);
    let renewed = f.ownerships.renew(&f.actor, &first).await?;
    assert_eq!(renewed.expires_at_ms, 12_000);
    assert_eq!(renewed.route, first.route);
    assert_eq!(f.owner().await?.lease, renewed);
    f.ownerships
        .release(&f.actor, &first.id, &first.session_id, None)
        .await?;
    assert!(f.ownerships.renew(&f.actor, &first).await.is_err());
    let second = f.won("second").await?;
    assert_eq!(second.record.epoch, 2);
    f.ownerships
        .release(&f.actor, &first.id, &first.session_id, None)
        .await?;
    assert_eq!(f.owner().await?, second.record.placement());
    Ok(())
}

#[tokio::test]
async fn a_route_is_set_once_and_expired_leases_cannot_renew() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.won("first").await?;
    f.ownerships.renew(&f.actor, &first).await?;
    let mut moved = first.clone();
    moved.route = "http://elsewhere".into();
    assert!(f.ownerships.renew(&f.actor, &moved).await.is_err());
    f.clock.0.store(EXPIRED, Ordering::SeqCst);
    assert!(f.ownerships.renew(&f.actor, &first).await.is_err());
    let elsewhere = f
        .ownerships
        .claim(&f.actor, &request("second").id, "second", "us-west", None)
        .await;
    assert!(elsewhere.is_err());
    assert_eq!(f.won("second").await?.record.epoch, 2);
    Ok(())
}

#[tokio::test]
async fn releasing_every_lease_lets_another_host_take_over_at_once() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.won("first").await?;
    f.ownerships.release_leases().await?;
    assert!(f.ownerships.renew(&f.actor, &first).await.is_err());
    assert_eq!(f.won("second").await?.record.epoch, 2);
    Ok(())
}

#[tokio::test]
async fn ambiguous_claim_and_renewal_responses_reconcile_the_exact_record() -> Result<()> {
    let f = Fixture::new()?;
    f.bucket.lose_reply.store(true, Ordering::SeqCst);
    assert_eq!(f.won("first").await?.record.epoch, 1);
    f.clock.0.store(2000, Ordering::SeqCst);
    f.bucket.lose_reply.store(true, Ordering::SeqCst);
    assert_eq!(
        f.ownerships
            .renew(&f.actor, &request("first"))
            .await?
            .expires_at_ms,
        12_000
    );
    assert_eq!(f.owner().await?.owner_epoch, 1);
    Ok(())
}

#[tokio::test]
async fn delayed_renewal_cannot_overwrite_a_completed_takeover() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.won("first").await?;
    f.clock.0.store(2000, Ordering::SeqCst);
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let takeover = async {
        entered.acquire().await?.forget();
        f.clock.0.store(EXPIRED, Ordering::SeqCst);
        let result = f.won("next").await;
        resume.add_permits(1);
        result
    };
    let (renewed, claimed) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(f.ownerships.renew(&f.actor, &first), takeover)
    })
    .await?;
    assert!(renewed.is_err());
    assert_eq!(claimed?.record.epoch, 2);
    assert_eq!(f.owner().await?.owner, request("next").id);
    Ok(())
}

#[tokio::test]
async fn release_retries_a_concurrent_lease_renewal() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.won("first").await?;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let release = f
        .ownerships
        .release(&f.actor, &first.id, &first.session_id, None);
    let renewal = async {
        entered.acquire().await?.forget();
        f.ownerships.renew(&f.actor, &first).await?;
        resume.add_permits(1);
        anyhow::Ok(())
    };
    let (released, renewed) = tokio::join!(release, renewal);
    renewed?;
    released?;
    assert_eq!(f.owner().await?.lease.expires_at_ms, 0);
    Ok(())
}

#[tokio::test]
async fn a_stale_hint_rereads_and_preserves_the_current_owner() -> Result<()> {
    for active in [true, false] {
        let f = Fixture::new()?;
        f.activate("first").await?;
        f.seal("first").await?;
        let hint = f.hint().await?;
        let second = f.activate("second").await?;
        let plan = f
            .runtime
            .prepare_actor_write(&f.actor, &second.placement.lease, 2, 1)
            .await?;
        let bytes = state(1, 2, 42)?;
        f.runtime.write_snapshot(&plan, bytes.clone()).await?;
        if !active {
            f.seal("second").await?;
        }
        let claimed = f
            .ownerships
            .claim(&f.actor, &request("next").id, "next", "us-east", Some(hint))
            .await?;
        match claimed {
            Claim::Held => {
                assert!(active);
                assert_eq!(f.owner().await?, second.placement);
            }
            Claim::Won(claim) => {
                assert!(!active);
                assert_eq!(claim.record.epoch, 3);
                let loaded = f.runtime.adopt(&claim, &CachedBase::default()).await?;
                assert_eq!(loaded.state.unwrap().bytes, bytes);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn clean_shutdown_reactivates_from_the_checkpoint_without_listing_or_session_reads()
-> Result<()> {
    for written in [true, false] {
        let f = Fixture::new()?;
        let loaded = f.activate("first").await?;
        let bytes = StateSnapshot::new(
            2,
            1,
            "committed".into(),
            crate::test_sqlite::snapshot(serde_json::json!({"count": 42}))?,
            serde_json::json!(42),
        )?
        .encode()?;
        if written {
            for version in [2, 1] {
                let plan = f
                    .runtime
                    .prepare_actor_write(&f.actor, &loaded.placement.lease, 1, version)
                    .await?;
                let snapshot = StateSnapshot::new(
                    version,
                    1,
                    "committed".into(),
                    StateSnapshot::decode(&bytes)?.sqlite,
                    serde_json::json!(42),
                )?
                .encode()?;
                f.runtime.write_snapshot(&plan, snapshot).await?;
            }
        }
        f.seal("first").await?;
        for (epoch, session) in [(2, "next"), (3, "again")] {
            let hint = f.hint().await?;
            assert!(hint.sealed());
            f.bucket.reads.store(0, Ordering::SeqCst);
            f.bucket.lists.store(0, Ordering::SeqCst);
            let claim = match f
                .ownerships
                .claim(
                    &f.actor,
                    &request(session).id,
                    session,
                    "us-east",
                    Some(hint),
                )
                .await?
            {
                Claim::Won(claim) => claim,
                Claim::Held => anyhow::bail!("sealed actor was held"),
            };
            let loaded = f.runtime.adopt(&claim, &CachedBase::default()).await?;
            assert_eq!(loaded.placement.owner_epoch, epoch);
            assert_eq!(
                loaded.state.as_ref().map(|snapshot| &snapshot.bytes),
                written.then_some(&bytes)
            );
            assert_eq!(f.bucket.lists.load(Ordering::SeqCst), 0);
            assert_eq!(f.bucket.reads.load(Ordering::SeqCst), u64::from(written));
            f.seal(session).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_parked_base_activates_without_reading_its_snapshot() -> Result<()> {
    let f = Fixture::new()?;
    let loaded = f.activate("first").await?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &loaded.placement.lease, 1, 1)
        .await?;
    f.runtime.write_snapshot(&plan, state(1, 1, 42)?).await?;
    f.seal("first").await?;
    let claim = f.won("next").await?;
    let base = claim.record.base.clone().unwrap();
    f.bucket.reads.store(0, Ordering::SeqCst);

    let cached = CachedBase {
        parked: Some(base.clone()),
    };
    let loaded = f.runtime.adopt(&claim, &cached).await?;

    assert!(loaded.state.is_none());
    assert_eq!(loaded.head, Some(base.clone()));
    assert_eq!(loaded.placement.state_version, base.state_version);
    assert_eq!(f.bucket.reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn rapid_resume_retains_the_durable_base_across_epochs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let snapshots = Arc::new(FileBucket::new(directory.path().to_owned())?);
    let f = Fixture::new()?.with_persistence(
        rapid_policy(),
        Arc::new(crate::bucket::BucketSnapshots(snapshots.clone())),
    )?;
    let loaded = f.activate("first").await?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &loaded.placement.lease, 1, 1)
        .await?;
    f.runtime.write_snapshot(&plan, state(1, 1, 42)?).await?;
    f.seal("first").await?;
    let resumed = f.activate("next").await?;
    let stored = snapshots
        .get(&plan.object_name)
        .await?
        .context("durable base missing")?;
    assert_eq!(resumed.placement.owner_epoch, 2);
    assert_eq!(
        resumed.state.as_ref().map(|snapshot| &snapshot.bytes),
        Some(&stored.bytes)
    );
    Ok(())
}

#[tokio::test]
async fn crash_recovery_persists_the_selected_snapshot_before_claiming_the_next_epoch() -> Result<()>
{
    let f = Fixture::new()?;
    let snapshots = Arc::new(RejectingSnapshots {
        inner: crate::bucket::BucketSnapshots(f.bucket.clone()),
        reject: AtomicBool::new(false),
    });
    let f = f.with_persistence(rapid_policy(), snapshots.clone())?;
    let first = f.activate("first").await?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &first.placement.lease, 1, 1)
        .await?;
    let bytes = state(1, 1, 42)?;
    f.runtime.write_snapshot(&plan, bytes.clone()).await?;
    f.clock.0.store(EXPIRED, Ordering::SeqCst);
    snapshots.reject.store(true, Ordering::SeqCst);
    assert!(f.claim("next").await.is_err());
    assert_eq!(f.owner().await?.owner_epoch, 1);
    snapshots.reject.store(false, Ordering::SeqCst);
    let resumed = f.activate("next").await?;
    assert_eq!(resumed.placement.owner_epoch, 2);
    assert_eq!(
        resumed.state.as_ref().map(|snapshot| &snapshot.bytes),
        Some(&bytes)
    );
    Ok(())
}

#[tokio::test]
async fn a_sealed_predecessor_resumes_its_checkpoint_progress() -> Result<()> {
    let (f, snapshots) = sealed_predecessor().await?;
    let loaded = f.activate("next").await?;
    assert_eq!(loaded.placement.owner_epoch, 2);
    assert_eq!(loaded.placement.state_version, 1);
    assert_eq!(snapshots.starts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_failed_download_is_released_without_losing_the_durable_base() -> Result<()> {
    let (f, snapshots) = sealed_predecessor().await?;
    snapshots.reject_head.store(true, Ordering::SeqCst);
    let claim = f.won("failed").await?;
    let failed = f.runtime.adopt(&claim, &CachedBase::default()).await;
    assert_eq!(failed.err().unwrap().to_string(), "head download failed");
    assert_eq!(snapshots.starts.load(Ordering::SeqCst), 0);
    let unchanged = SessionCheckpoint {
        snapshot: claim.record.base.clone(),
        progress: claim.progress,
    };
    let lease = &claim.record.lease;
    f.ownerships
        .release(&f.actor, &lease.id, &lease.session_id, Some(unchanged))
        .await?;
    let released = f.won("next").await?;
    assert_eq!(released.record.epoch, 3);
    assert_eq!(released.record.base, claim.record.base);
    assert_eq!(released.progress.map(|p| p.log_bytes), Some(1234));
    Ok(())
}

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

struct CountedBucket {
    inner: FileBucket,
    reads: AtomicU64,
    writes: AtomicU64,
    lists: AtomicU64,
    lose_reply: AtomicBool,
    delay_write: Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}

#[async_trait]
impl Bucket for CountedBucket {
    async fn range(
        &self,
        key: &str,
        start: u64,
        length: u64,
    ) -> Result<Option<crate::bucket::BucketObject>> {
        let Some(mut object) = self.get(key).await? else {
            return Ok(None);
        };
        let end = start
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("range overflow"))?;
        anyhow::ensure!(end <= object.bytes.len() as u64, "incomplete range");
        object.bytes = object.bytes.slice(start as usize..end as usize);
        Ok(Some(object))
    }

    async fn get(&self, key: &str) -> Result<Option<super::super::BucketObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Payload,
    ) -> Result<bool> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let delayed = self.delay_write.lock().unwrap().take();
        if let Some((entered, resume)) = delayed {
            entered.add_permits(1);
            resume.acquire().await?.forget();
        }
        let written = self.inner.compare_and_swap(key, generation, bytes).await?;
        ensure!(
            !self.lose_reply.swap(false, Ordering::SeqCst),
            "CAS response lost"
        );
        Ok(written)
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix).await
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    bucket: Arc<CountedBucket>,
    clock: Arc<TestClock>,
    runtime: RuntimeStorage,
    ownerships: Ownerships,
    actor: ActorKey,
}

impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(CountedBucket {
            inner: FileBucket::new(directory.path().into())?,
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            lose_reply: AtomicBool::new(false),
            delay_write: Mutex::new(None),
        });
        let clock = Arc::new(TestClock(AtomicU64::new(1000)));
        let runtime = RuntimeStorage::new(bucket.clone(), clock.clone())?;
        let ownerships = Ownerships::new(
            Arc::new(BucketOwnership::new(bucket.clone())),
            Arc::new(RuntimeStorageReader::new(
                bucket.clone(),
                clock.clone(),
                Arc::new(crate::litestream::EmbeddedRestore),
            )?),
        );
        Ok(Self {
            _directory: directory,
            bucket,
            clock,
            runtime,
            ownerships,
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
            },
        })
    }

    fn with_persistence(
        mut self,
        config: SnapshotStoreConfig,
        snapshots: Arc<dyn SnapshotStore>,
    ) -> Result<Self> {
        self.runtime = self
            .runtime
            .with_persistence(config.clone(), snapshots.clone())?;
        self.ownerships = Ownerships::new(
            Arc::new(BucketOwnership::new(self.bucket.clone())),
            Arc::new(
                RuntimeStorageReader::new(
                    self.bucket.clone(),
                    self.clock.clone(),
                    Arc::new(crate::litestream::EmbeddedRestore),
                )?
                .with_persistence(config, snapshots)?,
            ),
        );
        Ok(self)
    }

    async fn claim(&self, session: &str) -> Result<Claim> {
        self.ownerships
            .claim(&self.actor, &request(session).id, session, "us-east", None)
            .await
    }

    async fn won(&self, session: &str) -> Result<ActorClaim> {
        match self.claim(session).await? {
            Claim::Won(claim) => Ok(claim),
            Claim::Held => anyhow::bail!("previous owner lease is still active"),
        }
    }

    async fn activate(&self, session: &str) -> Result<LoadedActor> {
        let claim = self.won(session).await?;
        self.runtime.adopt(&claim, &CachedBase::default()).await
    }

    /// Ends the session as its host does: drains, then releases with the checkpoint.
    async fn seal(&self, session: &str) -> Result<()> {
        let host = request(session).id;
        let checkpoint = self
            .runtime
            .drain_activation(
                &self.actor,
                &host,
                session,
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await?;
        self.ownerships
            .release(&self.actor, &host, session, Some(checkpoint))
            .await
    }

    async fn owner(&self) -> Result<ObjectPlacement> {
        self.ownerships
            .get_owner(&self.actor.storage_key())
            .await?
            .context("actor has no owner")
    }

    async fn hint(&self) -> Result<OwnershipHint> {
        let (_, hint) = self
            .ownerships
            .get_owner_with_hint(&self.actor.storage_key())
            .await?;
        hint.context("actor has no owner")
    }
}

fn request(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new(format!("host-{session}")),
        session_id: session.into(),
        route: format!("http://{session}"),
        duration_ms: 10_000,
    }
}

fn state(version: u64, epoch: u64, count: u64) -> Result<Payload> {
    StateSnapshot::new(
        version,
        epoch,
        format!("write-{version}"),
        crate::test_sqlite::snapshot(serde_json::json!({"count": count}))?,
        serde_json::json!(count),
    )?
    .encode()
}

fn rapid_policy() -> SnapshotStoreConfig {
    serde_json::from_value(
        serde_json::json!({"type":"rapid", "archive_bucket":"archive-test", "buckets":[
            {"bucket":"rapid-test-a", "zone":"us-west4-a"},
            {"bucket":"rapid-test-b", "zone":"us-west4-b"}
        ]}),
    )
    .unwrap()
}

struct RejectingSnapshots {
    inner: crate::bucket::BucketSnapshots,
    reject: AtomicBool,
}

#[async_trait]
impl SnapshotStore for RejectingSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Payload>> {
        self.inner.get(object).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Payload)>> {
        self.inner.latest(prefix).await
    }
    async fn put(&self, object: &str, bytes: Payload) -> Result<()> {
        ensure!(
            !self.reject.load(Ordering::SeqCst),
            "snapshot persistence unavailable"
        );
        self.inner.put(object, bytes).await
    }
}

struct CountedSnapshots {
    inner: crate::bucket::BucketSnapshots,
    starts: AtomicU64,
    reject_head: AtomicBool,
}

#[async_trait]
impl SnapshotStore for CountedSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Payload>> {
        ensure!(
            !self.reject_head.load(Ordering::SeqCst),
            "head download failed"
        );
        self.inner.get(object).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Payload)>> {
        self.inner.latest(prefix).await
    }
    async fn put(&self, object: &str, bytes: Payload) -> Result<()> {
        self.inner.put(object, bytes).await
    }
    async fn start(
        &self,
        stream: &StateStream,
        resume: Option<(&SnapshotRef, CheckpointProgress)>,
    ) -> Result<()> {
        assert_eq!(
            resume.map(|(head, p)| (head.state_version, p.log_bytes)),
            (stream.base_version > 0).then_some((stream.base_version, 1234))
        );
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// An actor whose first session sealed at version 1 with checkpoint progress.
async fn sealed_predecessor() -> Result<(Fixture, Arc<CountedSnapshots>)> {
    let f = Fixture::new()?;
    let snapshots = Arc::new(CountedSnapshots {
        inner: crate::bucket::BucketSnapshots(f.bucket.clone()),
        starts: AtomicU64::new(0),
        reject_head: AtomicBool::new(false),
    });
    let f = f.with_persistence(SnapshotStoreConfig::Local, snapshots.clone())?;
    f.won("first").await?;
    let snapshot = StateSnapshot::new(
        1,
        1,
        "committed".into(),
        crate::test_sqlite::snapshot(serde_json::json!({"count":42}))?,
        serde_json::json!(42),
    )?;
    let bytes = snapshot.encode()?;
    let object = snapshot_object_name(&f.actor, 1, &format!("{:032x}", 1))?;
    SnapshotStore::put(&snapshots.inner, &object, bytes.clone()).await?;
    let first = request("first");
    f.ownerships
        .release(
            &f.actor,
            &first.id,
            &first.session_id,
            Some(SessionCheckpoint {
                snapshot: Some(SnapshotRef::new(object, &snapshot, &bytes)?),
                progress: Some(CheckpointProgress {
                    log_bytes: 1234,
                    ..Default::default()
                }),
            }),
        )
        .await?;
    Ok((f, snapshots))
}
