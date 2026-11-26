use std::sync::Arc;

use anyhow::{Context, Result};

use super::*;
use crate::{
    bucket::{Bucket, FileBucket, RuntimeStorageReader},
    clock::{Clock, SystemClock},
    host::HostId,
    postgres::{PostgresDatabase, testing::with_postgres},
};

#[tokio::test]
async fn postgres_records_replace_only_at_their_version() -> Result<()> {
    with_postgres(async |fixture| {
        let store = PostgresOwnership::new(PostgresDatabase::connect(&fixture.url).await?);
        let mut record = record(&actor(), 1, false);
        assert!(store.replace(&record, None).await?);
        assert!(!store.replace(&record, None).await?);
        let stored = store.load(&actor()).await?.context("record missing")?;
        assert_eq!(stored.version, Version::Postgres(1));
        assert_eq!(store.unsealed().await?.len(), 1);
        record.sealed = true;
        assert!(store.replace(&record, Some(stored.version)).await?);
        assert!(!store.replace(&record, Some(stored.version)).await?);
        assert!(store.unsealed().await?.is_empty());
        assert!(
            store
                .replace(&record, Some(Version::Bucket(1)))
                .await
                .is_err()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_claim_moves_a_bucket_record_behind_a_marker() -> Result<()> {
    with_postgres(async |fixture| {
        let f = Moving::new(&fixture.url).await?;
        assert!(f.bucket.replace(&record(&actor(), 3, true), None).await?);
        let claim = f.claim("next").await?;
        assert_eq!(claim.record.epoch, 4);
        let stored = f.postgres.load(&actor()).await?.context("not moved")?;
        assert_eq!(stored.record.lease.session_id, "next");
        let error = f
            .bucket
            .load(&actor())
            .await
            .err()
            .context("bucket still owns")?;
        assert!(error.to_string().contains("moved to PostgreSQL"));
        assert!(!f.bucket.replace(&record(&actor(), 9, true), None).await?);
        assert!(matches!(f.claim_outcome("late").await?, Claim::Held));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_new_actor_is_fenced_in_the_bucket_before_postgres_takes_it() -> Result<()> {
    with_postgres(async |fixture| {
        let f = Moving::new(&fixture.url).await?;
        assert_eq!(f.claim("first").await?.record.epoch, 1);
        assert!(!f.bucket.replace(&record(&actor(), 1, false), None).await?);
        assert!(f.postgres.load(&actor()).await?.is_some());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn backfill_moves_ended_leases_and_leaves_live_ones() -> Result<()> {
    with_postgres(async |fixture| {
        let f = Moving::new(&fixture.url).await?;
        let mut live = actor();
        live.actor_id = "live".into();
        let mut record_live = record(&live, 2, false);
        record_live.lease.expires_at_ms = SystemClock.now_ms()? + 60_000;
        assert!(f.bucket.replace(&record_live, None).await?);
        assert!(f.bucket.replace(&record(&actor(), 5, true), None).await?);
        assert_eq!(f.moving.backfill().await?, 1);
        assert_eq!(
            f.postgres
                .load(&actor())
                .await?
                .context("not moved")?
                .record
                .epoch,
            5
        );
        assert!(f.postgres.load(&live).await?.is_none());
        assert_eq!(
            f.bucket
                .load(&live)
                .await?
                .context("live lost")?
                .record
                .epoch,
            2
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn backfill_finishes_interrupted_moves_and_counts_unreadable_records() -> Result<()> {
    with_postgres(async |fixture| {
        let f = Moving::new(&fixture.url).await?;
        assert!(
            f.bucket
                .mark_moved(&record(&actor(), 4, true), None)
                .await?
        );
        let mut broken = actor();
        broken.actor_id = "broken".into();
        let key = crate::storage_paths::owner(&broken.storage_key())?;
        let garbage = crate::payload::encode(&serde_json::json!({"movedToPostgres": "old"}))?;
        assert!(f.files.compare_and_swap(&key, None, garbage).await?);
        assert_eq!(f.moving.backfill().await?, 1);
        let stored = f
            .postgres
            .load(&actor())
            .await?
            .context("move not finished")?;
        assert_eq!(stored.record.epoch, 4);
        Ok(())
    })
    .await
}

/// A cluster moving from a bucket to PostgreSQL, with direct access to both stores.
struct Moving {
    _directory: tempfile::TempDir,
    files: Arc<FileBucket>,
    bucket: BucketOwnership,
    postgres: PostgresOwnership,
    moving: Arc<MovingOwnership>,
    ownerships: Ownerships,
}

impl Moving {
    async fn new(url: &str) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let files = Arc::new(FileBucket::new(directory.path().into())?);
        let database = PostgresDatabase::connect(url).await?;
        let moving = Arc::new(MovingOwnership::new(
            PostgresOwnership::new(database.clone()),
            BucketOwnership::new(files.clone()),
            Arc::new(SystemClock),
        ));
        let reader = RuntimeStorageReader::new(
            files.clone(),
            Arc::new(SystemClock),
            Arc::new(crate::litestream::EmbeddedRestore),
        )?;
        Ok(Self {
            _directory: directory,
            bucket: BucketOwnership::new(files.clone()),
            files,
            postgres: PostgresOwnership::new(database),
            ownerships: Ownerships::new(moving.clone(), Arc::new(reader)),
            moving,
        })
    }

    async fn claim_outcome(&self, session: &str) -> Result<Claim> {
        self.ownerships
            .claim(
                &actor(),
                &HostId::new(format!("host-{session}")),
                session,
                "us-east",
                None,
            )
            .await
    }

    async fn claim(&self, session: &str) -> Result<ActorClaim> {
        match self.claim_outcome(session).await? {
            Claim::Won(claim) => Ok(claim),
            Claim::Held => anyhow::bail!("actor is held"),
        }
    }
}

fn actor() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

fn record(actor: &ActorKey, epoch: u64, sealed: bool) -> Ownership {
    Ownership {
        persistence: crate::bucket::SnapshotStoreConfig::Local,
        sealed,
        lease: HostLease {
            id: HostId::new("host-old"),
            session_id: "old".into(),
            route: "http://old".into(),
            expires_at_ms: 0,
        },
        mutation: String::new(),
        actor: actor.clone(),
        epoch,
        region: "us-east".into(),
        base: None,
        checkpoint_progress: None,
    }
}
