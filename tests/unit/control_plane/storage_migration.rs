use super::*;
use crate::{
    actor::ActorKey,
    bucket::{FileBucket, RapidBucket, RapidPlacement, owner_record},
};
use std::{collections::BTreeMap, sync::Mutex};

const CODE: &[u8] = b"export class Counter {}";
const ARTIFACT: &str =
    "durable-actors/v3/artifacts/00000000-0000-4000-8000-000000000001/actors.mjs";

#[tokio::test]
async fn dormant_installations_move_once_and_reruns_change_nothing() -> Result<()> {
    let f = Fixture::new().await?;
    f.owner("one", &source(), 10).await?;
    f.owner("two", &source(), 10).await?;
    f.owner("done", &target(), 10).await?;
    let report = f.migration().run(false).await?;
    assert_eq!(
        report,
        MigrationReport {
            dry_run: false,
            archive_objects: 2,
            deployments_rebased: 1,
            owners_current: 1,
            owners_rewritten: 2,
        }
    );
    for id in ["one", "two"] {
        assert_eq!(f.owner_migration(id).await?, OwnerMigration::Current);
    }
    let bundle = ActorBundle::decode(&f.deployments.0.lock().unwrap()["project"])?;
    assert_eq!(bundle.bucket, "code-dual");
    assert_eq!(
        bundle.files[0].generation,
        f.target_code.get(ARTIFACT).await?.unwrap().generation
    );
    let rerun = f.migration().run(false).await?;
    assert_eq!(
        (
            rerun.owners_rewritten,
            rerun.deployments_rebased,
            rerun.owners_current
        ),
        (0, 0, 3)
    );
    Ok(())
}

#[tokio::test]
async fn dry_runs_report_without_writing() -> Result<()> {
    let f = Fixture::new().await?;
    f.owner("one", &source(), 10).await?;
    let report = f.migration().run(true).await?;
    assert_eq!(
        (report.owners_rewritten, report.deployments_rebased),
        (1, 1)
    );
    assert!(matches!(
        f.owner_migration("one").await?,
        OwnerMigration::Rewrite(_)
    ));
    assert_eq!(
        ActorBundle::decode(&f.deployments.0.lock().unwrap()["project"])?.bucket,
        "code"
    );
    Ok(())
}

#[tokio::test]
async fn live_actors_or_an_incomplete_copy_stop_the_migration_before_any_write() -> Result<()> {
    let f = Fixture::new().await?;
    f.owner("dormant", &source(), 10).await?;
    f.owner("live", &source(), 30).await?;
    assert!(f.migration().run(false).await.is_err());
    assert!(matches!(
        f.owner_migration("dormant").await?,
        OwnerMigration::Rewrite(_)
    ));

    let f = Fixture::new().await?;
    f.owner("dormant", &source(), 10).await?;
    f.source_archive
        .compare_and_swap("not-copied", None, "x".into())
        .await?;
    assert!(f.migration().run(false).await.is_err());
    assert_eq!(
        ActorBundle::decode(&f.deployments.0.lock().unwrap()["project"])?.bucket,
        "code"
    );
    Ok(())
}

#[tokio::test]
async fn copied_code_must_match_the_deployment_digest() -> Result<()> {
    let f = Fixture::new().await?;
    let changed = f.target_code.get(ARTIFACT).await?.unwrap();
    f.target_code
        .compare_and_swap(ARTIFACT, Some(changed.generation), "tampered".into())
        .await?;
    assert!(f.migration().run(false).await.is_err());
    Ok(())
}

fn rapid(
    buckets: &[(&str, &str)],
    archive: &str,
    placement: RapidPlacement,
) -> SnapshotStoreConfig {
    SnapshotStoreConfig::Rapid {
        buckets: buckets
            .iter()
            .map(|(bucket, zone)| RapidBucket {
                bucket: (*bucket).into(),
                zone: (*zone).into(),
            })
            .collect(),
        archive_bucket: archive.into(),
        archive_batch: Default::default(),
        placement,
    }
}

fn source() -> SnapshotStoreConfig {
    rapid(&[("rapid-a", "us-west4-a")], "archive", RapidPlacement::All)
}

fn target() -> SnapshotStoreConfig {
    rapid(
        &[("rapid-a", "us-west4-a"), ("rapid-w1", "us-west1-a")],
        "archive-dual",
        RapidPlacement::Nearest,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    authority: Arc<FileBucket>,
    source_archive: Arc<FileBucket>,
    target_archive: Arc<FileBucket>,
    target_code: Arc<FileBucket>,
    deployments: Arc<MemoryDeployments>,
}

impl Fixture {
    /// An archive already copied object-for-object and one deployment whose code was copied.
    async fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = |name: &str| -> Result<Arc<FileBucket>> {
            let path = directory.path().join(name);
            std::fs::create_dir_all(&path)?;
            Ok(Arc::new(FileBucket::new(path)?))
        };
        let (authority, source_archive, target_archive, target_code) = (
            bucket("authority")?,
            bucket("archive")?,
            bucket("archive-dual")?,
            bucket("code-dual")?,
        );
        for archive in [&source_archive, &target_archive] {
            for key in [
                "durable-actors-v3-snapshots-a~1.json",
                "durable-actors-v3-snapshots-a~1.json.checkpoint",
            ] {
                archive.compare_and_swap(key, None, "state".into()).await?;
            }
        }
        target_code
            .compare_and_swap(ARTIFACT, None, CODE.into())
            .await?;
        let bundle = ActorBundle {
            bucket: "code".into(),
            files: vec![BundleFile {
                path: "actors.mjs".into(),
                object: ARTIFACT.into(),
                generation: 7,
                sha256: URL_SAFE_NO_PAD.encode(digest(&SHA256, CODE).as_ref()),
            }],
        };
        let deployments = Arc::new(MemoryDeployments(Mutex::new(BTreeMap::from([(
            "project".to_owned(),
            bundle.encode()?,
        )]))));
        Ok(Self {
            _directory: directory,
            authority,
            source_archive,
            target_archive,
            target_code,
            deployments,
        })
    }

    fn migration(&self) -> StorageMigration {
        StorageMigration {
            buckets: MigrationBuckets {
                authority: self.authority.clone(),
                source_archive: self.source_archive.clone(),
                target_archive: self.target_archive.clone(),
                target_code: self.target_code.clone(),
            },
            deployments: self.deployments.clone(),
            clock: Arc::new(FixedClock(20)),
            plan: MigrationPlan {
                source_archive: "archive".into(),
                source_artifact: "code".into(),
                target_artifact: "code-dual".into(),
                target: target(),
            },
        }
    }

    async fn owner(
        &self,
        id: &str,
        persistence: &SnapshotStoreConfig,
        expires_at_ms: u64,
    ) -> Result<()> {
        let record = owner_record(&actor(id), persistence, expires_at_ms);
        let key = crate::storage_paths::owner(&actor(id).storage_key())?;
        ensure!(self.authority.compare_and_swap(&key, None, record).await?);
        Ok(())
    }

    async fn owner_migration(&self, id: &str) -> Result<OwnerMigration> {
        let key = crate::storage_paths::owner(&actor(id).storage_key())?;
        let record = self.authority.get(&key).await?.context("owner missing")?;
        migrate_owner(&record.bytes, "archive", &target(), 20)
    }
}

fn actor(id: &str) -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: id.into(),
    }
}

struct FixedClock(u64);

impl Clock for FixedClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0)
    }
}

struct MemoryDeployments(Mutex<BTreeMap<String, String>>);

#[async_trait]
impl DeploymentStore for MemoryDeployments {
    async fn code_snapshots(&self) -> Result<Vec<(String, String)>> {
        Ok(self.0.lock().unwrap().clone().into_iter().collect())
    }

    async fn replace_code_snapshot(
        &self,
        project: &str,
        current: &str,
        replacement: &str,
    ) -> Result<bool> {
        let mut deployments = self.0.lock().unwrap();
        if deployments.get(project).map(String::as_str) != Some(current) {
            return Ok(false);
        }
        deployments.insert(project.into(), replacement.into());
        Ok(true)
    }
}
