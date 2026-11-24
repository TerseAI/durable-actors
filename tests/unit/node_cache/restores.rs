use super::*;
use crate::{
    bucket::{BaseName, Bucket, BucketObject, FileBucket, RestoredSqlite},
    payload::Payload,
    state_log::{SqliteSnapshot, StateSnapshot},
};
use serde_json::json;

/// A store whose history starts from one base, with no changes after it.
struct FromBase(SqliteBase);

#[async_trait::async_trait]
impl crate::bucket::SnapshotStore for FromBase {
    async fn get(&self, _: &str) -> Result<Option<Payload>> {
        Ok(None)
    }
    async fn restore_sqlite(
        &self,
        _: VerifiedSnapshot,
        _: Option<&Path>,
    ) -> Result<RestoredSqlite> {
        Ok(RestoredSqlite {
            sqlite: SqliteSnapshot {
                txid: self.0.name.txid,
                parent: None,
                files: Vec::new(),
            },
            base: Some(self.0.clone()),
        })
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn latest(&self, _: &str) -> Result<Option<(String, Payload)>> {
        Ok(None)
    }
    async fn put(&self, _: &str, _: Payload) -> Result<()> {
        Ok(())
    }
}

/// A bucket that holds nothing, so any download from it fails.
struct Empty;

#[async_trait::async_trait]
impl Bucket for Empty {
    async fn get(&self, _: &str) -> Result<Option<BucketObject>> {
        Ok(None)
    }
    async fn range(&self, _: &str, _: u64, _: u64) -> Result<Option<BucketObject>> {
        Ok(None)
    }
    async fn compare_and_swap(&self, _: &str, _: Option<i64>, _: Payload) -> Result<bool> {
        Ok(false)
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

fn database(directory: &Path, rows: i64) -> Result<Vec<u8>> {
    let path = directory.join("source.sqlite");
    let db = rusqlite::Connection::open(&path)?;
    db.execute_batch("PRAGMA journal_mode = DELETE; CREATE TABLE t(v INTEGER);")?;
    for row in 0..rows {
        db.execute("INSERT INTO t VALUES (?)", [row])?;
    }
    drop(db);
    let bytes = std::fs::read(&path)?;
    std::fs::remove_file(path)?;
    Ok(bytes)
}

fn base(key: &str, bytes: u64, bucket: Arc<dyn Bucket>) -> SqliteBase {
    SqliteBase {
        key: key.into(),
        name: BaseName {
            owner_epoch: 1,
            version: 1,
            txid: 1,
            bytes,
        },
        bucket,
        downloaded: None,
    }
}

fn head() -> Result<VerifiedSnapshot> {
    let sqlite = crate::test_sqlite::snapshot(json!({}))?;
    let bytes = StateSnapshot::new(1, 1, "request".into(), sqlite, json!(1))?.encode()?;
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "id".into(),
    };
    let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
    VerifiedSnapshot::named(object, bytes)
}

fn prepared(key: &str, file: PathBuf) -> Prepared {
    Prepared {
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "id".into(),
        },
        key: key.into(),
        file,
        started: Instant::now(),
        task: AbortOnDropHandle::new(tokio::spawn(async { Ok(()) })),
    }
}

#[tokio::test]
async fn restores_onto_the_prepared_base_without_downloading_it_again() -> Result<()> {
    let slot = tempfile::tempdir()?;
    let bytes = database(slot.path(), 3)?;
    let file = slot.path().join(PREPARED_DIR).join(PREPARED_FILE);
    std::fs::create_dir_all(file.parent().unwrap())?;
    std::fs::write(&file, &bytes)?;
    let store = FromBase(base("base", bytes.len() as u64, Arc::new(Empty)));
    let database = slot.path().join(DATABASE);
    let (txid, base_bytes) = restore_into(
        &store,
        head()?,
        Some(prepared("base", file.clone())),
        &database,
    )
    .await?;
    assert_eq!((txid, base_bytes), (1, Some(bytes.len() as u64)));
    assert_eq!(std::fs::read(database.join(DATABASE_FILE))?, bytes);
    assert!(!file.exists());
    Ok(())
}

#[tokio::test]
async fn downloads_another_base_when_the_history_starts_from_it() -> Result<()> {
    let slot = tempfile::tempdir()?;
    let bucket = Arc::new(FileBucket::new(slot.path().join("bucket"))?);
    let bytes = database(slot.path(), 5)?;
    assert!(
        bucket
            .compare_and_swap("newer", None, bytes.clone().into())
            .await?
    );
    let stale = slot.path().join(PREPARED_DIR).join(PREPARED_FILE);
    std::fs::create_dir_all(stale.parent().unwrap())?;
    std::fs::write(&stale, b"an older base")?;
    let store = FromBase(base("newer", bytes.len() as u64, bucket));
    let database = slot.path().join(DATABASE);
    restore_into(&store, head()?, Some(prepared("older", stale)), &database).await?;
    assert_eq!(std::fs::read(database.join(DATABASE_FILE))?, bytes);
    Ok(())
}
