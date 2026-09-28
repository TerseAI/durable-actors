use super::*;

#[test]
fn wal_changes_encode_as_incremental_ltx_and_restore_schema_and_data() -> Result<()> {
    let source = tempfile::tempdir()?;
    let path = source.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB); INSERT INTO entries VALUES (1, zeroblob(2097152));")?;
    let mut capture = SqliteCapture::new()?;
    let first = capture.capture(&wal_state(&path, 0, 2)?)?;
    let mut restored = SqliteCapture::new()?;
    restored.apply(&first)?;
    database.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); UPDATE entries SET value='changed' WHERE id=1; ALTER TABLE entries ADD COLUMN enabled INTEGER DEFAULT 1;")?;
    let second = capture.capture(&wal_state(&path, 2, 4)?)?;
    assert!(second.len() < first.len() / 10);
    restored.apply(&second)?;
    let reopened = rusqlite::Connection::open(restored.path())?;
    assert_eq!(
        reopened.query_row("SELECT value FROM entries", [], |row| row
            .get::<_, String>(0))?,
        "changed"
    );
    assert_eq!(
        reopened.query_row("SELECT enabled FROM entries", [], |row| row
            .get::<_, u32>(0))?,
        1
    );
    assert_eq!(restored.txid(), 4);
    Ok(())
}

#[test]
fn ltx_recovery_rejects_missing_and_corrupt_segments() -> Result<()> {
    let source = tempfile::tempdir()?;
    let path = source.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE entries (value INTEGER);")?;
    let mut capture = SqliteCapture::new()?;
    let first = capture.capture(&wal_state(&path, 0, 1)?)?;
    database.execute_batch("INSERT INTO entries VALUES (1)")?;
    let second = capture.capture(&wal_state(&path, 0, 2)?)?;
    assert!(SqliteCapture::new()?.apply(&second).is_err());
    let mut corrupt = first.clone();
    corrupt[150] ^= 1;
    assert!(SqliteCapture::new()?.apply(&corrupt).is_err());
    let mut restored = SqliteCapture::new()?;
    restored.apply(&first)?;
    assert!(restored.apply(&first).is_err());
    restored.apply(&second)?;
    Ok(())
}

fn wal_state(path: &std::path::Path, base_txid: u64, txid: u64) -> Result<SqliteState> {
    Ok(SqliteState {
        txid,
        path: None,
        wal: Some(SqliteWal {
            base_txid,
            data: base64::engine::general_purpose::STANDARD
                .encode(std::fs::read(format!("{}-wal", path.display()))?),
        }),
    })
}

#[test]
fn rejects_torn_or_corrupt_wal_without_advancing_capture() -> Result<()> {
    let source = tempfile::tempdir()?;
    let path = source.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE entries (value INTEGER)")?;
    let state = wal_state(&path, 0, 1)?;
    let mut capture = SqliteCapture::new()?;
    let bytes = STANDARD.decode(&state.wal.as_ref().unwrap().data)?;
    for corrupt in [bytes[..bytes.len() - 1].to_vec(), {
        let mut bytes = bytes.clone();
        bytes[100] ^= 1;
        bytes
    }] {
        let mut invalid = state.clone();
        invalid.wal.as_mut().unwrap().data = STANDARD.encode(corrupt);
        assert!(capture.capture(&invalid).is_err());
        assert_eq!(capture.txid(), 0);
    }
    capture.capture(&state)?;
    assert_eq!(capture.txid(), 1);
    Ok(())
}

#[tokio::test]
async fn replicas_retain_ltx_dependencies_until_a_compacted_checkpoint() -> Result<()> {
    use crate::replication::{FileReplicaStore, ReplicaStore, ReplicaStream};
    use crate::state_log::{SqliteSnapshot, StateSnapshot};
    let source = tempfile::tempdir()?;
    let path = source.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE entries (value INTEGER)")?;
    let actor = crate::actor::ActorKey {
        project_id: "test".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let stream = ReplicaStream {
        prefix: format!("{}1/", crate::storage_paths::snapshots(&actor)?),
        session: "session".into(),
        owner_epoch: 1,
        base_version: 0,
    };
    let store_path = source.path().join("replica");
    let store = FileReplicaStore::open(store_path.clone(), 8 * 1024 * 1024).await?;
    store.initialize_session(&stream.session).await?;
    let empty = FileReplicaStore::open(source.path().join("empty"), 8 * 1024 * 1024).await?;
    empty.initialize_session(&stream.session).await?;
    let mut capture = SqliteCapture::new()?;
    let mut parent = None;
    let mut first_object = String::new();
    let mut checkpoint = None;
    for txid in 1..=128 {
        if txid > 1 {
            database
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); INSERT INTO entries VALUES (1)")?;
        }
        let ltx = capture.capture(&wal_state(&path, txid - 1, txid)?)?;
        if is_checkpoint(&ltx)? {
            parent = None;
        }
        let mut snapshot = StateSnapshot::new(
            txid,
            1,
            format!("request-{txid}"),
            serde_json::json!({"count": txid}),
            serde_json::Value::Null,
        )?;
        snapshot.sqlite = Some(SqliteSnapshot {
            object: stream.object(txid),
            txid,
            parent: parent.clone(),
            ltx: Some(STANDARD.encode(&ltx)),
        });
        let bytes = snapshot.encode()?;
        if txid == 2 {
            assert!(empty.append(&stream, &bytes).await.is_err());
        }
        store.append(&stream, &bytes).await?;
        if txid == 1 {
            first_object = stream.object(txid);
        }
        if txid == 127 {
            assert!(store.read(&first_object).await?.is_some());
        }
        if txid == 128 {
            assert!(is_checkpoint(&ltx)?);
            assert!(store.read(&first_object).await?.is_none());
            empty.append(&stream, &bytes).await?;
            checkpoint = Some(ltx);
        }
        parent = Some(stream.snapshot(&bytes)?);
    }
    drop(store);
    let reopened = FileReplicaStore::open(store_path, 8 * 1024 * 1024).await?;
    assert_eq!(
        reopened
            .stream_head(&stream)
            .await?
            .latest
            .unwrap()
            .state_version,
        128
    );
    let mut recovered = SqliteCapture::new()?;
    recovered.apply(&checkpoint.unwrap())?;
    assert_eq!(
        rusqlite::Connection::open(recovered.path())?.query_row(
            "SELECT count(*) FROM entries",
            [],
            |row| row.get::<_, u32>(0)
        )?,
        127
    );
    Ok(())
}
