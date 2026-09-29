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

#[test]
fn checkpoints_compact_ltx_history_and_restore_independently() -> Result<()> {
    let source = tempfile::tempdir()?;
    let path = source.path().join("actor.sqlite");
    let database = rusqlite::Connection::open(&path)?;
    database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE entries (value INTEGER)")?;
    let mut capture = SqliteCapture::new()?;
    let mut checkpoint = None;
    for txid in 1..=128 {
        if txid > 1 {
            database
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); INSERT INTO entries VALUES (1)")?;
        }
        let ltx = capture.capture(&wal_state(&path, txid - 1, txid)?)?;
        assert_eq!(is_checkpoint(&ltx)?, txid == 1 || txid == 128);
        checkpoint = Some(ltx);
    }
    let mut recovered = SqliteCapture::new()?;
    recovered.apply(&checkpoint.unwrap())?;
    assert_eq!(recovered.txid(), 128);
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
