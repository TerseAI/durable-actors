use super::*;

#[test]
fn recovery_ignores_an_unfinished_append_without_losing_the_previous_commit() -> Result<()> {
    let first = frame(1, b"first")?;
    let second = frame(2, b"second")?;
    for cut in 0..second.len() {
        let mut bytes = first.to_vec();
        bytes.extend_from_slice(&second[..cut]);
        let recovered = records(&bytes)?;
        assert_eq!(recovered, vec![(1, Bytes::from_static(b"first"))]);
    }
    let mut bytes = first.to_vec();
    bytes.extend_from_slice(&second);
    assert_eq!(records(&bytes)?.last(), Some(&(2, Bytes::from_static(b"second"))));
    Ok(())
}

#[test]
fn corruption_of_a_complete_record_fails_recovery() -> Result<()> {
    let mut bytes = frame(1, b"first")?.to_vec();
    bytes[HEADER] ^= 1;
    assert!(records(&bytes).is_err());
    Ok(())
}

#[test]
fn replay_rejects_conflicting_duplicate_versions() -> Result<()> {
    let mut bytes = frame(1, b"first")?.to_vec();
    bytes.extend_from_slice(&frame(1, b"different")?);
    assert!(records(&bytes).is_err());
    Ok(())
}

#[test]
fn repeated_identical_append_is_idempotent() -> Result<()> {
    let mut bytes = frame(1, b"first")?.to_vec();
    bytes.extend_from_slice(&bytes.clone());
    assert_eq!(records(&bytes)?, vec![(1, Bytes::from_static(b"first"))]);
    Ok(())
}

struct MemoryLog {
    bytes: Bytes,
    read: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl LogReader for MemoryLog {
    fn size(&self) -> u64 { self.bytes.len() as u64 }
    async fn range(&self, start: u64, length: u64) -> Result<Bytes> {
        self.read.fetch_add(length as usize, std::sync::atomic::Ordering::SeqCst);
        Ok(self.bytes.slice(start as usize..(start + length) as usize))
    }
}

#[tokio::test]
async fn activation_reads_the_tail_without_replaying_old_states() -> Result<()> {
    let mut bytes = Vec::new();
    for version in 1..=32 { bytes.extend_from_slice(&frame(version, &vec![b'x'; 128 * 1024])?); }
    bytes.extend_from_slice(&frame(33, b"latest")?);
    let log = MemoryLog { bytes: bytes.into(), read: 0.into() };
    assert_eq!(last_record(&log).await?, Some((33, Bytes::from_static(b"latest"))));
    assert!(log.read.load(std::sync::atomic::Ordering::SeqCst) < 128 * 1024);
    Ok(())
}

#[tokio::test]
async fn tail_recovery_ignores_each_possible_partial_frame() -> Result<()> {
    let first = frame(1, b"first")?;
    let next = frame(2, b"second")?;
    for cut in 0..next.len() {
        let mut bytes = first.to_vec(); bytes.extend_from_slice(&next[..cut]);
        let log = MemoryLog { bytes: bytes.into(), read: 0.into() };
        assert_eq!(last_record(&log).await?, Some((1, Bytes::from_static(b"first"))));
    }
    Ok(())
}

#[tokio::test]
async fn complete_corruption_is_not_treated_as_an_unacknowledged_tail() -> Result<()> {
    let mut bytes = frame(1, b"first")?.to_vec();
    bytes.extend_from_slice(&frame(2, b"second")?);
    let damaged = frame(1, b"first")?.len() + HEADER;
    bytes[damaged] ^= 1;
    let log = MemoryLog { bytes: bytes.into(), read: 0.into() };
    assert!(last_record(&log).await.is_err());
    Ok(())
}
