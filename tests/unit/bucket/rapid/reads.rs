use super::*;
use crate::bucket::BucketObject;

#[tokio::test]
async fn archived_history_is_read_once_for_successive_snapshot_lookups() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    writer.start(&f.stream).await?;
    for version in 1..=28 {
        writer
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    writer.finish(&f.stream).await?;
    f.cleaned().await?;
    let (reader, bucket) = reader(&f)?;
    for version in (1..28).rev() {
        assert_eq!(
            reader.get(&f.stream.object(version)).await?,
            Some(state(version, 1)?)
        );
    }
    let gets = bucket.gets.lock().unwrap();
    assert!(!gets.contains_key(&f.stream.object(1)));
    let batches: Vec<_> = gets
        .iter()
        .filter(|(key, _)| key.ends_with(".batch"))
        .collect();
    assert!(!batches.is_empty());
    assert!(
        batches.iter().all(|(_, reads)| **reads == 1),
        "batch reads: {batches:?}"
    );
    assert!(
        bucket
            .lists
            .lock()
            .unwrap()
            .values()
            .all(|reads| *reads == 1)
    );
    Ok(())
}

#[tokio::test]
async fn history_reads_discover_new_records_after_a_miss() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    writer.start(&f.stream).await?;
    writer.put(&f.stream.object(1), state(1, 1)?).await?;
    let (reader, _) = reader(&f)?;
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    assert_eq!(reader.get(&f.stream.object(2)).await?, None);
    writer.put(&f.stream.object(2), state(2, 1)?).await?;
    assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    assert_eq!(
        reader.latest(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    reader.recover(&f.stream.prefix).await?;
    assert!(writer.put(&f.stream.object(3), state(3, 1)?).await.is_err());
    Ok(())
}

#[tokio::test]
async fn cached_history_is_scoped_to_the_full_snapshot_object() -> Result<()> {
    let f = Fixture::new()?;
    let first = f.store()?;
    first.start(&f.stream).await?;
    for version in 1..=3 {
        first
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    first.finish(&f.stream).await?;
    f.cleaned().await?;
    let (reader, _) = reader(&f)?;
    assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    for (id, epoch) in [("one", 2), ("another", 1)] {
        let mut actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
        actor.actor_id = id.into();
        let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{epoch:032x}"))?;
        let stream = StateStream {
            prefix: object.strip_suffix("1.json").unwrap().into(),
            owner_epoch: epoch,
            ..f.stream.clone()
        };
        let writer = f.store()?;
        writer.start(&stream).await?;
        let mut snapshot = StateSnapshot::decode(&state(1, epoch)?)?;
        snapshot.result = serde_json::json!(id);
        let bytes: Bytes = snapshot.encode()?.into();
        writer.put(&stream.object(1), bytes.clone()).await?;
        assert_eq!(reader.get(&stream.object(1)).await?, Some(bytes));
    }
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

#[tokio::test]
async fn corrupt_history_is_not_cached_and_a_repaired_read_can_retry() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    writer.start(&f.stream).await?;
    for version in 1..=3 {
        writer
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    writer.finish(&f.stream).await?;
    f.cleaned().await?;
    let key = f
        .archive
        .list("")
        .await?
        .into_iter()
        .find(|key| key.ends_with(".batch"))
        .unwrap();
    let original = f.archive.get(&key).await?.unwrap();
    let mut corrupt = original.bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(
        f.archive
            .compare_and_swap(&key, Some(original.generation), corrupt)
            .await?
    );
    let (reader, _) = reader(&f)?;
    assert!(reader.get(&f.stream.object(1)).await.is_err());
    let broken = f.archive.get(&key).await?.unwrap();
    assert!(
        f.archive
            .compare_and_swap(&key, Some(broken.generation), original.bytes)
            .await?
    );
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

fn reader(f: &Fixture) -> Result<(RapidSnapshots, Arc<ReadBucket>)> {
    let bucket = Arc::new(ReadBucket {
        inner: f.archive.clone(),
        gets: Mutex::new(BTreeMap::new()),
        lists: Mutex::new(BTreeMap::new()),
    });
    let reader = RapidSnapshots::new(
        bucket.clone(),
        Arc::new(BucketSnapshots(bucket.clone())),
        f.zones
            .iter()
            .map(|zone| zone.clone() as Arc<dyn LogZone>)
            .collect(),
        crate::bucket::ArchiveBatchConfig::default(),
        CancellationToken::new(),
    )?;
    Ok((reader, bucket))
}

struct ReadBucket {
    inner: Arc<FileBucket>,
    gets: Mutex<BTreeMap<String, usize>>,
    lists: Mutex<BTreeMap<String, usize>>,
}

#[async_trait]
impl Bucket for ReadBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        *self.gets.lock().unwrap().entry(key.into()).or_default() += 1;
        self.inner.get(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        *self.lists.lock().unwrap().entry(prefix.into()).or_default() += 1;
        self.inner.list(prefix).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}
