#[cfg(test)]
#[path = "../../tests/unit/bucket/rapid.rs"]
mod tests;
use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::digest::{SHA256, digest};
use bytes::Bytes;
use google_cloud_storage::{
    appendable_object_writer::AppendableObjectWriter,
    client::{Storage, StorageControl},
    model_ext::ReadRange,
};
use tokio::sync::Mutex;

use super::SnapshotStore;

const MAGIC: &[u8; 8] = b"DARAPID1";
const HEADER: usize = 24;
const CHECKSUM: usize = 32;
const TRAILER: &[u8; 8] = b"DARAPEND";
const FOOTER: usize = 16;
const READ_WINDOW: u64 = 64 * 1024;
const APPEND_CHUNK: usize = 256 * 1024;
const SEGMENT_TARGET: i64 = 64 * 1024 * 1024;

pub(crate) struct RapidSnapshots {
    bucket: String,
    storage: Storage,
    control: StorageControl,
    writers: Mutex<BTreeMap<String, Arc<Mutex<Writer>>>>,
}

struct Writer {
    stream: Option<AppendableObjectWriter>,
    offset: i64,
    latest: Option<(u64, Bytes)>,
}

impl RapidSnapshots {
    pub fn with_clients(bucket: &str, clients: super::gcs::GcsClients) -> Result<Self> {
        crate::storage::validate_bucket(bucket)?;
        Ok(Self {
            bucket: format!("projects/_/buckets/{bucket}"),
            storage: clients.storage,
            control: clients.control,
            writers: Mutex::new(BTreeMap::new()),
        })
    }

    async fn writer(&self, prefix: &str) -> Result<Arc<Mutex<Writer>>> {
        let mut writers = self.writers.lock().await;
        if let Some(writer) = writers.get(prefix) {
            return Ok(writer.clone());
        }
        let stream = self
            .storage
            .open_appendable_object(&self.bucket, segment(prefix, 0))
            .set_if_generation_match(0)
            .send()
            .await?;
        let writer = Arc::new(Mutex::new(Writer {
            stream: Some(stream),
            offset: 0,
            latest: None,
        }));
        writers.insert(prefix.into(), writer.clone());
        Ok(writer)
    }

    async fn objects(&self, prefix: &str) -> Result<Vec<google_cloud_storage::model::Object>> {
        let mut token = String::new();
        let mut objects = Vec::new();
        loop {
            let page = self
                .control
                .list_objects()
                .set_parent(&self.bucket)
                .set_prefix(prefix)
                .set_page_token(&token)
                .send()
                .await?;
            objects.extend(page.objects);
            token = page.next_page_token;
            if token.is_empty() {
                return Ok(objects);
            }
        }
    }

    async fn seal_object(&self, object: google_cloud_storage::model::Object) -> Result<()> {
        if object.finalize_time.is_some() {
            return Ok(());
        }
        let finalize = async {
            let writer = self
                .storage
                .reopen_appendable_object(&self.bucket, &object.name, object.generation)
                .send()
                .await?;
            writer.finalize().await?;
            anyhow::Ok(())
        }
        .await;
        if let Err(error) = finalize {
            let current = self
                .storage
                .open_object(&self.bucket, &object.name)
                .set_generation(object.generation)
                .send()
                .await?;
            if current.object().finalize_time.is_none() {
                return Err(error);
            }
        }
        Ok(())
    }

    async fn read_log(&self, object: &str) -> Result<Vec<(u64, Bytes)>> {
        let reader = self.reader(object).await?;
        if reader.size() == 0 {
            return Ok(vec![]);
        }
        records(&reader.range(0, reader.size()).await?)
    }

    async fn reader(&self, object: &str) -> Result<GcsLog> {
        let descriptor = self
            .storage
            .open_object(&self.bucket, object)
            .send()
            .await?;
        let size = u64::try_from(descriptor.object().size)?;
        Ok(GcsLog { descriptor, size })
    }

    async fn segments(&self, prefix: &str) -> Result<Vec<google_cloud_storage::model::Object>> {
        let mut objects = self.objects(prefix).await?;
        objects.retain(|object| segment_position(&object.name).is_some());
        objects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(objects)
    }
}

#[async_trait]
impl SnapshotStore for RapidSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        let (prefix, version) = position(object)?;
        for log in self.segments(prefix).await?.into_iter().rev() {
            if segment_position(&log.name).is_some_and(|(_, start)| start <= version) {
                let reader = self.reader(&log.name).await?;
                if let Some((latest, bytes)) = last_record(&reader).await? {
                    if latest == version {
                        return Ok(Some(bytes));
                    }
                }
                return Ok(self
                    .read_log(&log.name)
                    .await?
                    .into_iter()
                    .find(|(v, _)| *v == version)
                    .map(|(_, bytes)| bytes));
            }
        }
        Ok(None)
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        for log in self.segments(prefix).await?.into_iter().rev() {
            if let Some((version, bytes)) = last_record(&self.reader(&log.name).await?).await? {
                return Ok(Some((format!("{prefix}{version}.json"), bytes)));
            }
        }
        Ok(None)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        for object in self.segments(prefix).await? {
            let (stream, _) = segment_position(&object.name).context("invalid Rapid segment")?;
            for (version, _) in self.read_log(&object.name).await? {
                keys.push(format!("{stream}{version}.json"));
            }
        }
        Ok(keys)
    }

    async fn seal(&self, prefix: &str) -> Result<()> {
        if let Some(writer) = self.writers.lock().await.get(prefix).cloned() {
            writer.lock().await.stream.take();
        }
        for object in self.segments(prefix).await? {
            self.seal_object(object).await?;
        }
        Ok(())
    }

    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let (prefix, version) = position(object)?;
        let writer = self.writer(prefix).await?;
        let mut writer = writer.lock().await;
        if let Some((previous, payload)) = &writer.latest {
            if *previous == version {
                ensure!(*payload == bytes, "conflicting Rapid snapshot");
                return Ok(());
            }
            ensure!(*previous < version, "Rapid state version regressed");
        }
        let frame = frame(version, &bytes)?;
        let mut stream = writer
            .stream
            .take()
            .context("Rapid stream failed; activation must recover before another write")?;
        if writer.offset >= SEGMENT_TARGET {
            stream.finalize().await?;
            stream = self
                .storage
                .open_appendable_object(&self.bucket, segment(prefix, version))
                .set_if_generation_match(0)
                .send()
                .await?;
            writer.offset = 0;
        }
        let expected = writer
            .offset
            .checked_add(i64::try_from(frame.len())?)
            .context("Rapid offset overflow")?;
        for offset in (0..frame.len()).step_by(APPEND_CHUNK) {
            stream
                .append(frame.slice(offset..(offset + APPEND_CHUNK).min(frame.len())))
                .await?;
        }
        ensure!(
            stream.flush().await? == expected,
            "Rapid flush did not confirm the complete record"
        );
        writer.stream = Some(stream);
        writer.offset = expected;
        writer.latest = Some((version, bytes));
        Ok(())
    }

    async fn prepare(&self, prefix: &str) -> Result<()> {
        self.writer(prefix).await?;
        Ok(())
    }
}

fn position(object: &str) -> Result<(&str, u64)> {
    let (prefix, file) = object.rsplit_once('/').context("snapshot stream missing")?;
    let version: u64 = file
        .strip_suffix(".json")
        .context("snapshot extension missing")?
        .parse()?;
    ensure!(version > 0, "invalid snapshot version");
    Ok((&object[..prefix.len() + 1], version))
}

fn frame(version: u64, bytes: &[u8]) -> Result<Bytes> {
    let mut frame = Vec::new();
    frame.extend_from_slice(MAGIC);
    frame.extend_from_slice(&version.to_le_bytes());
    frame.extend_from_slice(&u64::try_from(bytes.len())?.to_le_bytes());
    frame.extend_from_slice(bytes);
    frame.extend_from_slice(digest(&SHA256, &frame).as_ref());
    let length = u64::try_from(
        frame
            .len()
            .checked_add(FOOTER)
            .context("Rapid frame length overflow")?,
    )?;
    frame.extend_from_slice(TRAILER);
    frame.extend_from_slice(&length.to_le_bytes());
    Ok(Bytes::from(frame))
}

fn records(mut bytes: &[u8]) -> Result<Vec<(u64, Bytes)>> {
    let mut records = BTreeMap::new();
    while bytes.len() >= HEADER {
        ensure!(&bytes[..8] == MAGIC, "invalid Rapid record header");
        let version = u64::from_le_bytes(bytes[8..16].try_into()?);
        let length = usize::try_from(u64::from_le_bytes(bytes[16..24].try_into()?))?;
        let end = HEADER
            .checked_add(length)
            .context("Rapid record length overflow")?;
        let complete = end
            .checked_add(CHECKSUM + FOOTER)
            .context("Rapid record length overflow")?;
        if bytes.len() < complete {
            break;
        }
        ensure!(version > 0, "invalid Rapid state version");
        ensure!(
            &bytes[end + CHECKSUM..end + CHECKSUM + 8] == TRAILER
                && u64::from_le_bytes(bytes[complete - 8..complete].try_into()?) == complete as u64,
            "invalid Rapid record footer"
        );
        ensure!(
            digest(&SHA256, &bytes[..end]).as_ref() == &bytes[end..end + CHECKSUM],
            "Rapid checksum mismatch"
        );
        let payload = Bytes::copy_from_slice(&bytes[HEADER..end]);
        if let Some(previous) = records.insert(version, payload.clone()) {
            ensure!(previous == payload, "conflicting Rapid record version");
        }
        bytes = &bytes[complete..];
    }
    Ok(records.into_iter().collect())
}

fn segment(prefix: &str, first_version: u64) -> String {
    format!("{prefix}{first_version:020}.log")
}

fn segment_position(object: &str) -> Option<(&str, u64)> {
    let (prefix, file) = object.rsplit_once('/')?;
    let version = file.strip_suffix(".log")?.parse().ok()?;
    Some((&object[..prefix.len() + 1], version))
}

#[async_trait]
trait LogReader: Send + Sync {
    fn size(&self) -> u64;
    async fn range(&self, start: u64, length: u64) -> Result<Bytes>;
}

struct GcsLog {
    descriptor: google_cloud_storage::object_descriptor::ObjectDescriptor,
    size: u64,
}

#[async_trait]
impl LogReader for GcsLog {
    fn size(&self) -> u64 {
        self.size
    }
    async fn range(&self, start: u64, length: u64) -> Result<Bytes> {
        let mut response = self
            .descriptor
            .read_range(ReadRange::segment(start, length))
            .await;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        ensure!(bytes.len() as u64 == length, "truncated Rapid range read");
        Ok(bytes.into())
    }
}

async fn last_record(reader: &dyn LogReader) -> Result<Option<(u64, Bytes)>> {
    let mut end = reader.size();
    while end >= FOOTER as u64 {
        let start = end.saturating_sub(READ_WINDOW);
        let window = reader.range(start, end - start).await?;
        for offset in (0..=window.len() - FOOTER).rev() {
            if &window[offset..offset + 8] != TRAILER {
                continue;
            }
            let length = u64::from_le_bytes(window[offset + 8..offset + FOOTER].try_into()?);
            let record_end = start + offset as u64 + FOOTER as u64;
            if length < (HEADER + CHECKSUM + FOOTER) as u64 || length > record_end {
                continue;
            }
            let record_start = record_end - length;
            let bytes = if record_start >= start {
                window.slice((record_start - start) as usize..offset + FOOTER)
            } else {
                reader.range(record_start, length).await?
            };
            if &bytes[..8] != MAGIC {
                continue;
            }
            let payload_length = u64::from_le_bytes(bytes[16..24].try_into()?);
            if payload_length.checked_add((HEADER + CHECKSUM + FOOTER) as u64) != Some(length) {
                continue;
            }
            return Ok(records(&bytes)?.pop());
        }
        if start == 0 {
            break;
        }
        end = start + FOOTER as u64 - 1;
    }
    Ok(None)
}
