//! Deletes history no restore can need, as its own process. Versions only grow across owner
//! epochs, so everything in an epoch older than the epoch of an actor's second-newest base lies
//! below that base, and so do bases older than the two newest. Both go once all of it is older
//! than the observer's lookback. An actor with fewer than two bases keeps everything.

use super::{
    base::{self, BaseName},
    *,
};
use std::time::{Instant, SystemTime};

const SNAPSHOTS: &str = "durable-actors-v3-snapshots-";

pub struct SweepConfig {
    pub archive_bucket: String,
    pub min_age: Duration,
    pub interval: Duration,
    pub dry_run: bool,
}

impl SweepConfig {
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let number = |value: Option<String>, name: &str, default: u64| -> Result<u64> {
            value
                .map(|value| value.parse())
                .transpose()
                .with_context(|| format!("{name} must be an integer"))
                .map(|value| value.unwrap_or(default))
        };
        Ok(Self {
            archive_bucket: get("DURABLE_ACTORS_ARCHIVE_BUCKET")
                .context("DURABLE_ACTORS_ARCHIVE_BUCKET is required")?,
            min_age: Duration::from_secs(
                3600 * number(
                    get("DURABLE_ACTORS_SWEEP_MIN_AGE_HOURS"),
                    "DURABLE_ACTORS_SWEEP_MIN_AGE_HOURS",
                    30 * 24,
                )?,
            ),
            interval: Duration::from_secs(number(
                get("DURABLE_ACTORS_SWEEP_INTERVAL_SECONDS"),
                "DURABLE_ACTORS_SWEEP_INTERVAL_SECONDS",
                3600,
            )?),
            dry_run: get("DURABLE_ACTORS_SWEEP_DRY_RUN").as_deref() == Some("true"),
        })
    }
}

/// An archive object as a listing describes it.
pub(super) struct Listed {
    pub name: String,
    pub created: SystemTime,
    pub bytes: u64,
}

#[async_trait]
pub(super) trait SweepStore: Send + Sync {
    /// One page of the archive's objects under `prefix`, after `page`, and the next page's token,
    /// empty after the last.
    async fn page(&self, prefix: &str, page: &str) -> Result<(Vec<Listed>, String)>;
    async fn get(&self, name: &str) -> Result<Option<Payload>>;
    /// Deletes an object of `bucket`, the archive when `None`; a missing object is deleted.
    async fn delete(&self, bucket: Option<&str>, name: &str, generation: Option<i64>)
    -> Result<()>;
}

#[derive(Default, Debug, PartialEq, Eq)]
pub(super) struct Summary {
    pub actors: u64,
    pub objects: u64,
    pub bytes: u64,
}

pub async fn serve(config: SweepConfig, shutdown: impl Future<Output = ()>) -> Result<()> {
    let store = GcsSweep::new(&config.archive_bucket).await?;
    tokio::pin!(shutdown);
    loop {
        let started = Instant::now();
        let cutoff = SystemTime::now() - config.min_age;
        match sweep(&store, cutoff, config.dry_run).await {
            Ok(summary) => tracing::info!(
                event = "history_sweep",
                actors = summary.actors,
                objects = summary.objects,
                bytes = summary.bytes,
                dry_run = config.dry_run,
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            ),
            Err(error) => {
                tracing::warn!(event = "history_sweep_failed", error = %format!("{error:#}"))
            }
        }
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            () = tokio::time::sleep(config.interval) => {}
        }
    }
}

/// One pass over the archive, an actor at a time: its objects are adjacent in the listing.
pub(super) async fn sweep(
    store: &dyn SweepStore,
    cutoff: SystemTime,
    dry_run: bool,
) -> Result<Summary> {
    let mut summary = Summary::default();
    let mut actor: Vec<Listed> = Vec::new();
    let mut page = String::new();
    loop {
        let (objects, next) = store.page(SNAPSHOTS, &page).await?;
        for object in objects {
            if actor
                .first()
                .is_some_and(|first| actor_of(&first.name) != actor_of(&object.name))
            {
                let done = std::mem::take(&mut actor);
                sweep_actor(store, &done, cutoff, dry_run, &mut summary).await?;
            }
            actor.push(object);
        }
        if next.is_empty() {
            break;
        }
        page = next;
    }
    sweep_actor(store, &actor, cutoff, dry_run, &mut summary).await?;
    Ok(summary)
}

async fn sweep_actor(
    store: &dyn SweepStore,
    objects: &[Listed],
    cutoff: SystemTime,
    dry_run: bool,
    summary: &mut Summary,
) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    summary.actors += 1;
    for object in plan(objects, cutoff) {
        summary.objects += 1;
        summary.bytes += object.bytes;
        if dry_run {
            continue;
        }
        if object.name.ends_with(".manifest") {
            delete_replicas(store, &object.name).await?;
        }
        store.delete(None, &object.name, None).await?;
    }
    Ok(())
}

/// The objects of one actor no restore can need, manifests first so the segments they name go
/// with them.
fn plan(objects: &[Listed], cutoff: SystemTime) -> Vec<&Listed> {
    let mut bases: Vec<(BaseName, &Listed)> = objects
        .iter()
        .filter_map(|object| Some((base::parse(&object.name)?, object)))
        .collect();
    bases.sort_by_key(|(name, _)| std::cmp::Reverse(name.version));
    let Some(&(floor, _)) = bases.get(1) else {
        return Vec::new();
    };
    let old = |object: &&Listed| object.created < cutoff;
    let mut doomed: Vec<&Listed> = bases[2..]
        .iter()
        .map(|(_, object)| *object)
        .filter(old)
        .collect();
    let mut epochs: BTreeMap<u64, Vec<&Listed>> = BTreeMap::new();
    for object in objects {
        if let Some(epoch) = epoch_of(&object.name) {
            epochs.entry(epoch).or_default().push(object);
        }
    }
    for epoch in epochs.range(..floor.owner_epoch).map(|(_, epoch)| epoch) {
        if epoch.iter().all(old) {
            doomed.extend(epoch);
        }
    }
    doomed.sort_by_key(|object| !object.name.ends_with(".manifest"));
    doomed
}

async fn delete_replicas(store: &dyn SweepStore, manifest: &str) -> Result<()> {
    let Some(bytes) = store.get(manifest).await? else {
        return Ok(());
    };
    let manifest: Manifest = serde_json::from_reader(bytes.reader())?;
    for replica in &manifest.replicas {
        store
            .delete(
                Some(&replica.bucket),
                &replica.object,
                Some(replica.generation),
            )
            .await?;
    }
    Ok(())
}

fn actor_of(name: &str) -> &str {
    name.split_once('~').map_or(name, |(actor, _)| actor)
}

/// The owner epoch an object of the actor's history belongs to; bases have none.
fn epoch_of(name: &str) -> Option<u64> {
    let epoch = name.split('~').nth(1)?;
    (epoch.len() == 32)
        .then(|| u64::from_str_radix(epoch, 16).ok())
        .flatten()
}

struct GcsSweep {
    archive: String,
    objects: super::super::GcsBucket,
    clients: super::super::gcs::GcsClients,
}

impl GcsSweep {
    async fn new(bucket: &str) -> Result<Self> {
        let objects = super::super::GcsBucket::new(bucket).await?;
        Ok(Self {
            archive: format!("projects/_/buckets/{bucket}"),
            clients: objects.clients(),
            objects,
        })
    }
}

#[async_trait]
impl SweepStore for GcsSweep {
    async fn page(&self, prefix: &str, page: &str) -> Result<(Vec<Listed>, String)> {
        let response = self
            .clients
            .control
            .list_objects()
            .set_parent(&self.archive)
            .set_prefix(prefix)
            .set_page_token(page)
            .send()
            .await?;
        let objects = response
            .objects
            .into_iter()
            .map(|object| Listed {
                created: object
                    .create_time
                    .and_then(|time| SystemTime::try_from(time).ok())
                    .unwrap_or_else(SystemTime::now),
                bytes: u64::try_from(object.size).unwrap_or_default(),
                name: object.name,
            })
            .collect();
        Ok((objects, response.next_page_token))
    }

    async fn get(&self, name: &str) -> Result<Option<Payload>> {
        Ok(self.objects.get(name).await?.map(|object| object.bytes))
    }

    async fn delete(
        &self,
        bucket: Option<&str>,
        name: &str,
        generation: Option<i64>,
    ) -> Result<()> {
        let mut request = self
            .clients
            .control
            .delete_object()
            .set_bucket(bucket.unwrap_or(&self.archive))
            .set_object(name);
        if let Some(generation) = generation {
            request = request.set_if_generation_match(generation);
        }
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error)
                if error.http_status_code() == Some(404)
                    || error.status().is_some_and(|s| s.code.name() == "NOT_FOUND") =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/rapid/sweep.rs"]
mod tests;
