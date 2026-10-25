use super::*;
use crate::bucket::SnapshotStoreConfig;

/// What a storage migration does with one ownership record.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OwnerMigration {
    Current,
    Live(ActorKey),
    Rewrite(Bytes),
}

/// Points a dormant actor's record at `target` storage. Its archive must come from
/// `source_archive` (or already be the target's), and `target` must keep every Rapid bucket the
/// record names, because unsealed manifests still reference those replicas.
pub(crate) fn migrate_owner(
    bytes: &[u8],
    source_archive: &str,
    target: &SnapshotStoreConfig,
    now_ms: u64,
) -> Result<OwnerMigration> {
    let mut record: Ownership = serde_json::from_slice(bytes)?;
    if record.persistence == *target {
        return Ok(OwnerMigration::Current);
    }
    if record.lease.expires_at_ms > now_ms {
        return Ok(OwnerMigration::Live(record.actor));
    }
    ensure!(
        movable(&record.persistence, source_archive, target),
        "{} uses storage this migration does not cover",
        record.actor.storage_key().as_str()
    );
    record.persistence = target.clone();
    Ok(OwnerMigration::Rewrite(serde_json::to_vec(&record)?.into()))
}

fn movable(
    current: &SnapshotStoreConfig,
    source_archive: &str,
    target: &SnapshotStoreConfig,
) -> bool {
    let (Some((buckets, archive)), Some((target_buckets, target_archive))) =
        (current.rapid_settings(), target.rapid_settings())
    else {
        return false;
    };
    (archive == source_archive || archive == target_archive)
        && buckets.iter().all(|bucket| target_buckets.contains(bucket))
}

#[cfg(test)]
pub(crate) fn owner_record(
    actor: &ActorKey,
    persistence: &SnapshotStoreConfig,
    expires_at_ms: u64,
) -> Bytes {
    let record = Ownership {
        persistence: persistence.clone(),
        sealed: true,
        lease: HostLease {
            id: HostId::new("host"),
            session_id: "session".into(),
            route: "http://host".into(),
            expires_at_ms,
        },
        mutation: String::new(),
        actor: actor.clone(),
        epoch: 1,
        region: "north-america-west".into(),
        base: None,
    };
    serde_json::to_vec(&record).unwrap().into()
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/owner_migration.rs"]
mod tests;
