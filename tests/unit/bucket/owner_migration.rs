use super::*;
use crate::bucket::{RapidBucket, RapidPlacement};

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

fn actor() -> ActorKey {
    ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    }
}

#[test]
fn dormant_records_change_only_their_storage() -> Result<()> {
    let migrated = migrate_owner(
        &owner_record(&actor(), &source(), 10),
        "archive",
        &target(),
        20,
    )?;
    assert_eq!(
        migrated,
        OwnerMigration::Rewrite(owner_record(&actor(), &target(), 10))
    );
    assert_eq!(
        migrate_owner(
            &owner_record(&actor(), &target(), 10),
            "archive",
            &target(),
            20
        )?,
        OwnerMigration::Current
    );
    Ok(())
}

#[test]
fn live_actors_are_reported_instead_of_moved() -> Result<()> {
    assert_eq!(
        migrate_owner(
            &owner_record(&actor(), &source(), 30),
            "archive",
            &target(),
            20
        )?,
        OwnerMigration::Live(actor())
    );
    Ok(())
}

#[test]
fn storage_outside_the_migration_is_refused() {
    let dropped_zone = rapid(
        &[("rapid-w1", "us-west1-a")],
        "archive-dual",
        RapidPlacement::Nearest,
    );
    for (record, source_archive, target) in [
        (
            owner_record(&actor(), &source(), 10),
            "archive",
            dropped_zone,
        ),
        (
            owner_record(&actor(), &source(), 10),
            "other-archive",
            target(),
        ),
        (
            owner_record(&actor(), &SnapshotStoreConfig::Local, 10),
            "archive",
            target(),
        ),
    ] {
        assert!(migrate_owner(&record, source_archive, &target, 20).is_err());
    }
}
