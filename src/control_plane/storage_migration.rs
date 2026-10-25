//! Moves an installation's archive and code to new buckets while its control plane is stopped.
//! Bytes are copied beforehand with `gcloud storage rsync`; this verifies the copies and rewrites
//! the records that name buckets: deployment bundles and actor ownership.

use std::{collections::HashSet, sync::Arc};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::digest::{SHA256, digest};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{StreamExt, TryStreamExt};
use serde::Serialize;

use crate::{
    bucket::{Bucket, GcsBucket, OwnerMigration, SnapshotStoreConfig, migrate_owner},
    bundle::{ActorBundle, BundleFile},
    clock::{Clock, SystemClock},
    postgres::PostgresDatabase,
};

const OWNERS: &str = "durable-actors/v3/owners/";
const CONCURRENCY: usize = 64;

#[derive(Debug, clap::Args)]
pub struct StorageMigrationOptions {
    /// Archive bucket the actors' history was copied from
    #[arg(long)]
    pub from_archive_bucket: String,
    /// Code bucket existing deployments were registered in
    #[arg(long)]
    pub from_artifact_bucket: String,
    /// Report what would change without writing anything
    #[arg(long)]
    pub dry_run: bool,
}

/// Reads the target storage from the same environment the new control plane will run with.
pub async fn migrate_storage(options: StorageMigrationOptions) -> Result<()> {
    let target = Target::from_lookup(|name| std::env::var(name).ok())?;
    let (_, target_archive) = target
        .persistence
        .rapid_settings()
        .context("Rapid target required")?;
    let migration = StorageMigration {
        buckets: MigrationBuckets {
            authority: Arc::new(GcsBucket::new(&target.authority).await?),
            source_archive: Arc::new(GcsBucket::new(&options.from_archive_bucket).await?),
            target_archive: Arc::new(GcsBucket::new(target_archive).await?),
            target_code: Arc::new(GcsBucket::new(&target.artifact).await?),
        },
        deployments: Arc::new(PostgresDeployments(
            PostgresDatabase::lazy_with_max_connections(
                &target.postgres_url,
                crate::postgres::DEFAULT_MAX_CONNECTIONS,
            )?,
        )),
        clock: Arc::new(SystemClock),
        plan: MigrationPlan {
            source_archive: options.from_archive_bucket,
            source_artifact: options.from_artifact_bucket,
            target_artifact: target.artifact,
            target: target.persistence,
        },
    };
    let report = migration.run(options.dry_run).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

struct Target {
    authority: String,
    artifact: String,
    postgres_url: String,
    persistence: SnapshotStoreConfig,
}

impl Target {
    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let persistence = super::process::persistence_config(&mut get)?;
        ensure!(
            persistence.rapid_settings().is_some(),
            "storage migration targets Rapid persistence"
        );
        Ok(Self {
            authority: super::process::required(&mut get, "DURABLE_ACTORS_BUCKET")?,
            artifact: super::process::required(&mut get, "DURABLE_ACTORS_ARTIFACT_BUCKET")?,
            postgres_url: super::process::required(&mut get, "DURABLE_ACTORS_POSTGRES_URL")?,
            persistence,
        })
    }
}

pub(crate) struct StorageMigration {
    pub buckets: MigrationBuckets,
    pub deployments: Arc<dyn DeploymentStore>,
    pub clock: Arc<dyn Clock>,
    pub plan: MigrationPlan,
}

pub(crate) struct MigrationBuckets {
    pub authority: Arc<dyn Bucket>,
    pub source_archive: Arc<dyn Bucket>,
    pub target_archive: Arc<dyn Bucket>,
    pub target_code: Arc<dyn Bucket>,
}

pub(crate) struct MigrationPlan {
    pub source_archive: String,
    pub source_artifact: String,
    pub target_artifact: String,
    pub target: SnapshotStoreConfig,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MigrationReport {
    pub dry_run: bool,
    pub archive_objects: usize,
    pub deployments_rebased: usize,
    pub owners_current: usize,
    pub owners_rewritten: usize,
}

#[async_trait]
pub(crate) trait DeploymentStore: Send + Sync {
    /// Every deployment's project and encoded code bundle.
    async fn code_snapshots(&self) -> Result<Vec<(String, String)>>;
    /// Replaces a bundle only if it is still `current`.
    async fn replace_code_snapshot(
        &self,
        project: &str,
        current: &str,
        replacement: &str,
    ) -> Result<bool>;
}

struct OwnerRewrite {
    key: String,
    generation: i64,
    bytes: bytes::Bytes,
}

struct Rebased {
    project: String,
    current: String,
    replacement: String,
}

impl StorageMigration {
    /// Checks everything before writing anything, so a refusal leaves the installation untouched.
    pub(crate) async fn run(&self, dry_run: bool) -> Result<MigrationReport> {
        let (owners_current, owners) = self.plan_owners().await?;
        let archive_objects = self.verify_archive_copy().await?;
        let deployments = self.plan_deployments().await?;
        let report = MigrationReport {
            dry_run,
            archive_objects,
            deployments_rebased: deployments.len(),
            owners_current,
            owners_rewritten: owners.len(),
        };
        if !dry_run {
            self.apply_deployments(deployments).await?;
            self.apply_owners(owners).await?;
        }
        Ok(report)
    }

    async fn plan_owners(&self) -> Result<(usize, Vec<OwnerRewrite>)> {
        let now_ms = self.clock.now_ms()?;
        let keys = self.buckets.authority.list(OWNERS).await?;
        let planned: Vec<_> = futures_util::stream::iter(keys)
            .map(|key| self.plan_owner(key, now_ms))
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await?;
        let live: Vec<_> = planned
            .iter()
            .filter_map(|(_, _, plan)| match plan {
                OwnerMigration::Live(actor) => Some(actor),
                _ => None,
            })
            .collect();
        ensure!(
            live.is_empty(),
            "{} actors still hold a lease (first: {}); stop the control plane and wait for hosts to exit",
            live.len(),
            live[0].storage_key().as_str()
        );
        let current = planned
            .iter()
            .filter(|(_, _, plan)| *plan == OwnerMigration::Current)
            .count();
        let rewrites = planned
            .into_iter()
            .filter_map(|(key, generation, plan)| match plan {
                OwnerMigration::Rewrite(bytes) => Some(OwnerRewrite {
                    key,
                    generation,
                    bytes,
                }),
                _ => None,
            })
            .collect();
        Ok((current, rewrites))
    }

    /// A record deleted after listing needs nothing, so it counts as current.
    async fn plan_owner(&self, key: String, now_ms: u64) -> Result<(String, i64, OwnerMigration)> {
        let Some(object) = self.buckets.authority.get(&key).await? else {
            return Ok((key, 0, OwnerMigration::Current));
        };
        let plan = &self.plan;
        let migration = migrate_owner(&object.bytes, &plan.source_archive, &plan.target, now_ms)
            .with_context(|| format!("ownership record {key}"))?;
        Ok((key, object.generation, migration))
    }

    async fn verify_archive_copy(&self) -> Result<usize> {
        let copied: HashSet<String> = self
            .buckets
            .target_archive
            .list("")
            .await?
            .into_iter()
            .collect();
        let source = self.buckets.source_archive.list("").await?;
        let missing: Vec<_> = source.iter().filter(|key| !copied.contains(*key)).collect();
        ensure!(
            missing.is_empty(),
            "{} archive objects are not in the new archive yet (first: {}); rerun gcloud storage rsync",
            missing.len(),
            missing[0]
        );
        Ok(source.len())
    }

    async fn plan_deployments(&self) -> Result<Vec<Rebased>> {
        let snapshots = self.deployments.code_snapshots().await?;
        let rebased: Vec<_> = futures_util::stream::iter(snapshots)
            .map(|(project, current)| async move {
                let replacement = self
                    .rebase(&current)
                    .await
                    .with_context(|| format!("deployment {project}"))?;
                anyhow::Ok(replacement.map(|replacement| Rebased {
                    project,
                    current,
                    replacement,
                }))
            })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        Ok(rebased.into_iter().flatten().collect())
    }

    async fn rebase(&self, encoded: &str) -> Result<Option<String>> {
        let bundle = ActorBundle::decode(encoded)?;
        if bundle.bucket == self.plan.target_artifact {
            return Ok(None);
        }
        ensure!(
            bundle.bucket == self.plan.source_artifact,
            "code is in unrelated bucket {}",
            bundle.bucket
        );
        let mut files = Vec::new();
        for file in bundle.files {
            files.push(self.copied(file).await?);
        }
        let bucket = self.plan.target_artifact.clone();
        Ok(Some(ActorBundle { bucket, files }.encode()?))
    }

    /// The copy's generation, once its bytes match the digest the deployment recorded.
    async fn copied(&self, file: BundleFile) -> Result<BundleFile> {
        let copy = self
            .buckets
            .target_code
            .get(&file.object)
            .await?
            .with_context(|| format!("{} is not in the new code bucket yet", file.object))?;
        let sha256 = URL_SAFE_NO_PAD.encode(digest(&SHA256, &copy.bytes).as_ref());
        ensure!(
            sha256 == file.sha256,
            "{} differs from its deployment record",
            file.object
        );
        Ok(BundleFile {
            generation: copy.generation,
            ..file
        })
    }

    async fn apply_deployments(&self, rebased: Vec<Rebased>) -> Result<()> {
        for change in rebased {
            ensure!(
                self.deployments
                    .replace_code_snapshot(&change.project, &change.current, &change.replacement)
                    .await?,
                "deployment {} changed during migration; rerun",
                change.project
            );
        }
        Ok(())
    }

    async fn apply_owners(&self, rewrites: Vec<OwnerRewrite>) -> Result<()> {
        let authority = &self.buckets.authority;
        let conflicts = futures_util::stream::iter(rewrites)
            .map(|rewrite| async move {
                let swapped = authority
                    .compare_and_swap(&rewrite.key, Some(rewrite.generation), rewrite.bytes)
                    .await?;
                anyhow::Ok(usize::from(!swapped))
            })
            .buffer_unordered(CONCURRENCY)
            .try_fold(0, |total, conflict| async move { Ok(total + conflict) })
            .await?;
        ensure!(
            conflicts == 0,
            "{conflicts} ownership records changed during migration; rerun"
        );
        Ok(())
    }
}

struct PostgresDeployments(PostgresDatabase);

#[async_trait]
impl DeploymentStore for PostgresDeployments {
    async fn code_snapshots(&self) -> Result<Vec<(String, String)>> {
        let client = self.0.connection().await?;
        let rows = client
            .query(
                "SELECT project_id, code_snapshot FROM durable_actors_deployment WHERE code_snapshot IS NOT NULL",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    async fn replace_code_snapshot(
        &self,
        project: &str,
        current: &str,
        replacement: &str,
    ) -> Result<bool> {
        let client = self.0.connection().await?;
        let changed = client
            .execute(
                "UPDATE durable_actors_deployment SET code_snapshot = $3, updated_at = clock_timestamp()
                 WHERE project_id = $1 AND code_snapshot = $2",
                &[&project, &current, &replacement],
            )
            .await?;
        Ok(changed == 1)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/storage_migration.rs"]
mod tests;
