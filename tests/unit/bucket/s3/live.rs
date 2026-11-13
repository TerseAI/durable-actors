//! Runs against a real bucket when `DURABLE_ACTORS_TEST_S3_BUCKET` and
//! `DURABLE_ACTORS_TEST_S3_REGION` are set, with AWS credentials from the environment. Setting
//! `DURABLE_ACTORS_TEST_S3_ACTOR_ROLE_ARN` also issues actor credentials through STS. Each run
//! writes under fresh keys and leaves them for the bucket's lifecycle rules.

use std::{future::Future, sync::Arc};

use anyhow::{Context, Result};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::{
    actor::ActorKey,
    bucket::{
        Bucket, BucketSnapshots, RuntimeStorage, SnapshotStore, SnapshotStoreConfig,
        StorageRevision,
        access::{AccessIssuer, StorageAccess},
        s3::{
            S3Bucket, S3ExpressLogs,
            access::{AwsCredentialIssuer, S3Access, SdkAssumeRole, TemporaryCredentials},
            bundle::{S3Bundle, S3BundleFile},
        },
    },
    bundle::ArtifactBundle,
    clock::SystemClock,
    host::HostId,
    host_leases::HostLeaseRequest,
};

struct Live {
    /// Two independent clients of one bucket, like two hosts.
    clients: [Arc<dyn Bucket>; 2],
    prefix: String,
}

impl Live {
    async fn connect() -> Result<Option<Self>> {
        let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_S3_BUCKET") else {
            return Ok(None);
        };
        let region = std::env::var("DURABLE_ACTORS_TEST_S3_REGION")?;
        let client = || async {
            let sdk = sdk(&region).await;
            anyhow::Ok(
                Arc::new(S3Bucket::new(&bucket, aws_sdk_s3::Client::new(&sdk))?) as Arc<dyn Bucket>,
            )
        };
        Ok(Some(Self {
            clients: [client().await?, client().await?],
            prefix: format!("test/{}/", uuid::Uuid::new_v4().simple()),
        }))
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

async fn sdk(region: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_owned()))
        .load()
        .await
}

fn unique(label: &str) -> Bytes {
    Bytes::from(format!("{label}-{}", uuid::Uuid::new_v4()))
}

/// Runs twelve conditional writes at once, alternating clients, and returns the winners' bytes.
async fn race(live: &Live, key: &str, revision: Option<StorageRevision>) -> Result<Vec<Bytes>> {
    let attempts: Vec<_> = (0..12)
        .map(|index| {
            let (bucket, key, revision) = (
                live.clients[index % 2].clone(),
                key.to_owned(),
                revision.clone(),
            );
            tokio::spawn(async move {
                let bytes = unique(&format!("attempt-{index}"));
                let won = bucket
                    .compare_and_swap(&key, revision, bytes.clone())
                    .await?;
                anyhow::Ok(won.then_some(bytes))
            })
        })
        .collect();
    let mut winners = Vec::new();
    for attempt in attempts {
        winners.extend(attempt.await??);
    }
    Ok(winners)
}

#[tokio::test]
async fn reads_report_absence_and_the_written_revision() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let [bucket, other] = &live.clients;
    let key = live.key("owner.json");
    assert!(bucket.get(&key).await?.is_none());
    let bytes = unique("first");
    assert!(bucket.compare_and_swap(&key, None, bytes.clone()).await?);
    let read = other.get(&key).await?.context("written object")?;
    assert_eq!(read.bytes, bytes);
    assert!(matches!(&read.revision, StorageRevision::ETag(_)) && read.revision.is_valid());
    assert_eq!(bucket.get(&key).await?.unwrap().revision, read.revision);
    Ok(())
}

#[tokio::test]
async fn concurrent_creates_have_exactly_one_winner() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let key = live.key("create.json");
    let winners = race(&live, &key, None).await?;
    assert_eq!(winners.len(), 1);
    assert_eq!(live.clients[0].get(&key).await?.unwrap().bytes, winners[0]);
    Ok(())
}

#[tokio::test]
async fn concurrent_swaps_from_one_revision_have_exactly_one_winner() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let key = live.key("swap.json");
    assert!(
        live.clients[0]
            .compare_and_swap(&key, None, unique("base"))
            .await?
    );
    let base = live.clients[1].get(&key).await?.unwrap().revision;
    let winners = race(&live, &key, Some(base.clone())).await?;
    assert_eq!(winners.len(), 1);
    let current = live.clients[0].get(&key).await?.unwrap();
    assert_eq!(current.bytes, winners[0]);
    assert_ne!(current.revision, base);
    Ok(())
}

#[tokio::test]
async fn stale_and_mismatched_preconditions_lose() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let [writer, stale] = &live.clients;
    let key = live.key("stale.json");
    let absent = StorageRevision::ETag("\"absent\"".into());
    // S3 answers a swap of a missing object with 404, which the adapter reports as an error.
    assert!(!matches!(
        writer
            .compare_and_swap(&key, Some(absent), unique("absent"))
            .await,
        Ok(true)
    ));
    assert!(writer.compare_and_swap(&key, None, unique("one")).await?);
    assert!(!writer.compare_and_swap(&key, None, unique("again")).await?);
    let first = stale.get(&key).await?.unwrap().revision;
    assert!(
        writer
            .compare_and_swap(&key, Some(first.clone()), unique("two"))
            .await?
    );
    let second = writer.get(&key).await?.unwrap().revision;
    assert_ne!(first, second);
    assert!(
        !stale
            .compare_and_swap(&key, Some(first), unique("late"))
            .await?
    );
    assert!(
        stale
            .compare_and_swap(&key, Some(second), unique("three"))
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn ambiguous_replies_reconcile_by_content() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let key = live.key("ambiguous.json");
    let bytes = unique("once");
    let [first, retry] = &live.clients;
    assert!(crate::bucket::replace(first.as_ref(), &key, None, bytes.clone()).await?);
    assert!(crate::bucket::replace(retry.as_ref(), &key, None, bytes).await?);
    assert!(!crate::bucket::replace(retry.as_ref(), &key, None, unique("other")).await?);
    Ok(())
}

#[tokio::test]
async fn ranges_and_paginated_prefix_listing() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let bucket = &live.clients[0];
    let key = live.key("range/object.json");
    assert!(
        bucket
            .compare_and_swap(&key, None, Bytes::from_static(b"0123456789"))
            .await?
    );
    assert_eq!(
        bucket.range(&key, 2, 5).await?.unwrap().bytes.as_ref(),
        b"23456"
    );
    assert!(bucket.range(&key, 8, 5).await.is_err());
    assert!(
        bucket
            .range(&live.key("range/missing"), 0, 1)
            .await?
            .is_none()
    );

    // One more than a ListObjectsV2 page.
    let count = 1001;
    stream::iter(0..count)
        .map(|index| {
            let key = live.key(&format!("list/{index:05}.json"));
            async move {
                bucket
                    .compare_and_swap(&key, None, Bytes::from_static(b"x"))
                    .await
            }
        })
        .buffer_unordered(64)
        .try_for_each(|created| async move {
            anyhow::ensure!(created, "create lost");
            Ok(())
        })
        .await?;
    let mut listed = live.clients[1].list(&live.key("list/")).await?;
    listed.sort();
    assert_eq!(listed.len(), count);
    assert_eq!(listed[0], live.key("list/00000.json"));
    assert!(
        live.clients[1]
            .list(&live.key("nothing/"))
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn immutable_snapshots_cannot_be_replaced() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let snapshots = BucketSnapshots(live.clients[0].clone());
    let object = live.key("snapshots/1.json");
    let bytes = unique("state");
    snapshots.put(&object, bytes.clone()).await?;
    snapshots.put(&object, bytes.clone()).await?;
    assert!(snapshots.put(&object, unique("forged")).await.is_err());
    assert_eq!(
        BucketSnapshots(live.clients[1].clone())
            .get(&object)
            .await?,
        Some(bytes)
    );
    Ok(())
}

fn lease(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new(format!("host-{session}")),
        session_id: session.into(),
        route: format!("http://{session}"),
        duration_ms: 30_000,
    }
}

fn actor(project: &str, id: &str) -> ActorKey {
    ActorKey {
        project_id: project.into(),
        actor_name: "Counter".into(),
        actor_id: id.into(),
    }
}

fn test_project() -> String {
    format!("test{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
}

#[tokio::test]
async fn ownership_fences_a_second_host() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let actor = actor(&test_project(), "one");
    let [left, right] = &live.clients;
    let first = RuntimeStorage::new(left.clone(), Arc::new(SystemClock))?;
    let second = RuntimeStorage::new(right.clone(), Arc::new(SystemClock))?;
    let (left_lease, right_lease) = (lease("left"), lease("right"));
    let (a, b) = tokio::join!(
        first.register_activation(&actor, &left_lease, "us-west-2", true, None),
        second.register_activation(&actor, &right_lease, "us-west-2", true, None),
    );
    assert_ne!(a.is_ok(), b.is_ok(), "exactly one host activates");
    let (winner, winner_lease, loser, loser_lease) = if a.is_ok() {
        (&first, left_lease, &second, right_lease)
    } else {
        (&second, right_lease, &first, left_lease)
    };
    assert!(
        loser
            .register_activation(&actor, &loser_lease, "us-west-2", false, None)
            .await
            .is_err(),
        "a live lease fences takeover"
    );
    assert!(loser.renew_activation(&actor, &loser_lease).await.is_err());
    winner.renew_activation(&actor, &winner_lease).await?;
    winner
        .release_activation(&actor, &winner_lease.id, &winner_lease.session_id)
        .await?;
    let takeover = loser
        .register_activation(&actor, &loser_lease, "us-west-2", false, None)
        .await?;
    assert_eq!(takeover.placement.owner_epoch, 2);
    assert!(
        winner
            .renew_activation(&actor, &winner_lease)
            .await
            .is_err(),
        "the old owner is fenced"
    );
    Ok(())
}

/// Issues one actor's credentials as the control plane does and proves, against S3 itself, that
/// they reach only that actor's ownership record, state and code.
#[tokio::test]
async fn actor_credentials_reach_only_their_own_actor() -> Result<()> {
    let (Ok(bucket), Ok(role)) = (
        std::env::var("DURABLE_ACTORS_TEST_S3_BUCKET"),
        std::env::var("DURABLE_ACTORS_TEST_S3_ACTOR_ROLE_ARN"),
    ) else {
        return Ok(());
    };
    let region = std::env::var("DURABLE_ACTORS_TEST_S3_REGION")?;
    let sdk = sdk(&region).await;
    let platform = aws_sdk_s3::Client::new(&sdk);
    let project = test_project();
    let (one, two) = (actor(&project, "one"), actor(&project, "two"));
    let owner = |actor: &ActorKey| crate::storage_paths::owner(&actor.storage_key());
    let state = |actor: &ActorKey| crate::storage_paths::snapshots(actor);
    let code = |actor: &ActorKey| {
        format!(
            "{}artifacts/{project}/{}/actors.mjs",
            crate::storage_paths::ROOT,
            actor.actor_id
        )
    };
    for actor in [&one, &two] {
        for key in [
            owner(actor)?,
            format!("{}1.json", state(actor)?),
            code(actor),
        ] {
            put(&platform, &bucket, &key).await?;
        }
    }
    let bundle = S3Bundle {
        bucket: bucket.clone(),
        region: region.clone(),
        files: vec![S3BundleFile {
            path: "actors.mjs".into(),
            object: code(&one),
            etag: "\"code\"".into(),
            sha256: "0".repeat(64),
        }],
    };
    let issuer = AwsCredentialIssuer::new(
        role,
        Arc::new(SdkAssumeRole::new(aws_sdk_sts::Client::new(&sdk))),
        Arc::new(SystemClock),
    )?;
    let access = S3Access::new(
        bucket.clone(),
        region,
        bucket.clone(),
        Arc::new(issuer),
        Arc::new(S3ExpressLogs::new(platform.clone())),
    );
    let Some(StorageAccess::S3(granted)) = access
        .issue(
            &one,
            &SnapshotStoreConfig::Local,
            Some(ArtifactBundle::S3(bundle)),
        )
        .await?
    else {
        anyhow::bail!("S3 access expected");
    };
    let ownership = scoped(&sdk, &granted.ownership);
    let snapshots = scoped(&sdk, &granted.state);
    let artifacts = scoped(&sdk, &granted.artifacts[0].credentials);

    let own_owner = S3Bucket::new(&bucket, ownership.clone())?;
    let revision = own_owner.get(&owner(&one)?).await?.unwrap().revision;
    assert!(
        own_owner
            .compare_and_swap(&owner(&one)?, Some(revision), unique("lease"))
            .await?
    );
    let own_state = S3Bucket::new(&bucket, snapshots.clone())?;
    let next = format!("{}2.json", state(&one)?);
    assert!(
        own_state
            .compare_and_swap(&next, None, unique("state"))
            .await?
    );
    assert!(own_state.get(&next).await?.is_some());
    assert_eq!(own_state.list(&state(&one)?).await?.len(), 2);
    assert!(
        S3Bucket::new(&bucket, artifacts.clone())?
            .get(&code(&one))
            .await?
            .is_some()
    );

    for (client, key) in [
        (&ownership, owner(&two)?),
        (&ownership, format!("{}1.json", state(&one)?)),
        (&snapshots, format!("{}1.json", state(&two)?)),
        (&snapshots, owner(&two)?),
        (&artifacts, code(&two)),
    ] {
        assert!(
            denied(client.get_object().bucket(&bucket).key(&key).send()).await,
            "read {key}"
        );
    }
    for (client, key) in [
        (&ownership, owner(&two)?),
        (&snapshots, format!("{}2.json", state(&two)?)),
        (&snapshots, owner(&two)?),
        (
            &snapshots,
            format!("{}hosts/outside", crate::storage_paths::ROOT),
        ),
        (&artifacts, code(&one)),
    ] {
        let write = client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(unique("forged").into());
        assert!(denied(write.send()).await, "write {key}");
    }
    for (client, prefix) in [
        (&snapshots, state(&two)?),
        (&snapshots, String::new()),
        (&ownership, state(&one)?),
    ] {
        let list = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
        assert!(denied(list.send()).await, "list {prefix:?}");
    }
    Ok(())
}

async fn put(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(unique("platform").into())
        .send()
        .await?;
    Ok(())
}

fn scoped(sdk: &aws_config::SdkConfig, granted: &TemporaryCredentials) -> aws_sdk_s3::Client {
    let credentials = aws_sdk_s3::config::Credentials::new(
        &granted.access_key_id,
        &granted.secret_access_key,
        Some(granted.session_token.clone()),
        None,
        "actor",
    );
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(sdk)
            .credentials_provider(credentials)
            .build(),
    )
}

async fn denied<T, E: ProvideErrorMetadata, R>(
    request: impl Future<Output = std::result::Result<T, SdkError<E, R>>>,
) -> bool {
    matches!(request.await, Err(error) if error.code() == Some("AccessDenied"))
}
