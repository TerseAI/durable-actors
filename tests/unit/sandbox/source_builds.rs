use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Store {
    entries: Mutex<std::collections::HashMap<String, BuiltActorCode>>,
}

#[async_trait]
impl BuildStore for Store {
    async fn get(&self, key: &str) -> Result<Option<BuiltActorCode>> {
        Ok(self.entries.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, key: &str, build: &BuiltActorCode) -> Result<()> {
        self.entries
            .lock()
            .unwrap()
            .insert(key.into(), build.clone());
        Ok(())
    }
    async fn token(&self, _: &SourceArchive, _: &str, _: &str) -> Result<String> {
        Ok("scoped-token".into())
    }
}

struct Workers(AtomicUsize);

struct InvalidWorkers;
#[async_trait]
impl BuildExecutor for InvalidWorkers {
    async fn build(&self, region: &str, request: &BuildRequest) -> Result<BuildReply> {
        let mut reply = Workers(AtomicUsize::new(0)).build(region, request).await?;
        reply.contract = serde_json::json!({"version": 999, "actors": []});
        Ok(reply)
    }
}

#[tokio::test]
async fn invalid_compilations_are_not_cached() -> Result<()> {
    let store = Arc::new(Store {
        entries: Mutex::new(Default::default()),
    });
    let builds = SourceBuilds {
        bucket: "code-bucket".into(),
        store: store.clone(),
        executor: Arc::new(InvalidWorkers),
    };
    let source = SourceArchive {
        sha256: "a".repeat(64),
        entrypoint: "src/actor.ts".into(),
        object: Some(crate::sandbox::source::SourceObject {
            bucket: "sources".into(),
            name: "source.zip".into(),
            generation: "1".into(),
        }),
    };
    assert!(
        builds
            .build("project", "runtime", "west", &source)
            .await
            .is_err()
    );
    assert!(store.entries.lock().unwrap().is_empty());
    assert!(!builds.cached("project", "runtime", &source).await?);
    Ok(())
}
#[async_trait]
impl BuildExecutor for Workers {
    async fn build(&self, _: &str, request: &BuildRequest) -> Result<BuildReply> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(BuildReply {
            manifest: crate::artifacts::ArtifactManifest {
                bucket: request.bucket.clone(),
                files: vec![crate::artifacts::ArtifactFile {
                    path: "actors.mjs".into(),
                    object: format!("{}actors.mjs", request.artifact_prefix),
                    generation: 1,
                    sha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                }],
            },
            contract: serde_json::json!({"version": 1, "actors": []}),
            timings: Default::default(),
            dependency_cache_hit: false,
        })
    }
}

#[tokio::test]
async fn repeated_source_reuses_compiled_code_without_a_worker_and_isolates_projects() -> Result<()>
{
    let store = Arc::new(Store {
        entries: Mutex::new(Default::default()),
    });
    let executor = Arc::new(Workers(AtomicUsize::new(0)));
    let builds = SourceBuilds {
        bucket: "code-bucket".into(),
        store,
        executor: executor.clone(),
    };
    let source = SourceArchive {
        sha256: "a".repeat(64),
        entrypoint: "src/actor.ts".into(),
        object: Some(crate::sandbox::source::SourceObject {
            bucket: "source-bucket".into(),
            name: "source.zip".into(),
            generation: "1".into(),
        }),
    };
    assert!(!builds.cached("one", "runtime", &source).await?);
    let first = builds.build("one", "runtime", "region", &source).await?;
    let mut cached = source.clone();
    cached.object = None;
    assert!(builds.cached("one", "runtime", &cached).await?);
    assert_eq!(
        builds
            .build("one", "runtime", "region", &cached)
            .await?
            .source_archive,
        source.clone()
    );
    assert_eq!(
        builds
            .build("one", "runtime", "region", &cached)
            .await?
            .code_snapshot,
        first.code_snapshot
    );
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    assert!(
        builds
            .build("two", "runtime", "region", &cached)
            .await
            .is_err()
    );
    builds.build("two", "runtime", "region", &source).await?;
    builds
        .build("one", "new-runtime", "region", &source)
        .await?;
    assert_eq!(executor.0.load(Ordering::SeqCst), 3);
    Ok(())
}

#[test]
fn build_storage_grants_only_one_source_and_scoped_outputs() -> Result<()> {
    let source = SourceArchive {
        sha256: "a".repeat(64),
        entrypoint: "src/actor.ts".into(),
        object: Some(crate::sandbox::source::SourceObject {
            bucket: "source-bucket".into(),
            name: "source.zip".into(),
            generation: "1".into(),
        }),
    };
    let boundary = build_boundary("code-bucket", &source, "artifacts/job/", "deps/project/")?;
    let rules = boundary["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 3);
    assert_eq!(
        rules[0]["availablePermissions"],
        serde_json::json!(["inRole:roles/storage.objectViewer"])
    );
    assert_eq!(
        rules[0]["availabilityCondition"]["expression"],
        "resource.name == \"projects/_/buckets/source-bucket/objects/source.zip\""
    );
    assert_eq!(
        rules[1]["availablePermissions"],
        serde_json::json!(["inRole:roles/storage.objectCreator"])
    );
    assert!(
        rules[2]["availabilityCondition"]["expression"]
            .as_str()
            .unwrap()
            .contains("deps/project/")
    );
    Ok(())
}
