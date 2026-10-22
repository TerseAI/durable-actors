use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Default)]
struct Store(
    Mutex<HashMap<String, String>>,
    Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
);
#[async_trait]
impl SnapshotStore for Store {
    async fn lock(&self, key: &str) -> Result<Box<dyn Send>> {
        let lock = self
            .1
            .lock()
            .unwrap()
            .entry(key.into())
            .or_default()
            .clone();
        Ok(Box::new(lock.lock_owned().await))
    }
    async fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, key: &str, image: &str) -> Result<()> {
        self.0.lock().unwrap().insert(key.into(), image.into());
        Ok(())
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}
#[derive(Default)]
struct Backend {
    calls: AtomicUsize,
    fail: AtomicBool,
    block: AtomicBool,
    started: tokio::sync::Notify,
    finish: tokio::sync::Notify,
}
#[async_trait]
impl SnapshotBackend for Backend {
    async fn create(&self, _: &SandboxDeployment) -> Result<Image> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.block.load(Ordering::SeqCst) {
            self.started.notify_one();
            self.finish.notified().await;
        }
        tokio::task::yield_now().await;
        ensure!(!self.fail.load(Ordering::SeqCst), "staging failed");
        Ok(Image::from_id(format!("im-code-{n}")))
    }
}
fn deployment() -> SandboxDeployment {
    SandboxDeployment { project_id: "project".into(), image_ref: "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(), code_snapshot: Some(crate::sandbox::testing::code_artifact(1)), canonical_region: "us".into() }
}

#[tokio::test]
async fn snapshot_lookup_never_starts_or_waits_for_preparation() -> Result<()> {
    let backend = Arc::new(Backend::default());
    let store = Arc::new(Store::default());
    let cache = Arc::new(Snapshots::new("app".into(), backend.clone(), store.clone()));
    assert!(cache.get(&deployment()).await?.is_none());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    backend.block.store(true, Ordering::SeqCst);
    let preparing = cache.clone();
    let task = tokio::spawn(async move { preparing.prepare(&deployment()).await });
    backend.started.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cache.get(&deployment()))
            .await??
            .is_none()
    );
    backend.finish.notify_one();
    task.await??;
    assert_eq!(
        cache.get(&deployment()).await?.unwrap().image_id(),
        Some("im-code-0")
    );
    let mut newer = deployment();
    newer.code_snapshot = Some(crate::sandbox::testing::code_artifact(2));
    assert!(cache.get(&newer).await?.is_none());
    let restarted = Snapshots::new("app".into(), backend.clone(), store);
    assert!(restarted.get(&deployment()).await?.is_some());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
async fn distinct_snapshots_start_concurrently() -> Result<()> {
    let backend = Arc::new(Backend::default());
    backend.block.store(true, Ordering::SeqCst);
    let store = Arc::new(Store::default());
    let cache = Arc::new(Snapshots::new("app".into(), backend.clone(), store.clone()));
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..50 {
        let mut deployment = deployment();
        deployment.project_id = format!("project-{n}");
        let cache = cache.clone();
        tasks.spawn(async move { cache.prepare(&deployment).await });
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.calls.load(Ordering::SeqCst) < 50 {
            backend.started.notified().await;
        }
    })
    .await?;
    assert!(store.0.lock().unwrap().is_empty());
    backend.finish.notify_waiters();
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    assert_eq!(store.0.lock().unwrap().len(), 50);
    Ok(())
}

#[tokio::test]
async fn snapshots_deduplicate_and_survive_controller_restart() -> Result<()> {
    let backend = Arc::new(Backend::default());
    let store = Arc::new(Store::default());
    let cache = Snapshots::new("app".into(), backend.clone(), store.clone());
    let other_controller = Snapshots::new("app".into(), backend.clone(), store.clone());
    let deployment = deployment();
    let images = futures_util::future::try_join_all((0..50).map(|n| {
        let cache = if n % 2 == 0 {
            &cache
        } else {
            &other_controller
        };
        cache.prepare(&deployment)
    }))
    .await?;
    assert!(images.iter().all(|i| i.image_id() == Some("im-code-0")));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let restarted = Snapshots::new("app".into(), backend.clone(), store.clone());
    restarted.prepare(&deployment).await?;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    restarted.invalidate(&deployment).await?;
    restarted.prepare(&deployment).await?;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    Ok(())
}
#[tokio::test]
async fn failed_preparation_is_retryable_and_keys_bind_the_complete_deployment() -> Result<()> {
    let backend = Arc::new(Backend::default());
    let store = Arc::new(Store::default());
    let cache = Snapshots::new("app".into(), backend.clone(), store.clone());
    backend.fail.store(true, Ordering::SeqCst);
    assert!(cache.prepare(&deployment()).await.is_err());
    assert!(store.0.lock().unwrap().is_empty());
    backend.fail.store(false, Ordering::SeqCst);
    cache.prepare(&deployment()).await?;
    let first = snapshot_key("app", &deployment())?;
    for field in ["image", "code", "tenant"] {
        let mut changed = deployment();
        match field {
            "image" => changed.image_ref.push('b'),
            "code" => changed.code_snapshot = Some(crate::sandbox::testing::code_artifact(2)),
            _ => changed.project_id.push('b'),
        }
        assert_ne!(first, snapshot_key("app", &changed)?);
    }
    assert_ne!(first, snapshot_key("another-app", &deployment())?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires Modal authentication and MODAL_SNAPSHOT_TEST_IMAGE_ID containing Bun and jq"]
async fn live_stage_mount_and_execute_binary_dependency() -> Result<()> {
    let image_id = match std::env::var("MODAL_SNAPSHOT_TEST_IMAGE_ID") {
        Ok(id) => id,
        Err(std::env::VarError::NotPresent) => {
            eprintln!("Skipping live Modal test: MODAL_SNAPSHOT_TEST_IMAGE_ID is not set");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let client = Client::from_env()?;
    let app = client
        .app_from_name(
            "durable-actors-snapshot-integration-test",
            AppLookupParams {
                create_if_missing: true,
                ..Default::default()
            },
        )
        .await?;
    let image = Image::from_id(image_id);
    let create = || {
        client.create_sandbox(
            &app,
            &image,
            SandboxCreateParams {
                command: vec!["/bin/sleep".into(), "300".into()],
                cpu: Some(0.5),
                memory_mib: Some(512),
                timeout: Some(Duration::from_secs(300)),
                cloud: Some("gcp".into()),
                regions: vec!["us-west".into()],
                ..Default::default()
            },
        )
    };
    let source = tokio::time::timeout(STARTUP_TIMEOUT, create()).await??;
    let _source_cleanup = SandboxCleanup(Some(source.clone()));
    let root = tempfile::tempdir()?;
    std::fs::write(
        root.path().join("actors.mjs"),
        "import {execFileSync} from 'node:child_process'; console.log(execFileSync('/usr/bin/jq',['-n','41 + 1'],{encoding:'utf8'}).trim())",
    )?;
    let bundle = ActorBundle::decode(&crate::sandbox::testing::code_artifact(1))?;
    let snapshot = stage(&source, archive(root.path(), &bundle)?).await?;
    let destination = tokio::time::timeout(STARTUP_TIMEOUT, create()).await??;
    let _destination_cleanup = SandboxCleanup(Some(destination.clone()));
    destination.mount_image("/customer", &snapshot).await?;
    let result = destination
        .exec(["bun", "/customer/actors.mjs"], ExecParams::default())
        .await?
        .output()
        .await?;
    ensure!(result.exit_code == 0, "actor failed: {:?}", result.stderr);
    assert_eq!(String::from_utf8(result.stdout)?.trim(), "42");
    source.terminate().await?;
    destination.terminate().await?;
    Ok(())
}
