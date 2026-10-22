use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Default)]
struct Store(Mutex<HashMap<String, String>>);
#[async_trait]
impl SnapshotStore for Store {
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
}
#[async_trait]
impl SnapshotBackend for Backend {
    async fn create(&self, _: &SandboxDeployment) -> Result<Image> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        ensure!(!self.fail.load(Ordering::SeqCst), "staging failed");
        Ok(Image::from_id(format!("im-code-{n}")))
    }
}
fn deployment() -> SandboxDeployment {
    SandboxDeployment { project_id: "project".into(), image_ref: "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(), code_snapshot: Some(crate::sandbox::testing::code_artifact(1)), canonical_region: "us".into() }
}
#[tokio::test]
async fn snapshots_deduplicate_and_survive_controller_restart() -> Result<()> {
    let backend = Arc::new(Backend::default());
    let store = Arc::new(Store::default());
    let cache = Snapshots::new("app".into(), backend.clone(), store.clone());
    let deployment = deployment();
    let images =
        futures_util::future::try_join_all((0..50).map(|_| cache.prepare(&deployment))).await?;
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
