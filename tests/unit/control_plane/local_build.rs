use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Compiler {
    calls: AtomicUsize,
    fail: AtomicBool,
}

#[async_trait]
impl LocalCodeCompiler for Compiler {
    async fn compile(&self, _: &Path, _: &str, output: &Path) -> Result<PublicActorContract> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ensure!(!self.fail.load(Ordering::SeqCst), "invalid actor source");
        tokio::fs::write(output.join("actors.mjs"), "compiled actors").await?;
        PublicActorContract::new(serde_json::json!({"version": 1, "actors": []}))
    }
}

#[tokio::test]
async fn builds_preserve_the_current_code_until_successful_publication_and_keep_only_latest()
-> Result<()> {
    let root = tempfile::tempdir()?;
    let compiler = Arc::new(Compiler::default());
    let builds = LocalBuilds::new(
        root.path().into(),
        root.path().join("code"),
        compiler.clone(),
    );
    let source = source(root.path());
    let first = builds.prepare(&source).await?;
    let first_path = PathBuf::from(first.spec.actor_entrypoint.as_ref().unwrap());
    assert_eq!(
        first.spec.source.as_ref().unwrap().actor_entrypoint,
        source.actor_entrypoint
    );
    assert!(first.contract.is_some());
    first.commit().await;
    compiler.fail.store(true, Ordering::SeqCst);
    assert!(builds.prepare(&source).await.is_err());
    assert!(first_path.is_file());
    compiler.fail.store(false, Ordering::SeqCst);
    let abandoned = builds.prepare(&source).await?;
    let abandoned_path = PathBuf::from(abandoned.spec.actor_entrypoint.as_ref().unwrap());
    drop(abandoned);
    assert!(!abandoned_path.exists());
    assert!(first_path.is_file());
    let second = builds.prepare(&source).await?;
    let second_path = PathBuf::from(second.spec.actor_entrypoint.as_ref().unwrap());
    assert_ne!(first_path, second_path);
    assert!(first_path.is_file());
    second.commit().await;
    assert!(!first_path.exists());
    assert!(second_path.is_file());
    assert_eq!(
        std::fs::read_dir(root.path().join("code/local"))?.count(),
        1
    );
    assert_eq!(compiler.calls.load(Ordering::SeqCst), 4);
    Ok(())
}

#[tokio::test]
async fn prepared_modules_keep_their_import_resolution_without_running_the_compiler() -> Result<()>
{
    let root = tempfile::tempdir()?;
    let compiler = Arc::new(Compiler::default());
    let builds = LocalBuilds::new(
        root.path().into(),
        root.path().join("code"),
        compiler.clone(),
    );
    let mut source = source(root.path());
    source.actor_entrypoint = Some("actors.mjs".into());
    std::fs::write(root.path().join("actors.mjs"), "export const actors = {}")?;
    let prepared = builds.prepare(&source).await?;
    assert_eq!(
        std::fs::canonicalize(prepared.spec.actor_entrypoint.as_ref().unwrap())?,
        std::fs::canonicalize(root.path().join("actors.mjs"))?
    );
    assert_eq!(
        std::fs::read_to_string(prepared.spec.actor_entrypoint.as_ref().unwrap())?,
        "export const actors = {}"
    );
    assert_eq!(compiler.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

fn source(project: &Path) -> HostLaunchSpec {
    HostLaunchSpec {
        project_id: "local".into(),
        source: None,
        code_snapshot: None,
        image_ref: "local".into(),
        working_directory: project.display().to_string(),
        actor_entrypoint: Some("src/actors.ts".into()),
        secret_refs: vec![],
    }
}
