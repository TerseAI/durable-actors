use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Backend {
    imports: AtomicUsize,
    fail: AtomicBool,
}

#[async_trait]
impl ImageBackend for Backend {
    async fn import(&self, _: &str) -> Result<Image> {
        self.imports.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        ensure!(
            !self.fail.swap(false, Ordering::SeqCst),
            "registry unavailable"
        );
        Ok(Image::from_id("im-test"))
    }
}

fn reference() -> String {
    format!(
        "us-west4-docker.pkg.dev/project/actors/runtime@sha256:{}",
        "a".repeat(64)
    )
}

#[tokio::test]
async fn concurrent_snapshot_and_spare_preparation_share_an_image_import() -> Result<()> {
    let backend = Arc::new(Backend::default());
    let images = Images::new(backend.clone());
    let reference = reference();
    futures_util::future::try_join_all((0..32).map(|_| images.image(&reference))).await?;
    images.image(&reference).await?;
    assert_eq!(backend.imports.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_imports_are_retryable_and_mutable_tags_are_rejected() -> Result<()> {
    let backend = Arc::new(Backend::default());
    backend.fail.store(true, Ordering::SeqCst);
    let images = Images::new(backend.clone());
    assert!(images.image(&reference()).await.is_err());
    images.image(&reference()).await?;
    assert!(images.image("registry/runtime:latest").await.is_err());
    assert_eq!(backend.imports.load(Ordering::SeqCst), 2);
    Ok(())
}
