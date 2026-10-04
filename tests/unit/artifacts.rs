use super::*;
use futures_util::stream;

fn artifact(path: &str, bytes: &[u8]) -> ArtifactFile {
    ArtifactFile {
        path: path.into(),
        object: "compiled/actors.mjs".into(),
        generation: 123,
        sha256: checksum(bytes),
    }
}

#[tokio::test]
async fn restored_code_is_reused_only_when_its_digest_matches_the_assignment() -> Result<()> {
    use axum::{Router, routing::get};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let routes = Router::new().route(
        "/storage/v1/b/code-bucket/o/{*key}",
        get(move || {
            let observed = observed.clone();
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                (
                    [
                        ("x-goog-generation", "123"),
                        ("content-type", "application/octet-stream"),
                    ],
                    "new code",
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let storage = Storage::builder()
        .with_endpoint(format!("http://{}", listener.local_addr()?))
        .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
        .build()
        .await?;
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let root = tempfile::tempdir()?;
    let manifest = ArtifactManifest {
        bucket: "code-bucket".into(),
        files: vec![artifact("actors.mjs", b"new code")],
    };
    tokio::fs::write(root.path().join("actors.mjs"), b"new code").await?;
    manifest.install(root.path(), &storage).await?;
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "matching snapshot code must not be downloaded"
    );
    tokio::fs::write(root.path().join("actors.mjs"), b"older deployment").await?;
    manifest.install(root.path(), &storage).await?;
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"new code"
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn streamed_code_is_published_only_after_its_digest_matches() -> Result<()> {
    let root = tempfile::tempdir()?;
    tokio::fs::write(root.path().join("actors.mjs"), b"old").await?;
    let file = artifact("actors.mjs", b"new code");
    assert!(
        install_file(
            root.path(),
            &file,
            stream::iter([Ok(Bytes::from_static(b"wrong"))])
        )
        .await
        .is_err()
    );
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"old"
    );
    install_file(
        root.path(),
        &file,
        stream::iter([
            Ok(Bytes::from_static(b"new ")),
            Ok(Bytes::from_static(b"code")),
        ]),
    )
    .await?;
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"new code"
    );
    Ok(())
}

#[tokio::test]
async fn artifact_paths_cannot_escape_the_sandbox_code_directory() -> Result<()> {
    let root = tempfile::tempdir()?;
    for path in ["../outside.mjs", "/outside.mjs", "nested/../../outside.mjs"] {
        assert!(
            install_file(
                root.path(),
                &artifact(path, b"code"),
                stream::iter([Ok(Bytes::from_static(b"code"))])
            )
            .await
            .is_err()
        );
    }
    Ok(())
}

fn checksum(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(aws_lc_rs::digest::digest(&SHA256, bytes).as_ref())
}

#[test]
fn python_code_manifests_include_the_entrypoint_and_installed_dependencies() -> Result<()> {
    let manifest = ArtifactManifest {
        bucket: "code-bucket".into(),
        files: vec![
            artifact("actors.pyz", b"code"),
            artifact("python/dependency/module.so", b"native"),
        ],
    };
    let decoded = ArtifactManifest::decode(&manifest.encode()?)?;
    assert_eq!(decoded.entrypoint()?, "actors.pyz");
    assert_eq!(decoded.files.len(), 2);
    Ok(())
}
