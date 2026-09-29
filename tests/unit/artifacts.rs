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
async fn streaming_does_not_impose_a_32_mib_bundle_limit() -> Result<()> {
    let root = tempfile::tempdir()?;
    let chunk = Bytes::from(vec![b'x'; 1024 * 1024]);
    let bytes = vec![b'x'; 33 * 1024 * 1024];
    let file = artifact("actors.mjs", &bytes);
    install_file(
        root.path(),
        &file,
        stream::iter((0..33).map(|_| Ok(chunk.clone()))),
    )
    .await?;
    assert_eq!(
        tokio::fs::metadata(root.path().join("actors.mjs"))
            .await?
            .len(),
        bytes.len() as u64
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
