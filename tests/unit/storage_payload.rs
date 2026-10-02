use crate::payload::{Spool, Text, encode};
use anyhow::Result;
use std::io::Write;

#[test]
fn large_payloads_spill_and_survive_slicing_and_cloning() -> Result<()> {
    let mut spool = Spool::new();
    for n in 0..256u32 {
        spool.write_all(&n.to_le_bytes().repeat(1024))?;
    }
    let path = spool.file.as_ref().unwrap().path().to_owned();
    let bytes = spool.finish()?;
    let tail = bytes.slice(255 * 4096..);
    drop(bytes);
    assert_eq!(tail.as_ref(), 255u32.to_le_bytes().repeat(1024));
    assert!(path.exists());
    drop(tail);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn large_base64_text_roundtrips_and_clones() -> Result<()> {
    let input = "YWJj".repeat(256 * 1024);
    let json = format!("\"{input}\"");
    let text: Text = serde_json::from_slice(json.as_bytes())?;
    let copy = text.clone();
    drop(text);
    assert_eq!(encode(&copy)?.as_ref(), json.as_bytes());
    Ok(())
}

#[tokio::test]
async fn uploads_have_bounded_chunks_and_replay_the_requested_offset() -> Result<()> {
    use google_cloud_storage::streaming_source::{Seek, StreamingSource};
    let bytes = bytes::Bytes::from(vec![17; 1024 * 1024 + 7]);
    let mut upload = crate::payload::Upload::new(bytes.clone());
    let mut received = Vec::new();
    while let Some(chunk) = upload.next().await {
        let chunk = chunk?;
        assert!(chunk.len() <= 256 * 1024);
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received.as_slice(), bytes.as_ref());
    upload.seek(1024 * 1024).await?;
    assert_eq!(upload.next().await.unwrap()?.as_ref(), &[17; 7]);
    assert!(upload.next().await.is_none());
    Ok(())
}
