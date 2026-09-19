use super::*;
#[tokio::test]
async fn independent_local_clients_share_atomic_generations_across_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let first = FileBucket::new(directory.path().into())?;
    let second = FileBucket::new(directory.path().into())?;
    let key = "runtime/owners/actor.json";
    assert!(
        first
            .compare_and_swap(key, None, b"initial".to_vec())
            .await?
    );
    let generation = first.get(key).await?.unwrap().generation;
    let (one, two) = tokio::join!(
        first.compare_and_swap(key, Some(generation), b"one".to_vec()),
        second.compare_and_swap(key, Some(generation), b"two".to_vec())
    );
    assert_ne!(one?, two?);
    drop(first);
    drop(second);
    let reopened = FileBucket::new(directory.path().into())?;
    let object = reopened.get(key).await?.unwrap();
    assert_eq!(object.generation, generation + 1);
    assert!(object.bytes == b"one" || object.bytes == b"two");
    assert_eq!(reopened.list("runtime/owners/").await?, [key]);
    assert!(reopened.list("other/").await?.is_empty());
    assert!(
        reopened
            .compare_and_swap("../escape", None, vec![])
            .await
            .is_err()
    );
    Ok(())
}
