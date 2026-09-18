use super::{Bucket, BucketObject};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct FileBucket {
    directory: PathBuf,
}

impl FileBucket {
    pub fn new(directory: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&directory)?;
        Ok(Self {
            directory: directory.canonicalize()?,
        })
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Path) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || operation(&directory)).await?
    }
}

#[async_trait]
impl Bucket for FileBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        let key = key.to_owned();
        self.run(move |directory| read(&object_path(directory, &key)?))
            .await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        let key = key.to_owned();
        self.run(move |directory| {
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(directory.join(".bucket.lock"))?;
            lock.lock()?;
            let path = object_path(directory, &key)?;
            let generation = read(&path)?.map(|object| object.generation);
            if generation != expected {
                return Ok(false);
            }
            let next = generation
                .unwrap_or(0)
                .checked_add(1)
                .context("bucket generation overflow")?;
            let parent = path.parent().context("object parent missing")?;
            std::fs::create_dir_all(parent)?;
            let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
            temporary.write_all(&next.to_be_bytes())?;
            temporary.write_all(&bytes)?;
            temporary.as_file().sync_all()?;
            temporary.persist(&path)?;
            File::open(parent)?.sync_all()?;
            Ok(true)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.run(move |directory| {
            let mut objects = Vec::new();
            list(directory, directory, &prefix, &mut objects)?;
            objects.sort();
            Ok(objects)
        })
        .await
    }
}

fn object_path(directory: &Path, key: &str) -> Result<PathBuf> {
    ensure!(
        !key.is_empty()
            && key
                .split('/')
                .all(|part| !part.is_empty() && !part.starts_with('.') && !part.contains('\\')),
        "invalid bucket object path"
    );
    Ok(directory.join(key))
}

fn read(path: &Path) -> Result<Option<BucketObject>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut generation = [0; 8];
    file.read_exact(&mut generation)
        .context("incomplete bucket object")?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(Some(BucketObject {
        generation: i64::from_be_bytes(generation),
        bytes,
    }))
}

fn list(root: &Path, directory: &Path, prefix: &str, objects: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        if entry.file_type()?.is_dir() {
            list(root, &entry.path(), prefix, objects)?;
        } else if entry.file_type()?.is_file() {
            let key = entry
                .path()
                .strip_prefix(root)?
                .to_string_lossy()
                .into_owned();
            if key.starts_with(prefix) {
                objects.push(key);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
}
