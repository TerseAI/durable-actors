use super::{FileBucket, GrpcReplicaPeers, RuntimeStorage};
use crate::{
    clock::SystemClock,
    replication::{ReplicaAccess, ReplicaSet},
};
use anyhow::Result;
use std::sync::Arc;

pub(crate) struct RuntimeFixture {
    pub directory: tempfile::TempDir,
    pub bucket: Arc<FileBucket>,
    pub runtime: Arc<RuntimeStorage>,
    pub access: ReplicaAccess,
}

impl RuntimeFixture {
    pub fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(FileBucket::new(directory.path().into())?);
        let access = ReplicaAccess::new("test-secret", Arc::new(SystemClock));
        let runtime = Arc::new(RuntimeStorage::new(
            bucket.clone(),
            Arc::new(ReplicaSet::default()),
            Arc::new(GrpcReplicaPeers::new(access.clone())?),
            access.clone(),
            "http://unused".into(),
            std::sync::Arc::new(crate::clock::SystemClock),
        )?);
        Ok(Self {
            directory,
            bucket,
            runtime,
            access,
        })
    }
}
