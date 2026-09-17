use super::{BucketHostLeases, FileBucket, HttpReplicaPeers, RuntimeStorage};
use crate::{
    clock::SystemClock,
    replication::{ReplicaAccess, ReplicaSet},
};
use anyhow::Result;
use std::sync::Arc;

pub(crate) struct RuntimeFixture {
    pub directory: tempfile::TempDir,
    pub bucket: Arc<FileBucket>,
    pub leases: Arc<BucketHostLeases>,
    pub runtime: Arc<RuntimeStorage>,
    pub access: ReplicaAccess,
}

impl RuntimeFixture {
    pub fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(FileBucket::new(directory.path().into())?);
        let leases = Arc::new(BucketHostLeases::new(bucket.clone(), Arc::new(SystemClock)));
        let access = ReplicaAccess::new("test-secret", Arc::new(SystemClock));
        let runtime = Arc::new(RuntimeStorage::new(
            bucket.clone(),
            leases.clone(),
            Arc::new(ReplicaSet::default()),
            Arc::new(HttpReplicaPeers::new(access.clone())?),
            access.clone(),
            "http://unused".into(),
        )?);
        Ok(Self {
            directory,
            bucket,
            leases,
            runtime,
            access,
        })
    }
}
