use super::{FileBucket, LoadedActor, RuntimeStorage, RuntimeStorageReader};
use crate::{
    actor::ActorKey,
    clock::SystemClock,
    host::HostId,
    ownership::{ActorClaim, BucketOwnership, Claim, Ownerships},
};
use anyhow::Result;
use std::sync::Arc;

/// A host's storage and its control plane's ownership over one directory.
pub(crate) struct RuntimeFixture {
    pub directory: tempfile::TempDir,
    pub runtime: Arc<RuntimeStorage>,
    pub ownerships: Arc<Ownerships>,
}
impl RuntimeFixture {
    pub fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(FileBucket::new(directory.path().into())?);
        let runtime = Arc::new(RuntimeStorage::new(bucket.clone(), Arc::new(SystemClock))?);
        let reader = RuntimeStorageReader::new(
            bucket.clone(),
            Arc::new(SystemClock),
            Arc::new(crate::litestream::EmbeddedRestore),
        )?;
        let ownerships = Arc::new(Ownerships::new(
            Arc::new(BucketOwnership::new(bucket)),
            Arc::new(reader),
        ));
        Ok(Self {
            directory,
            runtime,
            ownerships,
        })
    }

    pub async fn claim(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        region: &str,
    ) -> Result<ActorClaim> {
        match self
            .ownerships
            .claim(actor, host, session, region, None)
            .await?
        {
            Claim::Won(claim) => Ok(claim),
            Claim::Held => anyhow::bail!("actor is held by another host"),
        }
    }

    /// Claims the actor and starts the session on this fixture's host storage.
    pub async fn activate(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<LoadedActor> {
        let claim = self
            .claim(actor, host, session, "north-america-east")
            .await?;
        self.runtime
            .adopt(&claim, &super::CachedBase::default())
            .await
    }
}
