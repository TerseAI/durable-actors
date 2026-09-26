use anyhow::Result;
use async_trait::async_trait;
use durable_actors::{
    actor::ActorKey,
    regional::{ActorDirectory, DirectoryStore, ObjectAssignment, Region},
};
use std::sync::Arc;
fn actor(project: &str) -> ActorKey {
    ActorKey {
        project_id: project.into(),
        actor_name: "Counter".into(),
        actor_id: "shared".into(),
    }
}

struct Unavailable;
#[async_trait]
impl DirectoryStore for Unavailable {
    async fn by_name(&self, _: &ActorKey) -> Result<Option<ObjectAssignment>> {
        anyhow::bail!("unavailable")
    }
    async fn by_id(&self, _: &str) -> Result<Option<ObjectAssignment>> {
        anyhow::bail!("unavailable")
    }
    async fn create(&self, _: &ObjectAssignment) -> Result<ObjectAssignment> {
        panic!("read failure must never become creation")
    }
}
#[tokio::test]
async fn directory_errors_do_not_become_missing_assignments() {
    let directory = ActorDirectory::new(Arc::new(Unavailable));
    assert!(
        directory
            .get_or_create(&actor("one"), Region::West)
            .await
            .is_err()
    );
}
