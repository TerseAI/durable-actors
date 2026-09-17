use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;

use super::admin::{AdminRegistry, HostLaunchSpec};
use crate::bucket::{Bucket, component, replace};

pub(super) struct RuntimeRegistry {
    admin: Arc<dyn AdminRegistry>,
    bucket: Arc<dyn Bucket>,
}

impl RuntimeRegistry {
    pub fn new(admin: Arc<dyn AdminRegistry>, bucket: Arc<dyn Bucket>) -> Self {
        Self { admin, bucket }
    }

    pub fn admin_view(self: Arc<Self>) -> Arc<dyn AdminRegistry> {
        Arc::new(PublishedAdmin(self))
    }

    async fn publish(&self, namespace: &str) -> Result<()> {
        let key = key(namespace);
        for _ in 0..16 {
            // Read the bucket generation before PostgreSQL so a late publisher cannot roll back a newer deployment.
            let current = self.bucket.get(&key).await?;
            let latest = self.admin.launch_spec(namespace).await?;
            if replace(
                self.bucket.as_ref(),
                &key,
                current.map(|object| object.generation),
                serde_json::to_vec(&latest)?,
            )
            .await?
            {
                return Ok(());
            }
        }
        anyhow::bail!("deployment changed repeatedly during publication; retry the deployment")
    }
}

struct PublishedAdmin(Arc<RuntimeRegistry>);

#[async_trait]
impl AdminRegistry for PublishedAdmin {
    async fn ensure_namespace_and_register_deployment(
        &self,
        spec: &HostLaunchSpec,
    ) -> Result<bool> {
        self.0.ensure_namespace_and_register_deployment(spec).await
    }
    async fn launch_spec(&self, namespace: &str) -> Result<Option<HostLaunchSpec>> {
        self.0.admin.launch_spec(namespace).await
    }
    async fn remove_deployment(&self, namespace: &str) -> Result<()> {
        self.0.remove_deployment(namespace).await
    }
}

#[async_trait]
impl AdminRegistry for RuntimeRegistry {
    async fn ensure_namespace_and_register_deployment(
        &self,
        spec: &HostLaunchSpec,
    ) -> Result<bool> {
        let changed = self
            .admin
            .ensure_namespace_and_register_deployment(spec)
            .await?;
        self.publish(&spec.namespace_id)
            .await
            .context("publish runtime deployment; retry this deployment if publication failed")?;
        Ok(changed)
    }

    async fn launch_spec(&self, namespace: &str) -> Result<Option<HostLaunchSpec>> {
        match self.bucket.get(&key(namespace)).await? {
            Some(object) => Ok(serde_json::from_slice(&object.bytes)?),
            None => Ok(None),
        }
    }

    async fn remove_deployment(&self, namespace: &str) -> Result<()> {
        self.admin.remove_deployment(namespace).await?;
        self.publish(namespace).await
    }
}

fn key(namespace: &str) -> String {
    format!("runtime/deployments/{}.json", component(namespace))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::{Bucket, BucketObject};
    use crate::control_plane::admin::{AdminRegistry, HostLaunchSpec};
    use anyhow::Result;
    use async_trait::async_trait;
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    #[derive(Default)]
    struct MemoryBucket(Mutex<HashMap<String, BucketObject>>);
    #[async_trait]
    impl Bucket for MemoryBucket {
        async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            expected: Option<i64>,
            bytes: Vec<u8>,
        ) -> Result<bool> {
            let mut objects = self.0.lock().unwrap();
            let current = objects.get(key).map(|o| o.generation);
            if current != expected {
                return Ok(false);
            }
            objects.insert(
                key.into(),
                BucketObject {
                    generation: current.unwrap_or(0) + 1,
                    bytes,
                },
            );
            Ok(true)
        }
        async fn list(&self, _: &str) -> Result<Vec<String>> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct Admin {
        unavailable: AtomicBool,
        spec: Mutex<Option<HostLaunchSpec>>,
    }
    #[async_trait]
    impl AdminRegistry for Admin {
        async fn ensure_namespace_and_register_deployment(
            &self,
            spec: &HostLaunchSpec,
        ) -> Result<bool> {
            *self.spec.lock().unwrap() = Some(spec.clone());
            Ok(true)
        }
        async fn launch_spec(&self, _: &str) -> Result<Option<HostLaunchSpec>> {
            anyhow::ensure!(
                !self.unavailable.load(Ordering::SeqCst),
                "PostgreSQL unavailable"
            );
            Ok(self.spec.lock().unwrap().clone())
        }
        async fn remove_deployment(&self, _: &str) -> Result<()> {
            *self.spec.lock().unwrap() = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn published_deployments_survive_a_restart_without_postgres() -> Result<()> {
        let admin = Arc::new(Admin::default());
        let bucket = Arc::new(MemoryBucket::default());
        let registry = RuntimeRegistry::new(admin.clone(), bucket.clone());
        let spec = HostLaunchSpec {
            namespace_id: "project".into(),
            code_revision: "v1".into(),
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
            socket_gateway_url: None,
        };
        registry
            .ensure_namespace_and_register_deployment(&spec)
            .await?;
        admin.unavailable.store(true, Ordering::SeqCst);
        let restarted = RuntimeRegistry::new(admin.clone(), bucket.clone());
        assert_eq!(restarted.launch_spec("project").await?, Some(spec));
        admin.unavailable.store(false, Ordering::SeqCst);
        restarted.remove_deployment("project").await?;
        admin.unavailable.store(true, Ordering::SeqCst);
        assert!(
            RuntimeRegistry::new(admin, bucket)
                .launch_spec("project")
                .await?
                .is_none()
        );
        Ok(())
    }
}
