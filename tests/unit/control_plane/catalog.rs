use super::*;
use std::sync::Mutex;

#[derive(Default)]
struct Store(Mutex<Option<Value>>);
#[async_trait]
impl DeploymentCatalog for Store {
    async fn get(&self, _: &str) -> Result<Option<Value>> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn list(&self) -> Result<Vec<Value>> {
        Ok(self.0.lock().unwrap().clone().into_iter().collect())
    }
    async fn publish(&self, _: &str, document: Value) -> Result<bool> {
        *self.0.lock().unwrap() = Some(document);
        Ok(true)
    }
    async fn remove(&self, _: &str) -> Result<()> {
        self.0.lock().unwrap().take();
        Ok(())
    }
}

#[tokio::test]
async fn catalog_round_trips_validated_deployments_and_rejects_mismatched_records() -> Result<()> {
    let store = Arc::new(Store::default());
    let registry = CatalogRegistry(store.clone());
    let mut spec = HostLaunchSpec {
        project_id: "project".into(),
        source: None,
        image_ref: "im-runtime".into(),
        code_snapshot: None,
        working_directory: "/customer".into(),
        actor_entrypoint: Some("actors.mjs".into()),
        secret_refs: vec![],
    };
    let contract = PublicActorContract::new(
        serde_json::json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}}),
    )?;
    assert!(
        registry
            .register_deployment(&spec, Some(&contract))
            .await
            .is_err()
    );
    assert!(store.0.lock().unwrap().is_none());
    spec.code_snapshot = Some("im-code".into());
    registry.register_deployment(&spec, Some(&contract)).await?;
    assert_eq!(registry.launch_spec("project").await?, Some(spec.clone()));
    assert_eq!(registry.launch_specs().await?, vec![spec]);
    assert!(registry.launch_spec("other").await.is_err());
    store.0.lock().unwrap().as_mut().unwrap()["contract"]["contractHash"] = "wrong".into();
    assert!(registry.launch_spec("project").await.is_err());
    registry.remove_deployment("project").await?;
    assert!(registry.launch_spec("project").await?.is_none());
    Ok(())
}
