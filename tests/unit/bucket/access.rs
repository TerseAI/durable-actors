use super::*;

#[tokio::test]
async fn host_bootstrap_carries_replica_membership_without_a_separate_count() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let replicas: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|id| ReplicaTarget {
            host_id: id.into(),
            url: format!("https://{id}.example"),
            region: "north-america-east".into(),
        })
        .collect();
    let access = RuntimeAccess::new(
        BucketLocation::File {
            directory: directory.path().into(),
        },
        Arc::new(crate::replication::ReplicaSet(replicas.clone())),
        ReplicaAccess::new("secret", Arc::new(SystemClock)),
    )?;
    let document = access.bootstrap("default", "north-america-east").await?;
    let value: Value = serde_json::from_str(&document)?;
    assert!(value.get("replicaCount").is_none());
    let config: HostStorageConfig = serde_json::from_str(&document)?;
    assert_eq!(config.replicas, replicas);
    let partial = RuntimeAccess::new(
        BucketLocation::File {
            directory: directory.path().into(),
        },
        Arc::new(PartialFleet(crate::replication::ReplicaSet(replicas))),
        ReplicaAccess::new("secret", Arc::new(SystemClock)),
    )?;
    assert!(
        partial
            .bootstrap("default", "north-america-east")
            .await
            .is_err()
    );
    Ok(())
}

struct PartialFleet(crate::replication::ReplicaSet);

#[async_trait::async_trait]
impl ReplicaProvisioner for PartialFleet {
    fn replica_regions(&self) -> Vec<String> {
        self.0.replica_regions()
    }

    async fn ensure(&self, actor: &ActorKey, region: &str) -> Result<Vec<ReplicaTarget>> {
        let mut replicas = self.0.ensure(actor, region).await?;
        replicas.pop();
        Ok(replicas)
    }
}

#[test]
fn one_bucket_scopes_mutable_metadata_and_immutable_snapshots_separately() -> Result<()> {
    let boundary = boundary("actors");
    let rules = boundary["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0]["availableResource"], rules[1]["availableResource"]);
    assert!(
        rules[0]["availabilityCondition"]["expression"]
            .as_str()
            .unwrap()
            .contains("little-actors/v3/owners/")
    );
    assert_eq!(
        rules[1]["availablePermissions"],
        json!([
            "inRole:roles/storage.objectViewer",
            "inRole:roles/storage.objectCreator"
        ])
    );
    assert!(
        rules[1]["availabilityCondition"]["expression"]
            .as_str()
            .unwrap()
            .contains("little-actors/v3/snapshots/")
    );
    Ok(())
}
