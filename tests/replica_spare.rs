use anyhow::{Result, ensure};
use little_actors::{
    bucket::{GrpcReplicaPeers, ReplicaPeers},
    clock::{Clock, SystemClock},
    replication::{ReplicaAccess, ReplicaGrant, ReplicaScope, ReplicaStream, ReplicaTarget},
    state_log::StateSnapshot,
    state_transport::{GrpcStateTransport, StateTransport},
};
use std::{net::TcpListener, sync::Arc, time::Duration};

#[tokio::test]
async fn a_replica_can_listen_before_assignment_and_retries_cannot_reassign_it() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = TcpListener::bind("127.0.0.1:0")?;
    let control = TcpListener::bind("127.0.0.1:0")?;
    let storage_address = storage.local_addr()?;
    let control_address = control.local_addr()?;
    drop((storage, control));
    let token = "test-spare-token-with-at-least-32-bytes";
    let ready = directory.path().join("ready");
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_little-actors"))
        .env_clear()
        .env("DURABLE_OBJECT_PROCESS_ROLE", "replica")
        .env("DURABLE_OBJECT_SPARE_TOKEN", token)
        .env("DURABLE_OBJECT_SPARE_READY_FILE", &ready)
        .env("DURABLE_OBJECT_REPLICA_DATA", directory.path().join("data"))
        .env("DURABLE_OBJECT_HOST_BIND", storage_address.to_string())
        .env("DURABLE_OBJECT_SPARE_BIND", control_address.to_string())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready.exists() {
            ensure!(
                child.try_wait()?.is_none(),
                "unassigned replica exited before becoming ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    let client = reqwest::Client::new();
    assert!(
        client
            .get(format!("http://{storage_address}/health"))
            .send()
            .await?
            .status()
            .is_success()
    );
    let url = format!("http://{control_address}/assign");
    let mut assignment = serde_json::json!({"hostId":"replica-one", "secret":"replica-signing-secret-with-32-bytes", "scope":{"actor":{"actor_type":"Counter","actor_id":"one"},"host":"primary","session":"session","region":"us-east"}});
    let scope: ReplicaScope = serde_json::from_value(assignment["scope"].clone())?;
    let access = ReplicaAccess::new(
        assignment["secret"].as_str().unwrap(),
        Arc::new(SystemClock),
    );
    let peers = GrpcReplicaPeers::new(access.clone())?;
    let target = ReplicaTarget {
        host_id: "replica-one".into(),
        region: scope.region.clone(),
        url: format!("http://{storage_address}"),
    };
    assert!(peers.initialize(&target, &scope.identity()).await.is_err());
    assert_eq!(
        client.post(&url).json(&assignment).send().await?.status(),
        401
    );
    for _ in 0..2 {
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(token)
                .json(&assignment)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::NO_CONTENT
        );
    }
    let stream = ReplicaStream {
        session: scope.identity(),
        prefix: format!(
            "{}1/",
            little_actors::storage_paths::snapshots(&scope.actor)?
        ),
        owner_epoch: 1,
        base_version: 0,
    };
    let append = access.url(
        &target.url,
        &ReplicaGrant {
            stream: Some(stream.clone()),
            operation: "APPEND".into(),
            object: stream.prefix.clone(),
            region: target.region.clone(),
            host_id: target.host_id.clone(),
            expires_at_ms: SystemClock.now_ms()? + 60_000,
        },
    )?;
    let bytes = StateSnapshot::new(
        1,
        1,
        "first".into(),
        serde_json::json!({"count":1}),
        serde_json::json!(1),
    )?
    .encode()?;
    GrpcStateTransport::new().write(&append, bytes).await?;
    assert_eq!(
        peers
            .head(&target, &stream)
            .await?
            .latest
            .unwrap()
            .state_version,
        1
    );
    assert!(peers.initialize(&target, "another-session").await.is_err());
    assignment["scope"]["actor"]["actor_id"] = "another".into();
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(token)
            .json(&assignment)
            .send()
            .await?
            .status(),
        409
    );
    child.kill().await?;
    Ok(())
}
