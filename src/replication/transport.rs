use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::task::JoinSet;

use crate::{
    state_transport::{SnapshotWriter, StateTransport, StateWrite},
    storage::WritePlan,
};

use super::{ReplicaStore, ReplicationTicket};

#[derive(Clone)]
pub struct ReplicatedStateTransport {
    bucket: Arc<dyn SnapshotWriter>,
    transport: Arc<dyn StateTransport>,
    local: Arc<dyn ReplicaStore>,
}

impl ReplicatedStateTransport {
    pub fn new(
        bucket: Arc<dyn SnapshotWriter>,
        transport: Arc<dyn StateTransport>,
        local: Arc<dyn ReplicaStore>,
    ) -> Self {
        Self {
            bucket,
            transport,
            local,
        }
    }
}

#[async_trait]
impl SnapshotWriter for ReplicatedStateTransport {
    async fn write_snapshot(&self, ticket: &WritePlan, bytes: Vec<u8>) -> Result<StateWrite> {
        let Some(replication) = &ticket.replication else {
            return self.bucket.write_snapshot(ticket, bytes).await;
        };
        replication.validate()?;
        let started = Instant::now();
        let bucket = self.bucket.clone();
        let local = self.local.clone();
        let plan = ticket.clone();
        let object = ticket.object_name.clone();
        let bucket_bytes = bytes.clone();
        let bucket_task = tokio::spawn(async move {
            let result = bucket.write_snapshot(&plan, bucket_bytes).await;
            tracing::info!(event = "object_storage_upload", %object, uploaded = result.is_ok(),
                upload_ms = started.elapsed().as_secs_f64() * 1000.0);
            if result.is_ok() {
                let _ = local.archived(&object).await;
            }
            result
        });
        let bucket = async {
            bucket_task
                .await
                .context("object storage upload task failed")?
        };
        let replica_task =
            self.start_replication(ticket.clone(), replication.clone(), bytes, started);
        let replicas = async { replica_task.await.context("replication task failed")? };
        tokio::pin!(bucket, replicas);
        let outcome = tokio::select! {
            result = &mut bucket => match result {
                Ok(proof) => Ok(proof),
                Err(bucket_error) => replicas.await.with_context(|| format!("bucket and replication failed: {bucket_error:#}")),
            },
            result = &mut replicas => match result {
                Ok(proof) => Ok(proof),
                Err(replica_error) => bucket.await.with_context(|| format!("replication and bucket failed: {replica_error:#}")),
            },
        };
        tracing::info!(event = "actor_durability", object = %ticket.object_name,
            durability = "replication", replica_count = replication.replicas.len(),
            proof = match &outcome { Ok(StateWrite::Replicated) => "replicas", Ok(_) => "object_storage", Err(_) => "failed" },
            persistence_ms = started.elapsed().as_secs_f64() * 1000.0);
        outcome
    }
}

impl ReplicatedStateTransport {
    fn start_replication(
        &self,
        ticket: WritePlan,
        replication: ReplicationTicket,
        bytes: Vec<u8>,
        started: Instant,
    ) -> tokio::task::JoinHandle<Result<StateWrite>> {
        let transport = self.clone();
        tokio::spawn(async move {
            let result = transport.replicate(&ticket, &replication, bytes).await;
            tracing::info!(event = "replica_set_persisted", object = %ticket.object_name,
                succeeded = result.is_ok(), replication_ms = started.elapsed().as_secs_f64() * 1000.0);
            result
        })
    }

    async fn replicate(
        &self,
        ticket: &WritePlan,
        replication: &ReplicationTicket,
        bytes: Vec<u8>,
    ) -> Result<StateWrite> {
        let started = Instant::now();
        let local = async {
            self.local
                .put(&ticket.object_name, &replication.archive_url, &bytes)
                .await?;
            tracing::info!(event = "replica_local_sync", object = %ticket.object_name,
                local_sync_ms = started.elapsed().as_secs_f64() * 1000.0, bytes = bytes.len());
            anyhow::Ok(())
        };
        let remote = async {
            let mut writes = JoinSet::new();
            for replica in &replication.replicas {
                let transport = self.transport.clone();
                let url = replica.url.clone();
                let bytes = bytes.clone();
                let host_id = replica.host_id.clone();
                let region = replica.region.clone();
                let object = ticket.object_name.clone();
                writes.spawn(async move {
                    let result = transport.write(&url, bytes).await;
                    tracing::info!(event = "replica_ack", %object, %host_id, %region, acknowledged = result.is_ok(),
                        replica_ms = started.elapsed().as_secs_f64() * 1000.0);
                    result
                });
            }
            while let Some(result) = writes.join_next().await {
                result??;
            }
            anyhow::Ok(())
        };
        tokio::try_join!(local, remote)?;
        Ok(StateWrite::Replicated)
    }
}
