use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use tokio_util::sync::CancellationToken;

use crate::state_transport::{GrpcStateTransport, StateTransport, StateWrite};

use super::{ReplicaStore, store::PendingSnapshot};

pub fn start_archiver(store: Arc<dyn ReplicaStore>) -> CancellationToken {
    let stop = CancellationToken::new();
    let cancelled = stop.clone();
    tokio::spawn(async move {
        let transport = GrpcStateTransport::new();
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => break,
                _ = interval.tick() => {
                    let result = tokio::select! {
                        _ = cancelled.cancelled() => break,
                        result = archive_batch(store.as_ref(), &transport) => result,
                    };
                    if let Err(error) = result {
                        tracing::warn!(event = "replica_archive_failed", error = %error);
                    }
                }
            }
        }
    });
    stop
}

pub async fn archive_pending(store: &dyn ReplicaStore) -> Result<()> {
    archive_batch(store, &GrpcStateTransport::new()).await
}

async fn archive_batch(store: &dyn ReplicaStore, transport: &dyn StateTransport) -> Result<()> {
    for snapshot in store.pending(4).await? {
        match archive_snapshot(transport, &snapshot).await {
            Ok(()) => {
                store.archived(&snapshot.object).await?;
                tracing::info!(event = "replica_archived", object = %snapshot.object,
                    archive_lag_ms = now_ms().saturating_sub(snapshot.created_at_ms), bytes = snapshot.bytes.len());
            }
            Err(_) => {
                store.attempted(&snapshot.object).await?;
                tracing::warn!(event = "replica_archive_retry", object = %snapshot.object,
                    archive_lag_ms = now_ms().saturating_sub(snapshot.created_at_ms));
            }
        }
    }
    Ok(())
}

async fn archive_snapshot(
    transport: &dyn StateTransport,
    snapshot: &PendingSnapshot,
) -> Result<()> {
    let (channel, token) = crate::grpc::transport::capability(&snapshot.archive_url)?;
    let ticket = crate::grpc::proto::archive_service_client::ArchiveServiceClient::new(channel)
        .prepare(crate::grpc::transport::request(
            crate::grpc::proto::ArchiveRequest {
                object: snapshot.object.clone(),
            },
            &token,
        )?)
        .await?
        .into_inner();
    if transport
        .write(&ticket.write_url, snapshot.bytes.clone())
        .await?
        == StateWrite::AlreadyExists
    {
        let existing = transport.read(&ticket.read_url).await?;
        ensure!(
            existing.as_ref() == snapshot.bytes,
            "archived snapshot conflicts with replica"
        );
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| i64::try_from(time.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::proto;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    struct ArchiveFixture {
        available: Arc<AtomicBool>,
        target: String,
    }

    #[tonic::async_trait]
    impl proto::archive_service_server::ArchiveService for ArchiveFixture {
        async fn prepare(
            &self,
            _: Request<proto::ArchiveRequest>,
        ) -> Result<Response<proto::ArchiveReply>, Status> {
            Ok(Response::new(proto::ArchiveReply {
                write_url: self.target.clone(),
                read_url: self.target.clone(),
            }))
        }
    }
    #[tonic::async_trait]
    impl proto::snapshot_service_server::SnapshotService for ArchiveFixture {
        async fn read(
            &self,
            _: Request<proto::Empty>,
        ) -> Result<Response<proto::SnapshotData>, Status> {
            Ok(Response::new(proto::SnapshotData {
                data: b"snapshot".to_vec(),
            }))
        }
        async fn write(
            &self,
            _: Request<proto::SnapshotData>,
        ) -> Result<Response<proto::SnapshotWriteReply>, Status> {
            if !self.available.load(Ordering::Relaxed) {
                return Err(Status::unavailable("storage unavailable"));
            }
            Ok(Response::new(proto::SnapshotWriteReply {
                already_exists: true,
            }))
        }
    }

    #[tokio::test]
    async fn failed_archival_keeps_the_disk_copy_until_storage_is_confirmed() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = super::super::FileReplicaStore::open(directory.path().into(), 4096).await?;
        let available = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let target = format!("grpc://{}?token=test", listener.local_addr()?);
        let fixture = ArchiveFixture {
            available: available.clone(),
            target: target.clone(),
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(proto::archive_service_server::ArchiveServiceServer::new(
                    fixture.clone(),
                ))
                .add_service(proto::snapshot_service_server::SnapshotServiceServer::new(
                    fixture,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        store.put("snapshot", &target, b"snapshot").await?;
        archive_pending(&store).await?;
        assert_eq!(store.read("snapshot").await?, Some(b"snapshot".to_vec()));
        available.store(true, Ordering::Relaxed);
        archive_pending(&store).await?;
        assert!(store.read("snapshot").await?.is_none());
        store.put("conflict", &target, b"different").await?;
        archive_pending(&store).await?;
        assert_eq!(store.read("conflict").await?, Some(b"different".to_vec()));
        server.abort();
        Ok(())
    }
}
