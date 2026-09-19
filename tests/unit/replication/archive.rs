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
