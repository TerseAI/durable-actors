use super::*;
use crate::grpc::proto;
use futures_util::StreamExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct EchoToken;

#[tonic::async_trait]
impl proto::snapshot_service_server::SnapshotService for EchoToken {
    async fn read(
        &self,
        request: tonic::Request<proto::Empty>,
    ) -> Result<tonic::Response<proto::SnapshotData>, tonic::Status> {
        Ok(tonic::Response::new(proto::SnapshotData {
            data: crate::grpc::transport::token(&request)?.as_bytes().to_vec(),
        }))
    }
    async fn write(
        &self,
        _: tonic::Request<proto::SnapshotData>,
    ) -> Result<tonic::Response<proto::SnapshotWriteReply>, tonic::Status> {
        Ok(tonic::Response::new(proto::SnapshotWriteReply {
            already_exists: false,
        }))
    }
}

#[tokio::test]
async fn reuses_connection_without_reusing_request_credentials() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).inspect(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto::snapshot_service_server::SnapshotServiceServer::new(
                EchoToken,
            ))
            .serve_with_incoming(incoming),
    );
    let transport = GrpcStateTransport::new();
    for token in ["first", "second"] {
        assert_eq!(
            transport
                .read(&format!("grpc://{address}?token={token}"))
                .await?
                .as_ref(),
            token.as_bytes()
        );
    }
    server.abort();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn rejects_http_and_file_storage_capabilities() {
    for url in [
        "file:///tmp/state",
        "https://host/_replica/state?token=secret",
        "grpc://host?token=first&token=second",
    ] {
        assert!(GrpcStateTransport::new().read(url).await.is_err());
    }
}
