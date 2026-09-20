use super::*;
use crate::grpc::proto::{
    ControlPlaneReply, ControlPlaneRequest,
    actor_control_plane_service_server::{
        ActorControlPlaneService, ActorControlPlaneServiceServer,
    },
};

struct LocalCredentials;
#[tonic::async_trait]
impl ActorControlPlaneService for LocalCredentials {
    async fn execute(
        &self,
        _: Request<ControlPlaneRequest>,
    ) -> Result<tonic::Response<ControlPlaneReply>, tonic::Status> {
        Ok(tonic::Response::new(
            super::super::protocol::encode_reply(ControlPlaneCommandReply::StorageAccess {
                token: None,
                replacement_token: "renewed-local-token".into(),
            })
            .unwrap(),
        ))
    }
}

#[tokio::test]
async fn local_hosts_refresh_socket_credentials_without_a_cloud_token() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = ControlPlaneClient::connect(
        format!("http://{}", listener.local_addr()?),
        "initial-token",
    )
    .await?;
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ActorControlPlaneServiceServer::new(LocalCredentials))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let result = client.refresh_storage_access().await;
    server.abort();
    result?;
    assert_eq!(
        client.authorization.read().unwrap().to_str()?,
        "Bearer renewed-local-token"
    );
    Ok(())
}
