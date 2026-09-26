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

#[derive(Debug)]
struct ServiceToken;
impl google_cloud_auth::credentials::idtoken::IDTokenCredentialsProvider for ServiceToken {
    async fn id_token(
        &self,
    ) -> std::result::Result<String, google_cloud_auth::errors::CredentialsError> {
        Ok("google-id-token".into())
    }
}
struct IdentityProtectedControlPlane;
#[tonic::async_trait]
impl ActorControlPlaneService for IdentityProtectedControlPlane {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> Result<tonic::Response<ControlPlaneReply>, tonic::Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer host-token"
        );
        assert_eq!(
            request
                .metadata()
                .get("x-serverless-authorization")
                .unwrap(),
            "Bearer google-id-token"
        );
        Ok(tonic::Response::new(
            super::super::protocol::encode_reply(ControlPlaneCommandReply::Unit).unwrap(),
        ))
    }
}
#[tokio::test]
async fn host_callbacks_send_service_identity_and_scoped_host_token() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client =
        ControlPlaneClient::connect(format!("http://{}", listener.local_addr()?), "host-token")
            .await?
            .with_service_identity(Some(ServiceToken.into()));
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ActorControlPlaneServiceServer::new(
                IdentityProtectedControlPlane,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let result = client.notify_inventory_changed().await;
    task.abort();
    result
}

#[tokio::test]
async fn prewarmed_client_obtains_identity_before_assignment_then_uses_the_host_token() -> Result<()>
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ActorControlPlaneServiceServer::new(
                IdentityProtectedControlPlane,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let warm = ControlPlaneClient::prewarm(endpoint, ServiceToken.into()).await?;
    assert!(
        warm.identity_token_ms().is_some(),
        "identity must be obtained before assignment"
    );
    let result = warm
        .with_host_token("host-token")?
        .notify_inventory_changed()
        .await;
    task.abort();
    result
}
