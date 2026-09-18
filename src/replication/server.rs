use super::{ReplicaAccess, ReplicaGrant, ReplicaStore};
use crate::{
    grpc::{
        proto,
        transport::{MAX_STORAGE_MESSAGE_BYTES, token, unavailable},
    },
    state_log::StateSnapshot,
};
use axum::{Router, http::StatusCode, routing::get};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct ReplicaServer {
    store: Arc<dyn ReplicaStore>,
    access: ReplicaAccess,
    host_id: String,
}

pub fn replica_routes(
    store: Arc<dyn ReplicaStore>,
    access: ReplicaAccess,
    host_id: String,
) -> Router {
    let server = ReplicaServer {
        store,
        access,
        host_id,
    };
    tonic::service::Routes::from(Router::new().route("/health", get(|| async { StatusCode::OK })))
        .add_service(
            proto::snapshot_service_server::SnapshotServiceServer::new(server.clone())
                .max_decoding_message_size(MAX_STORAGE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_STORAGE_MESSAGE_BYTES),
        )
        .add_service(
            proto::replica_service_server::ReplicaServiceServer::new(server)
                .max_decoding_message_size(MAX_STORAGE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_STORAGE_MESSAGE_BYTES),
        )
        .into_axum_router()
}

#[tonic::async_trait]
impl proto::snapshot_service_server::SnapshotService for ReplicaServer {
    async fn read(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::SnapshotData>, Status> {
        let grant = self.authorize(&request, &["GET"])?;
        let data = self
            .store
            .read(&grant.object)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| Status::not_found("snapshot not found"))?;
        Ok(Response::new(proto::SnapshotData { data }))
    }

    async fn write(
        &self,
        request: Request<proto::SnapshotData>,
    ) -> Result<Response<proto::SnapshotWriteReply>, Status> {
        let grant = self.authorize(&request, &["PUT", "APPEND"])?;
        if grant.archive_url.is_empty() {
            return Err(Status::permission_denied("archive capability is required"));
        }
        let bytes = request.into_inner().data;
        StateSnapshot::decode(&bytes).map_err(|_| Status::invalid_argument("invalid snapshot"))?;
        if grant.operation == "APPEND" {
            let stream = grant
                .stream
                .as_ref()
                .ok_or_else(|| Status::permission_denied("stream authority is required"))?;
            self.store
                .append(stream, &grant.archive_url, &bytes)
                .await
                .map_err(unavailable)?;
        } else {
            if grant.stream.is_some() {
                return Err(Status::permission_denied("object authority is required"));
            }
            self.store
                .put(&grant.object, &grant.archive_url, &bytes)
                .await
                .map_err(unavailable)?;
        }
        Ok(Response::new(proto::SnapshotWriteReply {
            already_exists: false,
        }))
    }
}

#[tonic::async_trait]
impl proto::replica_service_server::ReplicaService for ReplicaServer {
    async fn initialize(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let grant = self.session(&request, "INITIALIZE_SESSION")?;
        self.store
            .initialize_session(&grant.object)
            .await
            .map_err(unavailable)?;
        Ok(Response::new(proto::Empty {}))
    }

    async fn head(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ReplicaStreamHead>, Status> {
        let grant = self.authorize(&request, &["HEAD"])?;
        let stream = grant
            .stream
            .as_ref()
            .ok_or_else(|| Status::permission_denied("stream authority is required"))?;
        let head = self.store.stream_head(stream).await.map_err(unavailable)?;
        Ok(Response::new(head.into()))
    }

    async fn seal(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ReplicaSessionHead>, Status> {
        let grant = self.session(&request, "SEAL_SESSION")?;
        let head = self
            .store
            .seal_session(&grant.object)
            .await
            .map_err(unavailable)?;
        Ok(Response::new(head.into()))
    }
}

impl ReplicaServer {
    fn session<T>(&self, request: &Request<T>, operation: &str) -> Result<ReplicaGrant, Status> {
        let grant = self.authorize(request, &[operation])?;
        if grant.stream.is_some() {
            return Err(Status::permission_denied("session authority is required"));
        }
        Ok(grant)
    }

    fn authorize<T>(
        &self,
        request: &Request<T>,
        operations: &[&str],
    ) -> Result<ReplicaGrant, Status> {
        let grant = self
            .access
            .verify_any(token(request)?, operations)
            .map_err(|_| Status::permission_denied("replica capability rejected"))?;
        if grant.host_id != self.host_id {
            return Err(Status::permission_denied(
                "capability belongs to another replica",
            ));
        }
        Ok(grant)
    }
}
