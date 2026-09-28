use super::{ReplicaAccess, ReplicaGrant, ReplicaStore};
use crate::{
    grpc::{
        proto,
        transport::{token, unavailable},
    },
    state_log::StateSnapshot,
};
use axum::{Router, http::StatusCode, routing::get};
use std::sync::{Arc, OnceLock};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct ReplicaServer {
    store: Arc<dyn ReplicaStore>,
    binding: Arc<OnceLock<ReplicaBinding>>,
}

pub(super) struct ReplicaBinding {
    pub access: ReplicaAccess,
    pub host_id: String,
    pub scope: super::ReplicaScope,
}

pub fn replica_routes(
    store: Arc<dyn ReplicaStore>,
    access: ReplicaAccess,
    host_id: String,
    scope: super::ReplicaScope,
) -> Router {
    let binding = Arc::new(OnceLock::new());
    let _ = binding.set(ReplicaBinding {
        access,
        host_id,
        scope,
    });
    routes(store, binding)
}

pub(super) fn routes(
    store: Arc<dyn ReplicaStore>,
    binding: Arc<OnceLock<ReplicaBinding>>,
) -> Router {
    let server = ReplicaServer { store, binding };
    tonic::service::Routes::from(Router::new().route("/health", get(|| async { StatusCode::OK })))
        .add_service(
            proto::snapshot_service_server::SnapshotServiceServer::new(server.clone())
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .add_service(
            proto::replica_service_server::ReplicaServiceServer::new(server)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
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
        Ok(Response::new(proto::SnapshotData {
            data,
            dependencies: Vec::new(),
        }))
    }

    async fn write(
        &self,
        request: Request<proto::SnapshotData>,
    ) -> Result<Response<proto::SnapshotWriteReply>, Status> {
        let grant = self.authorize(&request, &["APPEND"])?;
        let bundle = request.into_inner();
        let bytes = bundle.data;
        StateSnapshot::decode(&bytes).map_err(|_| Status::invalid_argument("invalid snapshot"))?;
        let stream = grant
            .stream
            .as_ref()
            .ok_or_else(|| Status::permission_denied("stream authority is required"))?;
        stream.snapshot(&bytes).map_err(unavailable)?;
        self.install_dependencies(&bytes, bundle.dependencies)
            .await
            .map_err(unavailable)?;
        self.store
            .append(stream, &bytes)
            .await
            .map_err(unavailable)?;
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

    async fn install_dependencies(
        &self,
        bytes: &[u8],
        dependencies: Vec<proto::SnapshotDependency>,
    ) -> anyhow::Result<()> {
        use anyhow::{Context, ensure};
        let actor = &self
            .binding
            .get()
            .context("replica is not assigned")?
            .scope
            .actor;
        let prefix = crate::storage_paths::snapshots(actor)?;
        let mut supplied = std::collections::HashMap::new();
        for dependency in dependencies {
            ensure!(
                supplied
                    .insert(dependency.object, dependency.data)
                    .is_none(),
                "duplicate SQLite dependency"
            );
        }
        let mut snapshot = StateSnapshot::decode(bytes)?;
        let mut installing = Vec::new();
        while let Some(parent) = snapshot
            .sqlite
            .as_ref()
            .and_then(|sqlite| sqlite.parent.clone())
        {
            ensure!(
                parent.object.starts_with(&prefix),
                "SQLite dependency belongs to another actor"
            );
            let Some(data) = supplied.remove(&parent.object) else {
                break;
            };
            parent.verify(&data)?;
            snapshot = StateSnapshot::decode(&data)?;
            snapshot.validate_object(&parent.object)?;
            ensure!(
                snapshot.state_version == parent.state_version,
                "SQLite dependency version mismatch"
            );
            installing.push((parent.object, data));
        }
        ensure!(supplied.is_empty(), "unreferenced SQLite dependencies");
        for (object, data) in installing.into_iter().rev() {
            self.store.put(&object, &data).await?;
        }
        Ok(())
    }

    fn authorize<T>(
        &self,
        request: &Request<T>,
        operations: &[&str],
    ) -> Result<ReplicaGrant, Status> {
        let binding = self
            .binding
            .get()
            .ok_or_else(|| Status::permission_denied("replica is not assigned"))?;
        let grant = binding
            .access
            .verify_any(token(request)?, operations)
            .map_err(|_| Status::permission_denied("replica capability rejected"))?;
        if grant.host_id != binding.host_id {
            return Err(Status::permission_denied(
                "capability belongs to another replica",
            ));
        }
        let scope = &binding.scope;
        let allowed = if matches!(
            grant.operation.as_str(),
            "INITIALIZE_SESSION" | "SEAL_SESSION"
        ) {
            grant.object == scope.identity()
        } else {
            let prefix = crate::storage_paths::snapshots(&scope.actor).map_err(unavailable)?;
            grant.object.starts_with(&prefix)
                && grant.stream.as_ref().is_none_or(|stream| {
                    stream.session == scope.identity() && stream.prefix.starts_with(&prefix)
                })
        };
        if !allowed {
            return Err(Status::permission_denied(
                "replica belongs to another actor activation",
            ));
        }
        Ok(grant)
    }
}
