use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::warn;

use super::proto::{
    ActivateActorReply, ActivateActorRequest, HostInvokeActorRequest, HostSocketEventRequest,
    InvokeActorReply,
    actor_host_service_server::{ActorHostService, ActorHostServiceServer},
};
use crate::{
    actor::{
        ActorExecutionResult, ActorInvocation, ActorInvocationFailure,
        MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
    },
    control_plane::{ActorJwtVerifier, ActorPrincipal},
    host::{ActorHost, ActorProcessRole, HostId},
};

pub(crate) struct ActorHostGrpcService {
    host_id: HostId,
    session_id: String,
    host: Arc<ActorHost>,
    invocation_auth: ActorJwtVerifier,
}

impl ActorHostGrpcService {
    pub(crate) fn new(host: Arc<ActorHost>, session_id: String, auth: ActorJwtVerifier) -> Self {
        Self {
            host_id: host.id().clone(),
            session_id,
            host,
            invocation_auth: auth,
        }
    }

    pub(crate) fn into_service(self) -> ActorHostServiceServer<Self> {
        ActorHostServiceServer::new(self)
            .max_decoding_message_size(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl ActorHostService for ActorHostGrpcService {
    async fn activate(
        &self,
        request: Request<ActivateActorRequest>,
    ) -> Result<Response<ActivateActorReply>, Status> {
        let principal = self.invocation_auth.authenticate(&request).await?;
        validate_activation(&principal, &self.host_id, &self.session_id)?;
        let actor: crate::actor::ActorKey = request
            .into_inner()
            .actor
            .ok_or_else(|| Status::invalid_argument("actor is required"))?
            .try_into()
            .map_err(|error| Status::invalid_argument(format!("{error:#}")))?;
        if !principal.scope.contains(&actor) {
            return Err(Status::permission_denied("activation crossed namespace"));
        }
        let activation = self
            .host
            .activate_actor(actor)
            .await
            .map_err(|error| Status::unavailable(format!("{error:#}")))?;
        Ok(Response::new(ActivateActorReply {
            owner_epoch: activation.owner_epoch,
        }))
    }

    async fn invoke(
        &self,
        request: Request<HostInvokeActorRequest>,
    ) -> Result<Response<InvokeActorReply>, Status> {
        let invocation = self.authorize_invocation(request).await?;
        self.invoke_authorized(invocation).await
    }

    async fn handle_socket(
        &self,
        request: Request<HostSocketEventRequest>,
    ) -> Result<Response<InvokeActorReply>, Status> {
        let principal = self.invocation_auth.authenticate(&request).await?;
        if principal.session_id != self.session_id {
            return Err(Status::permission_denied(
                "actor credential belongs to another host session",
            ));
        }
        let request = request.into_inner();
        let owner_epoch = request.owner_epoch;

        let invocation: crate::actor::ActorSocketInvocation = request
            .try_into()
            .map_err(|error| Status::invalid_argument(format!("{error:#}")))?;
        validate_host_request(&principal, &self.host_id, &invocation.actor, owner_epoch)?;
        let result = self
            .host
            .handle_socket_event(invocation, owner_epoch)
            .await
            .map_err(|error| {
                Status::unavailable(format!("actor socket event failed: {error:#}"))
            })?;
        Ok(Response::new(InvokeActorReply::from(result)))
    }
}

impl ActorHostGrpcService {
    async fn authorize_invocation(
        &self,
        request: Request<HostInvokeActorRequest>,
    ) -> Result<AuthorizedHostInvocation, Status> {
        let principal = self.invocation_auth.authenticate(&request).await?;
        if principal.session_id != self.session_id {
            return Err(Status::permission_denied(
                "actor credential belongs to another host session",
            ));
        }
        let request = request.into_inner();
        let invocation: ActorInvocation = request
            .invocation
            .ok_or_else(|| Status::invalid_argument("actor invocation is required"))?
            .try_into()
            .map_err(|error| Status::invalid_argument(format!("{error:#}")))?;
        validate_host_request(
            &principal,
            &self.host_id,
            &invocation.actor,
            request.owner_epoch,
        )?;
        Ok(AuthorizedHostInvocation {
            invocation,
            owner_epoch: request.owner_epoch,
        })
    }

    async fn invoke_authorized(
        &self,
        request: AuthorizedHostInvocation,
    ) -> Result<Response<InvokeActorReply>, Status> {
        let request_id = request.invocation.request_id.clone();
        let result = match self
            .host
            .invoke_actor(request.invocation, request.owner_epoch)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(request_id, error = %format!("{error:#}"), "actor invocation failed before execution");
                ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure {
                        code: "unavailable".into(),
                        message: "actor could not start because its state was unavailable".into(),
                    },
                }
            }
        };
        Ok(Response::new(InvokeActorReply::from(result)))
    }
}

fn validate_activation(
    principal: &ActorPrincipal,
    host: &HostId,
    session: &str,
) -> Result<(), Status> {
    if principal.process_role != ActorProcessRole::Host
        || principal.host_id != *host
        || principal.session_id != session
        || principal.invocation.is_some()
    {
        return Err(Status::permission_denied(
            "host activation authority is required",
        ));
    }
    Ok(())
}

fn validate_host_request(
    principal: &ActorPrincipal,
    host_id: &HostId,
    actor: &crate::actor::ActorKey,
    owner_epoch: u64,
) -> Result<(), Status> {
    if !principal.scope.contains(actor) {
        return Err(Status::permission_denied(
            "actor invocation crossed namespace scope",
        ));
    }
    if owner_epoch == 0 {
        return Err(Status::invalid_argument(
            "actor ownership capability is incomplete",
        ));
    }
    if principal.process_role != ActorProcessRole::Host || principal.host_id != *host_id {
        return Err(Status::permission_denied(
            "actor invocation credential is not for this host",
        ));
    }
    if let Some(capability) = &principal.invocation
        && (capability.actor != *actor
            || capability.host_id != *host_id
            || capability.owner_epoch != owner_epoch)
    {
        return Err(Status::permission_denied(
            "actor invocation does not match its direct capability",
        ));
    }
    Ok(())
}

struct AuthorizedHostInvocation {
    invocation: ActorInvocation,
    owner_epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actor::{ActorKey, ActorScope},
        control_plane::{ActorInvocationCapability, ActorPrincipal},
    };

    #[test]
    fn direct_capability_is_bound_to_the_actor_host_session_and_epoch() {
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        let host_id = HostId::new("host.v2.project-1:revision-1.host-1");
        let principal = ActorPrincipal {
            scope: ActorScope {
                namespace_id: "project-1".into(),
            },
            host_id: host_id.clone(),
            session_id: "00000000-0000-4000-8000-000000000001".into(),
            process_role: ActorProcessRole::Host,
            region: "north-america-east".into(),
            code_revision: Some("revision-1".into()),
            expires_at: i64::MAX,
            invocation: Some(ActorInvocationCapability {
                actor: actor.clone(),
                host_id: host_id.clone(),
                owner_epoch: 3,
            }),
        };

        assert!(validate_activation(&principal, &host_id, &principal.session_id).is_err());
        let mut activation = principal.clone();
        activation.invocation = None;
        assert!(validate_activation(&activation, &host_id, &activation.session_id).is_ok());
        assert!(validate_activation(&activation, &host_id, "another-session").is_err());
        assert!(
            validate_activation(
                &activation,
                &HostId::new("another-host"),
                &activation.session_id
            )
            .is_err()
        );

        assert!(validate_host_request(&principal, &host_id, &actor, 3).is_ok());
        assert!(validate_host_request(&principal, &host_id, &actor, 4).is_err());
        assert!(
            validate_host_request(
                &principal,
                &host_id,
                &ActorKey {
                    actor_id: "other".into(),
                    ..actor
                },
                3
            )
            .is_err()
        );
    }
}
