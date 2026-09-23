mod service;
pub(crate) mod transport;
mod wire;

pub(crate) mod proto {
    tonic::include_proto!("durable_actors.v1");
}

pub(crate) use self::service::ActorHostGrpcService;
