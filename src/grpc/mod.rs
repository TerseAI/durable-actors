pub(crate) mod transport;

pub(crate) mod proto {
    tonic::include_proto!("durable_actors.v1");
}
