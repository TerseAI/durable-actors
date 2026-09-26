use std::time::{Duration, Instant};
use tonic::{
    Request, Status,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use super::proto::actor_service_client::ActorServiceClient;

pub(crate) fn client(origin: &str) -> anyhow::Result<ActorServiceClient<Channel>> {
    crate::regional::proxy::validate_origin(origin)?;
    let mut endpoint = Endpoint::from_shared(origin.to_owned())?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if origin.starts_with("https:") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    }
    Ok(ActorServiceClient::new(endpoint.connect_lazy())
        .max_decoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES)
        .max_encoding_message_size(crate::actor::MAX_ACTOR_EXECUTOR_MESSAGE_BYTES))
}

pub(crate) fn deadline<T>(request: &Request<T>) -> Result<Instant, Status> {
    let Some(timeout) = request.metadata().get("grpc-timeout") else {
        return Ok(Instant::now() + Duration::from_secs(120));
    };
    let timeout = timeout
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid gRPC timeout"))?;
    if !(2..=9).contains(&timeout.len()) {
        return Err(Status::invalid_argument("invalid gRPC timeout"));
    }
    let (value, unit) = timeout.split_at(timeout.len() - 1);
    let value: u64 = value
        .parse()
        .map_err(|_| Status::invalid_argument("invalid gRPC timeout"))?;
    let scale = match unit {
        "H" => Duration::from_secs(3600),
        "M" => Duration::from_secs(60),
        "S" => Duration::from_secs(1),
        "m" => Duration::from_millis(1),
        "u" => Duration::from_micros(1),
        "n" => Duration::from_nanos(1),
        _ => return Err(Status::invalid_argument("invalid gRPC timeout")),
    };
    Ok(Instant::now()
        + scale
            .saturating_mul(value as u32)
            .min(Duration::from_secs(86400)))
}

pub(crate) fn authorize<T>(
    request: &mut Request<T>,
    token: &str,
    deadline: Instant,
) -> Result<(), Status> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("request deadline elapsed"))?;
    request.set_timeout(remaining);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .map_err(|_| Status::unauthenticated("invalid upstream credential"))?,
    );
    Ok(())
}
