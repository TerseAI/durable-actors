use anyhow::{Context, Result, ensure};
use std::time::Duration;
use tonic::{
    Request, Status,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

pub(crate) const MAX_STORAGE_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

pub(crate) fn channel(origin: &str) -> Result<Channel> {
    let url = reqwest::Url::parse(origin)?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "invalid gRPC origin"
    );
    let mut endpoint = Endpoint::from_shared(origin.to_owned())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(25));
    if url.scheme() == "https" {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    }
    Ok(endpoint.connect_lazy())
}

pub(crate) fn capability(url: &str) -> Result<(Channel, String)> {
    let mut url = reqwest::Url::parse(url)?;
    let scheme = match url.scheme() {
        "grpc" => "http",
        "grpcs" => "https",
        _ => anyhow::bail!("storage capability must use grpc or grpcs"),
    };
    let query = url.query_pairs().collect::<Vec<_>>();
    ensure!(
        query.len() == 1 && query[0].0 == "token" && !query[0].1.is_empty(),
        "storage capability requires one token"
    );
    let token = query[0].1.to_string();
    url.set_query(None);
    let origin = format!(
        "{scheme}:{}",
        url.as_str()
            .split_once(':')
            .context("missing capability scheme")?
            .1
    );
    ensure!(
        url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
        "invalid storage capability authority"
    );
    Ok((channel(&origin)?, token))
}

pub(crate) fn request<T>(value: T, token: &str) -> Result<Request<T>> {
    let mut request = Request::new(value);
    request.set_timeout(Duration::from_secs(25));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .context("invalid gRPC credential")?,
    );
    Ok(request)
}

pub(crate) fn token<T>(request: &Request<T>) -> Result<&str, Status> {
    request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("storage credential is required"))
}

pub(crate) fn unavailable(error: impl std::fmt::Display) -> Status {
    tracing::warn!(error = %error, "storage operation failed");
    Status::unavailable("storage operation failed")
}
