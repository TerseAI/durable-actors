use anyhow::{Context, Result, ensure};
use std::time::Duration;
use tonic::{
    Request, Status,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

pub(crate) const MAX_STORAGE_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Channels(moka::future::Cache<String, Channel>);

impl Default for Channels {
    fn default() -> Self {
        Self(
            moka::future::Cache::builder()
                .max_capacity(256)
                .time_to_idle(Duration::from_secs(300))
                .build(),
        )
    }
}

impl Channels {
    pub(crate) async fn preconnect(&self, origin: &str) -> Result<()> {
        let origin = reqwest::Url::parse(origin)?.to_string();
        let endpoint = endpoint(&origin)?;
        self.0
            .try_get_with(origin, async {
                endpoint.connect().await.map_err(anyhow::Error::from)
            })
            .await
            .map_err(|error| anyhow::anyhow!("connect storage endpoint: {error}"))?;
        Ok(())
    }

    pub(crate) async fn capability(&self, url: &str) -> Result<(Channel, String)> {
        let (origin, token) = capability_origin(url)?;
        let channel = self
            .0
            .try_get_with(origin.clone(), async { channel(&origin) })
            .await
            .map_err(|error| anyhow::anyhow!("connect storage endpoint: {error}"))?;
        Ok((channel, token))
    }
}

pub(crate) fn channel(origin: &str) -> Result<Channel> {
    Ok(endpoint(origin)?.connect_lazy())
}

fn endpoint(origin: &str) -> Result<Endpoint> {
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
    Ok(endpoint)
}

fn capability_origin(url: &str) -> Result<(String, String)> {
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
    let origin = reqwest::Url::parse(&format!(
        "{scheme}:{}",
        url.as_str()
            .split_once(':')
            .context("missing capability scheme")?
            .1
    ))?
    .to_string();
    ensure!(
        url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
        "invalid storage capability authority"
    );
    Ok((origin, token))
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
