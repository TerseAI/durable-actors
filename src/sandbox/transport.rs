use anyhow::{Result, ensure};
use terse_substrate::{ObjectRef, TARGET_ACTOR_HEADER, target_actor_header};

pub(crate) fn host_request(
    client: &reqwest::Client,
    route: &str,
    path: &str,
) -> Result<reqwest::RequestBuilder> {
    let (mut url, actor) = host_route(route)?;
    url.set_path(path);
    let request = client.post(url);
    Ok(match actor {
        Some(actor) => request.header(TARGET_ACTOR_HEADER, target_actor_header(&actor)?),
        None => request,
    })
}

pub(crate) fn host_route(route: &str) -> Result<(reqwest::Url, Option<ObjectRef>)> {
    let url = reqwest::Url::parse(route)?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none(),
        "invalid host route"
    );
    let actor = if url.path() == "/" {
        None
    } else {
        let parts: Vec<_> = url.path().split('/').collect();
        ensure!(
            parts.len() == 4 && parts[1] == "substrate",
            "invalid Substrate route"
        );
        let actor = ObjectRef {
            atespace: parts[2].into(),
            name: parts[3].into(),
        };
        target_actor_header(&actor)?;
        Some(actor)
    };
    Ok((url, actor))
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/transport.rs"]
mod tests;
