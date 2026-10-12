use super::*;

pub(super) fn environment(
    request: &EnsureHostRequest,
    route: &str,
    secrets: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let artifact = request
        .code_snapshot
        .as_deref()
        .context("compiled GCS code artifact required")?;
    let manifest = crate::artifacts::ArtifactManifest::decode(artifact)?;
    let actor = request.actor.as_ref().context("actor identity required")?;
    actor.validate()?;
    let mut environment = HashMap::from([
        ("DURABLE_ACTORS_PROCESS_ROLE".into(), "host".into()),
        (
            "DURABLE_ACTORS_HOST_TOKEN".into(),
            request.host_token.clone(),
        ),
        (
            "DURABLE_ACTORS_JWT_PUBLIC_KEYS".into(),
            request.jwt_public_keys.clone(),
        ),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL".into(),
            request.control_plane_url.clone(),
        ),
        (
            "DURABLE_ACTORS_JWT_ISSUER".into(),
            request.jwt_issuer.clone(),
        ),
        (
            "DURABLE_ACTORS_INVOKE_JWT_AUDIENCE".into(),
            request.invocation_jwt_audience.clone(),
        ),
        ("DURABLE_ACTORS_HOST_ID".into(), request.host_id.to_string()),
        (
            "DURABLE_ACTORS_SESSION_ID".into(),
            request.session_id.clone(),
        ),
        (
            "DURABLE_ACTORS_REGION".into(),
            request.canonical_region.clone(),
        ),
        ("DURABLE_ACTORS_HOST_ROUTE".into(), route.into()),
        ("DURABLE_ACTORS_HOST_BIND".into(), "0.0.0.0:80".into()),
        (
            "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS".into(),
            request.host_idle_timeout_ms.to_string(),
        ),
        (
            "DURABLE_ACTORS_EXECUTOR_SOCKET".into(),
            "/tmp/durable-actors-executor.sock".into(),
        ),
        (
            "DURABLE_ACTORS_ENTRYPOINT".into(),
            format!("/customer/{}", manifest.entrypoint()?),
        ),
        ("DURABLE_ACTORS_CODE_ARTIFACT".into(), artifact.into()),
        (
            "DURABLE_ACTORS_CUSTOMER_ENV".into(),
            serde_json::to_string(&secrets)?,
        ),
        ("DURABLE_ACTORS_ACTOR".into(), serde_json::to_string(actor)?),
        (
            "DURABLE_ACTORS_ACTOR_IS_NEW".into(),
            request.actor_is_new.to_string(),
        ),
        (
            "DURABLE_ACTORS_RUNTIME_CONFIG".into(),
            request
                .runtime_config
                .clone()
                .context("host storage configuration required")?,
        ),
    ]);
    if let Some(hint) = &request.owner_hint {
        environment.insert("DURABLE_ACTORS_OWNER_HINT".into(), hint.clone());
    }
    Ok(environment)
}

pub(crate) fn validate_image(image: &str) -> Result<()> {
    let (name, digest) = image
        .rsplit_once("@sha256:")
        .context("container image must be pinned to a sha256 digest")?;
    ensure!(
        !name.is_empty()
            && !name.chars().any(char::is_whitespace)
            && digest.len() == 64
            && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid container image digest"
    );
    Ok(())
}
