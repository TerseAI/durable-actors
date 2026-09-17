use crate::{actor::ActorKey, actor_state::ActorStorageKey, host::HostId};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

pub const ROOT: &str = "little-actors/v1/namespaces/";

pub fn namespace(namespace: &str) -> String {
    format!("{ROOT}{}/", component(namespace))
}

pub fn snapshots(actor: &ActorKey) -> Result<String> {
    actor.validate()?;
    Ok(format!(
        "{}snapshots/{}/",
        namespace(&actor.namespace_id),
        actor_path(actor)
    ))
}

pub fn owner(object: &ActorStorageKey) -> Result<String> {
    let actor = actor_from_key(object)?;
    Ok(format!(
        "{}owners/{}.json",
        namespace(&actor.namespace_id),
        actor_path(&actor)
    ))
}

pub fn host(host: &HostId) -> String {
    let ns = host
        .as_str()
        .strip_prefix("host.v2.")
        .and_then(|value| value.split_once(':'))
        .map_or("", |(ns, _)| ns);
    format!("{}hosts/{}/", namespace(ns), component(host.as_str()))
}

pub fn session(namespace_id: &str, host: &HostId, session: &str) -> String {
    format!(
        "{}hosts/{}/sessions/{}",
        namespace(namespace_id),
        component(host.as_str()),
        component(session)
    )
}

pub fn actor_from_snapshot(object: &str) -> Result<ActorKey> {
    let parts: Vec<_> = object
        .strip_prefix(ROOT)
        .context("invalid snapshot root")?
        .split('/')
        .collect();
    ensure!(
        parts.len() == 7 && parts[1] == "snapshots",
        "invalid snapshot path"
    );
    let actor = ActorKey {
        namespace_id: decode(parts[0])?,
        actor_type: decode(parts[3])?,
        actor_id: decode(parts[4])?,
    };
    actor.validate()?;
    ensure!(
        object.starts_with(&snapshots(&actor)?),
        "snapshot shard mismatch"
    );
    Ok(actor)
}

pub fn actor_from_key(key: &ActorStorageKey) -> Result<ActorKey> {
    let parts: Vec<_> = key
        .as_str()
        .strip_prefix("object.v2.")
        .context("invalid actor key")?
        .split(':')
        .collect();
    ensure!(parts.len() == 3, "invalid actor identity");
    let actor = ActorKey {
        namespace_id: parts[0].into(),
        actor_type: parts[1].into(),
        actor_id: parts[2].into(),
    };
    actor.validate()?;
    Ok(actor)
}

fn actor_path(actor: &ActorKey) -> String {
    let hash = aws_lc_rs::digest::digest(
        &aws_lc_rs::digest::SHA256,
        actor.storage_key().as_str().as_bytes(),
    );
    format!(
        "{:02x}/{}/{}",
        hash.as_ref()[0],
        component(&actor.actor_type),
        component(&actor.actor_id)
    )
}

fn component(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value)
}
fn decode(value: &str) -> Result<String> {
    Ok(String::from_utf8(URL_SAFE_NO_PAD.decode(value)?)?)
}
