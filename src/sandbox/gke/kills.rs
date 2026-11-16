//! Turns pods that stopped under an actor into kills. A stopped pod is all that is left of its
//! host, so the control plane names each pod's actor in annotations as it assigns the pod.

use std::collections::BTreeMap;

use k8s_openapi::{
    api::core::v1::{ContainerStateTerminated, Pod, PodStatus},
    apimachinery::pkg::apis::meta::v1::Time,
};
use kube::ResourceExt;

use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    platform_events::sandboxes::{KillReason, SandboxKill},
};

const PROJECT_ID: &str = "terse.ai/project-id";
const ACTOR_NAME: &str = "terse.ai/actor-name";
const ACTOR_ID: &str = "terse.ai/actor-id";

pub(super) fn actor_annotations(actor: &ActorKey) -> BTreeMap<&'static str, &str> {
    BTreeMap::from([
        (PROJECT_ID, actor.project_id.as_str()),
        (ACTOR_NAME, actor.actor_name.as_str()),
        (ACTOR_ID, actor.actor_id.as_str()),
    ])
}

/// The kill a pod's status shows, if it ran an actor.
pub(super) fn kill(pod: &Pod) -> Option<SandboxKill> {
    let actor = assigned_actor(pod)?;
    let status = pod.status.as_ref()?;
    let terminated = terminated(status);
    let reason = reason(pod, status, terminated)?;
    let storage = status
        .message
        .as_deref()
        .is_some_and(|message| message.contains("ephemeral local storage"));
    Some(SandboxKill {
        actor,
        sandbox_id: sandbox_id(pod)?,
        reason,
        message: (reason == KillReason::Evicted && storage)
            .then(|| "exceeded its temporary storage limit".into()),
        at_ms: stopped_at(status, terminated)?,
    })
}

pub(super) fn sandbox_id(pod: &Pod) -> Option<String> {
    Some(format!(
        "{}/{}/{}",
        pod.namespace()?,
        pod.name_any(),
        pod.uid()?
    ))
}

fn assigned_actor(pod: &Pod) -> Option<ActorKey> {
    let annotations = pod.annotations();
    Some(ActorKey {
        project_id: annotations.get(PROJECT_ID)?.clone(),
        actor_name: annotations.get(ACTOR_NAME)?.clone(),
        actor_id: annotations.get(ACTOR_ID)?.clone(),
    })
}

/// Pods we delete end with SIGKILL too, so a plain 137 only counts when nothing asked the pod to
/// stop. An out-of-memory kill counts even then: retiring a pod whose assignment just failed can
/// beat the kubelet's report of why it failed.
fn reason(
    pod: &Pod,
    status: &PodStatus,
    terminated: Option<&ContainerStateTerminated>,
) -> Option<KillReason> {
    if status.reason.as_deref() == Some("Evicted") {
        Some(KillReason::Evicted)
    } else if terminated.is_some_and(|state| state.reason.as_deref() == Some("OOMKilled")) {
        Some(KillReason::OutOfMemory)
    } else if terminated.is_some_and(|state| state.exit_code == 137)
        && pod.metadata.deletion_timestamp.is_none()
    {
        Some(KillReason::Killed)
    } else {
        None
    }
}

fn terminated(status: &PodStatus) -> Option<&ContainerStateTerminated> {
    status
        .container_statuses
        .iter()
        .flatten()
        .find_map(|container| container.state.as_ref()?.terminated.as_ref())
}

/// Evicted pods have no finished container. Their last condition change marks the stop and stays
/// the same when the watch replays the pod, so a replay doesn't move the kill.
fn stopped_at(status: &PodStatus, terminated: Option<&ContainerStateTerminated>) -> Option<u64> {
    terminated
        .and_then(|state| state.finished_at.as_ref())
        .or_else(|| last_transition(status))
        .and_then(|time| u64::try_from(time.0.timestamp_millis()).ok())
        .or_else(|| SystemClock.now_ms().ok())
}

fn last_transition(status: &PodStatus) -> Option<&Time> {
    status
        .conditions
        .iter()
        .flatten()
        .filter_map(|condition| condition.last_transition_time.as_ref())
        .max_by_key(|time| time.0)
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/kills.rs"]
mod tests;
