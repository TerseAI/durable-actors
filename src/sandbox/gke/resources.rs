//! Turns pod terminations into sandbox kills. Usage comes from the node collector instead.

use k8s_openapi::{
    api::core::v1::{ContainerStateTerminated, Pod, PodStatus},
    apimachinery::pkg::apis::meta::v1::Time,
};
use kube::ResourceExt;

use crate::{
    actor_resources::{KillReason, SandboxKill},
    clock::{Clock, SystemClock},
};

/// The kill a finished pod represents. Pods we deleted end with SIGKILL too, so a plain 137 only
/// counts when nothing asked the pod to stop.
pub(super) fn kill(pod: &Pod) -> Option<SandboxKill> {
    let status = pod.status.as_ref()?;
    let terminated = terminated(pod);
    let evicted = status.reason.as_deref() == Some("Evicted");
    let reason = if evicted {
        KillReason::Evicted
    } else if terminated.is_some_and(|state| state.reason.as_deref() == Some("OOMKilled")) {
        KillReason::OutOfMemory
    } else if terminated.is_some_and(|state| state.exit_code == 137)
        && pod.metadata.deletion_timestamp.is_none()
    {
        KillReason::Killed
    } else {
        return None;
    };
    let storage = status
        .message
        .as_deref()
        .is_some_and(|message| message.contains("ephemeral local storage"));
    Some(SandboxKill {
        sandbox_id: format!("{}/{}/{}", pod.namespace()?, pod.name_any(), pod.uid()?),
        killed_at_ms: terminated
            .and_then(|state| state.finished_at.as_ref())
            .or_else(|| stopped_at(status))
            .and_then(|time| u64::try_from(time.0.timestamp_millis()).ok())
            .or_else(|| SystemClock.now_ms().ok())?,
        reason,
        message: (evicted && storage).then(|| "exceeded its temporary storage limit".into()),
    })
}

/// Evicted pods have no finished container. Their last condition change marks the stop and stays
/// the same when the watch replays the pod, so a replay doesn't move the kill.
fn stopped_at(status: &PodStatus) -> Option<&Time> {
    status
        .conditions
        .iter()
        .flatten()
        .filter_map(|condition| condition.last_transition_time.as_ref())
        .max_by_key(|time| time.0)
}

fn terminated(pod: &Pod) -> Option<&ContainerStateTerminated> {
    pod.status
        .as_ref()?
        .container_statuses
        .iter()
        .flatten()
        .find_map(|container| container.state.as_ref()?.terminated.as_ref())
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/gke_resources.rs"]
mod tests;
