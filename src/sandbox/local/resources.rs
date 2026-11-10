//! Samples the local host processes the control plane spawned, including their SDK children.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use sysinfo::{Pid, Process, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::{
    actor::ActorKey,
    actor_resources::{ResourceSample, ResourceSampler},
    clock::{Clock, SystemClock},
    sandbox::ResourceLimits,
};

const MEBIBYTE: u64 = 1024 * 1024;

#[derive(Clone, Default)]
pub(super) struct LocalProcesses(Arc<Mutex<HashMap<u32, LocalProcess>>>);

#[derive(Clone)]
struct LocalProcess {
    actor: ActorKey,
    memory_limit_bytes: Option<u64>,
}

impl LocalProcesses {
    pub(super) fn register(
        &self,
        pid: u32,
        actor: ActorKey,
        resources: &ResourceLimits,
    ) -> ProcessRegistration {
        let process = LocalProcess {
            actor,
            memory_limit_bytes: resources
                .memory_limit_mib
                .map(|mib| u64::from(mib) * MEBIBYTE),
        };
        if let Ok(mut processes) = self.0.lock() {
            processes.insert(pid, process);
        }
        ProcessRegistration {
            processes: self.clone(),
            pid,
        }
    }

    fn snapshot(&self) -> Vec<(u32, LocalProcess)> {
        self.0
            .lock()
            .map(|processes| {
                processes
                    .iter()
                    .map(|(pid, process)| (*pid, process.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Removes the process from sampling when the host stops.
pub(super) struct ProcessRegistration {
    processes: LocalProcesses,
    pid: u32,
}

impl Drop for ProcessRegistration {
    fn drop(&mut self) {
        if let Ok(mut processes) = self.processes.0.lock() {
            processes.remove(&self.pid);
        }
    }
}

pub(super) struct LocalResources {
    processes: LocalProcesses,
    // CPU usage is measured between refreshes, so the same `System` must persist across samples.
    system: Arc<Mutex<System>>,
}

impl LocalResources {
    pub(super) fn new(processes: LocalProcesses) -> Self {
        Self {
            processes,
            system: Arc::new(Mutex::new(System::new())),
        }
    }
}

#[async_trait]
impl ResourceSampler for LocalResources {
    async fn sample(&self) -> Result<Vec<ResourceSample>> {
        let hosts = self.processes.snapshot();
        if hosts.is_empty() {
            return Ok(Vec::new());
        }
        let sampled_at_ms = SystemClock.now_ms()?;
        let system = self.system.clone();
        tokio::task::spawn_blocking(move || {
            let mut system = system
                .lock()
                .map_err(|_| anyhow!("process stats poisoned"))?;
            system.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing().with_memory().with_cpu(),
            );
            let children = children(system.processes());
            Ok(hosts
                .into_iter()
                .map(|(pid, host)| {
                    let (cpu_percent, memory_bytes) =
                        tree_usage(system.processes(), &children, Pid::from_u32(pid));
                    ResourceSample {
                        actor: host.actor,
                        sampled_at_ms,
                        cpu_millicores: (cpu_percent * 10.0).round() as u64,
                        memory_bytes,
                        memory_limit_bytes: host.memory_limit_bytes,
                    }
                })
                .collect())
        })
        .await?
    }
}

fn children(processes: &HashMap<Pid, Process>) -> HashMap<Pid, Vec<Pid>> {
    let mut children = HashMap::<Pid, Vec<Pid>>::new();
    for (pid, process) in processes {
        if let Some(parent) = process.parent() {
            children.entry(parent).or_default().push(*pid);
        }
    }
    children
}

/// CPU percent of one core and resident memory, summed over a process and its descendants.
fn tree_usage(
    processes: &HashMap<Pid, Process>,
    children: &HashMap<Pid, Vec<Pid>>,
    root: Pid,
) -> (f32, u64) {
    let (mut cpu, mut memory) = (0.0, 0);
    let mut pending = vec![root];
    while let Some(pid) = pending.pop() {
        if let Some(process) = processes.get(&pid) {
            cpu += process.cpu_usage();
            memory += process.memory();
        }
        pending.extend(children.get(&pid).into_iter().flatten());
    }
    (cpu, memory)
}
