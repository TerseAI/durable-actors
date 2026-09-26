use std::{
    collections::{HashMap, VecDeque},
    sync::{LazyLock, Mutex},
};

/// Per-request host milestones, returned as `Server-Timing` to callers that ask for them.
pub(super) static REPORTS: LazyLock<TimingReports> = LazyLock::new(|| TimingReports::new(1024));

pub(super) struct TimingReports {
    capacity: usize,
    state: Mutex<Reports>,
}

#[derive(Default)]
struct Reports {
    entries: HashMap<String, Vec<String>>,
    order: VecDeque<String>,
}

impl TimingReports {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::default(),
        }
    }

    pub(super) fn record<'a>(
        &self,
        request_id: &str,
        milestones: impl IntoIterator<Item = (&'a str, Option<f64>)>,
    ) {
        let metrics = milestones
            .into_iter()
            .filter_map(|(name, value)| value.map(|value| format!("{name};dur={value:.2}")));
        self.append(request_id, metrics);
    }

    pub(super) fn describe(&self, request_id: &str, name: &str, description: &str) {
        self.append(request_id, [format!("{name};desc={description}")]);
    }

    pub(super) fn take(&self, request_id: &str) -> Option<String> {
        let mut state = self.state.lock().unwrap();
        let metrics = state.entries.remove(request_id)?;
        state.order.retain(|id| id != request_id);
        Some(metrics.join(", "))
    }

    fn append(&self, request_id: &str, metrics: impl IntoIterator<Item = String>) {
        let mut state = self.state.lock().unwrap();
        if !state.entries.contains_key(request_id) {
            if state.order.len() == self.capacity
                && let Some(oldest) = state.order.pop_front()
            {
                state.entries.remove(&oldest);
            }
            state.order.push_back(request_id.to_owned());
        }
        state
            .entries
            .entry(request_id.to_owned())
            .or_default()
            .extend(metrics);
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/timing_report.rs"]
mod tests;
