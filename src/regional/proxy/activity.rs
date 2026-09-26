use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{sync::Notify, time::Instant};

pub(super) struct Activity {
    state: Mutex<State>,
    changed: Notify,
}

struct State {
    active: usize,
    last_finished: Instant,
    stopping: bool,
}

impl Default for Activity {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                active: 0,
                last_finished: Instant::now(),
                stopping: false,
            }),
            changed: Notify::new(),
        }
    }
}

impl Activity {
    pub fn enter(self: &Arc<Self>) -> Option<ActiveRequest> {
        let mut state = self.state.lock().unwrap();
        if state.stopping {
            return None;
        }
        state.active += 1;
        self.changed.notify_one();
        Some(ActiveRequest(self.clone()))
    }

    pub async fn wait_until_idle(&self, timeout: Duration) {
        loop {
            let deadline = {
                let mut state = self.state.lock().unwrap();
                if state.active == 0 && state.last_finished.elapsed() >= timeout {
                    state.stopping = true;
                    return;
                }
                if state.active == 0 {
                    state.last_finished + timeout
                } else {
                    Instant::now() + timeout
                }
            };
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {},
                _ = self.changed.notified() => {},
            }
        }
    }
}

pub(super) struct ActiveRequest(Arc<Activity>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.active -= 1;
        state.last_finished = Instant::now();
        self.0.changed.notify_one();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/regional/proxy_activity.rs"]
mod tests;
