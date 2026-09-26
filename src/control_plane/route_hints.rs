use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};

use tokio::sync::watch;

/// Routes of spares claimed for actors whose resolution is still activating the host.
pub(crate) static ROUTE_HINTS: LazyLock<RouteHints> = LazyLock::new(RouteHints::default);

#[derive(Default)]
pub(crate) struct RouteHints(Mutex<HashMap<String, watch::Sender<Option<String>>>>);

impl RouteHints {
    pub(crate) fn subscribe(&self, actor: &str) -> watch::Receiver<Option<String>> {
        self.0
            .lock()
            .unwrap()
            .entry(actor.to_owned())
            .or_insert_with(|| watch::channel(None).0)
            .subscribe()
    }

    pub(crate) fn publish(&self, actor: &str, route: &str) {
        if let Some(sender) = self.0.lock().unwrap().get(actor) {
            sender.send_replace(Some(route.to_owned()));
        }
    }

    pub(crate) fn release(&self, actor: &str, receiver: watch::Receiver<Option<String>>) {
        drop(receiver);
        let mut hints = self.0.lock().unwrap();
        if hints
            .get(actor)
            .is_some_and(|sender| sender.receiver_count() == 0)
        {
            hints.remove(actor);
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.0.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/route_hints.rs"]
mod tests;
