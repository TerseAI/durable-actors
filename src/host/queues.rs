use crate::{
    actor::ActorKey,
    host_leases::{ActorQueueInventory, WaitingOperation},
};
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct ActorQueues(watch::Sender<Vec<ActorQueueInventory>>);

impl ActorQueues {
    pub(crate) fn new() -> Self {
        Self(watch::channel(Vec::new()).0)
    }

    pub(crate) fn inventory(&self) -> Vec<ActorQueueInventory> {
        self.0.borrow().clone()
    }

    pub(crate) fn changes(&self) -> watch::Receiver<Vec<ActorQueueInventory>> {
        self.0.subscribe()
    }

    pub(crate) fn enqueue(&self, actor: &ActorKey, operation: String) -> WaitingRequest {
        let id = uuid::Uuid::new_v4().to_string();
        self.0.send_modify(|queues| {
            let index = queues
                .iter()
                .position(|queue| queue.actor == *actor)
                .unwrap_or_else(|| {
                    queues.push(ActorQueueInventory {
                        actor: actor.clone(),
                        waiting: Vec::new(),
                    });
                    queues.len() - 1
                });
            queues[index].waiting.push(WaitingOperation {
                id: id.clone(),
                operation,
            });
        });
        WaitingRequest {
            queues: self.clone(),
            id,
        }
    }
}

pub(crate) struct WaitingRequest {
    queues: ActorQueues,
    id: String,
}

impl Drop for WaitingRequest {
    fn drop(&mut self) {
        self.queues.0.send_modify(|queues| {
            for queue in queues.iter_mut() {
                queue.waiting.retain(|waiting| waiting.id != self.id);
            }
            queues.retain(|queue| !queue.waiting.is_empty());
        });
    }
}
