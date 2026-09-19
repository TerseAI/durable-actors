use crate::actor::{
    ActorKey, ActorSocketConnection, ActorSocketEffect, ActorSocketEvent, ActorSocketMessage,
    ActorSocketTagMatch,
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, mpsc, watch};
use tokio_util::sync::CancellationToken;

pub(crate) mod browser;
const MAX_CONNECTIONS_PER_ACTOR: usize = 128;

#[derive(Clone)]
pub(crate) struct SocketRegistry {
    inventory_changes: watch::Sender<()>,
    entries: Arc<RwLock<HashMap<ActorKey, HashMap<String, RegisteredSocket>>>>,
    activity: watch::Sender<usize>,
}

#[derive(Clone)]
struct RegisteredSocket {
    connection: ActorSocketConnection,
    outbound: SocketSender,
    open: bool,
    state_ready: bool,
}

pub(crate) enum OutboundMessage {
    Message(ActorSocketMessage),
    Control(Value),
    Close { code: u16, reason: String },
}

impl Default for SocketRegistry {
    fn default() -> Self {
        Self {
            inventory_changes: watch::channel(()).0,
            entries: Default::default(),
            activity: watch::channel(0).0,
        }
    }
}

impl SocketRegistry {
    pub(crate) fn inventory_changes(&self) -> watch::Receiver<()> {
        self.inventory_changes.subscribe()
    }

    pub(crate) async fn inventory(&self) -> Vec<crate::host_leases::ActorSocketInventory> {
        let entries = self.entries.read().await;
        let mut inventory = Vec::new();
        for (actor, sockets) in entries.iter() {
            let mut connections: Vec<_> = sockets
                .values()
                .filter(|entry| entry.open)
                .map(|entry| entry.connection.clone())
                .collect();
            connections.sort_by(|a, b| a.id.cmp(&b.id));
            if !connections.is_empty() {
                inventory.push(crate::host_leases::ActorSocketInventory {
                    actor: actor.clone(),
                    connections,
                });
            }
        }
        inventory.sort_by(|a, b| {
            (&a.actor.actor_type, &a.actor.actor_id).cmp(&(&b.actor.actor_type, &b.actor.actor_id))
        });
        inventory
    }

    pub(crate) fn activity(&self) -> watch::Receiver<usize> {
        self.activity.subscribe()
    }

    pub(crate) async fn insert(
        &self,
        actor: &ActorKey,
        connection: ActorSocketConnection,
        outbound: SocketSender,
        _trigger_id: Option<String>,
    ) -> bool {
        let mut entries = self.entries.write().await;
        let connections = entries.entry(actor.clone()).or_default();
        if connections.len() >= MAX_CONNECTIONS_PER_ACTOR {
            return false;
        }
        connections.insert(
            connection.id.clone(),
            RegisteredSocket {
                connection,
                outbound,
                open: false,
                state_ready: false,
            },
        );
        self.activity
            .send_replace(entries.values().map(HashMap::len).sum());
        true
    }

    pub(crate) async fn remove(
        &self,
        actor: &ActorKey,
        connection_id: &str,
    ) -> Option<ActorSocketConnection> {
        let mut entries = self.entries.write().await;
        let connections = entries.get_mut(actor)?;
        let removed = connections
            .remove(connection_id)
            .map(|entry| entry.connection);
        if connections.is_empty() {
            entries.remove(actor);
        }
        self.activity
            .send_replace(entries.values().map(HashMap::len).sum());
        if removed.is_some() {
            self.inventory_changes.send_replace(());
        }
        removed
    }

    pub(crate) async fn connections(&self, actor: &ActorKey) -> Vec<ActorSocketConnection> {
        self.entries
            .read()
            .await
            .get(actor)
            .map(|entries| {
                entries
                    .values()
                    .filter(|entry| entry.open)
                    .map(|entry| entry.connection.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) async fn prepare_event(
        &self,
        actor: &ActorKey,
        event: ActorSocketEvent,
    ) -> (ActorSocketEvent, Vec<ActorSocketConnection>) {
        match event {
            ActorSocketEvent::Connect { connection } => {
                let mut connections = self.connections(actor).await;
                connections.push(connection.clone());
                (ActorSocketEvent::Connect { connection }, connections)
            }
            ActorSocketEvent::Message {
                connection_id,
                message,
            } => (
                ActorSocketEvent::Message {
                    connection_id,
                    message,
                },
                self.connections(actor).await,
            ),
            ActorSocketEvent::Disconnect {
                connection,
                code,
                reason,
                was_clean,
            } => {
                let connection = self
                    .remove(actor, &connection.id)
                    .await
                    .unwrap_or(connection);
                (
                    ActorSocketEvent::Disconnect {
                        connection,
                        code,
                        reason,
                        was_clean,
                    },
                    self.connections(actor).await,
                )
            }
        }
    }

    pub(crate) async fn apply(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) {
        for effect in effects {
            self.apply_one(actor, effect).await;
        }
    }

    async fn apply_one(&self, actor: &ActorKey, effect: ActorSocketEffect) {
        match effect {
            ActorSocketEffect::StateSnapshot {
                connection_id,
                state,
                version,
            } => {
                let Some(version) = version else {
                    return;
                };
                let mut entries = self.entries.write().await;
                if let Some(entry) = entries
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.state_ready = true;
                    let _ = entry.outbound.send(OutboundMessage::Control(
                        serde_json::json!({ "type":"state", "state":state, "version":version }),
                    ));
                }
            }
            ActorSocketEffect::StateUpdate {
                changes,
                removed,
                except_connection_ids,
                version,
            } => {
                let Some(version) = version else {
                    return;
                };
                let entries = self.entries.read().await;
                if let Some(connections) = entries.get(actor) {
                    let value = serde_json::json!({ "type":"state_update", "changes":changes, "removed":removed, "version":version });
                    for entry in connections.values().filter(|entry| {
                        entry.state_ready && !except_connection_ids.contains(&entry.connection.id)
                    }) {
                        let _ = entry.outbound.send(OutboundMessage::Control(value.clone()));
                    }
                }
            }
            ActorSocketEffect::Broadcast {
                message,
                except_connection_ids,
                tags,
                tag_match,
            } => {
                let recipients = self
                    .entries
                    .read()
                    .await
                    .get(actor)
                    .map(|connections| {
                        connections
                            .values()
                            .filter(|entry| {
                                entry.open
                                    && !except_connection_ids.contains(&entry.connection.id)
                                    && (tags.is_empty()
                                        || match tag_match {
                                            ActorSocketTagMatch::All => tags
                                                .iter()
                                                .all(|tag| entry.connection.tags.contains(tag)),
                                            ActorSocketTagMatch::Any => tags
                                                .iter()
                                                .any(|tag| entry.connection.tags.contains(tag)),
                                        })
                            })
                            .map(|entry| entry.outbound.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for sender in recipients {
                    let _ = sender.send(OutboundMessage::Message(message.clone()));
                }
            }
            ActorSocketEffect::SetMetadata {
                connection_id,
                metadata,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.metadata = metadata;
                    self.inventory_changes.send_replace(());
                }
            }
            ActorSocketEffect::SetTags {
                connection_id,
                tags,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.tags = tags;
                }
            }
            ActorSocketEffect::Send {
                connection_id,
                message,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Message(message));
                }
            }
            ActorSocketEffect::Close {
                connection_id,
                code,
                reason,
            }
            | ActorSocketEffect::Reject {
                connection_id,
                code,
                reason,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Close { code, reason });
                }
            }
        }
    }

    async fn sender(&self, actor: &ActorKey, connection_id: &str) -> Option<SocketSender> {
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|connections| connections.get(connection_id))
            .map(|entry| entry.outbound.clone())
    }

    pub(crate) async fn activate(&self, actor: &ActorKey, connection_id: &str) {
        if let Some(entry) = self
            .entries
            .write()
            .await
            .get_mut(actor)
            .and_then(|connections| connections.get_mut(connection_id))
        {
            entry.open = true;
            self.inventory_changes.send_replace(());
        }
    }
}

#[derive(Clone)]
pub(crate) struct SocketSender {
    sender: mpsc::Sender<OutboundMessage>,
    overflow: CancellationToken,
}

pub(crate) struct SocketReceiver {
    receiver: mpsc::Receiver<OutboundMessage>,
    overflow: CancellationToken,
}

pub(crate) fn socket_channel() -> (SocketSender, SocketReceiver) {
    let (sender, receiver) = mpsc::channel(32);
    let overflow = CancellationToken::new();
    (
        SocketSender {
            sender,
            overflow: overflow.clone(),
        },
        SocketReceiver { receiver, overflow },
    )
}

impl SocketSender {
    pub(crate) fn send(&self, message: OutboundMessage) -> Result<(), ()> {
        self.sender
            .try_send(message)
            .map_err(|_| self.overflow.cancel())
    }
}

impl SocketReceiver {
    pub(crate) async fn recv(&mut self) -> Option<OutboundMessage> {
        tokio::select! {
            biased;
            _ = self.overflow.cancelled() => Some(OutboundMessage::Close { code: 1013, reason: "socket output queue is full".into() }),
            message = self.receiver.recv() => message,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<OutboundMessage, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sockets/mod.rs"]
mod tests;
