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
            entries: Default::default(),
            activity: watch::channel(0).0,
        }
    }
}

impl SocketRegistry {
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
mod tests {
    use serde_json::json;

    use super::*;
    use crate::actor::validate_socket_effects;

    #[tokio::test]
    async fn slow_consumers_close_without_blocking_actor_output() {
        let (sender, mut receiver) = socket_channel();
        for _ in 0..32 {
            sender
                .send(OutboundMessage::Close {
                    code: 1000,
                    reason: String::new(),
                })
                .unwrap();
        }
        assert!(
            sender
                .send(OutboundMessage::Close {
                    code: 1000,
                    reason: String::new()
                })
                .is_err()
        );
        assert!(matches!(
            receiver.recv().await,
            Some(OutboundMessage::Close { code: 1013, .. })
        ));
    }

    #[tokio::test]
    async fn broadcast_matches_all_or_any_tags_and_preserves_exclusions() {
        let registry = SocketRegistry::default();
        let actor = ActorKey {
            actor_type: "Files".into(),
            actor_id: "files".into(),
        };
        let mut receivers = Vec::new();
        for (id, tags, open) in [
            ("a", vec!["file:a"], true),
            ("b", vec!["file:b"], true),
            ("both", vec!["file:a", "file:b", "file:c"], true),
            ("neither", vec!["file:c"], true),
            ("excluded", vec!["file:a", "file:b"], true),
            ("connecting", vec!["file:a", "file:b"], false),
        ] {
            let (outbound, receiver) = socket_channel();
            assert!(
                registry
                    .insert(
                        &actor,
                        ActorSocketConnection {
                            id: id.into(),
                            metadata: json!({}),
                            tags: Vec::new(),
                        },
                        outbound,
                        None
                    )
                    .await
            );
            if open {
                registry.activate(&actor, id).await;
            }
            registry
                .apply(
                    &actor,
                    vec![ActorSocketEffect::SetTags {
                        connection_id: id.into(),
                        tags: tags.into_iter().map(String::from).collect(),
                    }],
                )
                .await;
            receivers.push((id, receiver));
        }
        for (mode, tags, expected) in [
            (None, vec!["file:a", "file:b"], vec!["both"]),
            (Some("all"), vec!["file:a", "file:b"], vec!["both"]),
            (
                Some("any"),
                vec!["file:a", "file:b"],
                vec!["a", "b", "both"],
            ),
            (Some("any"), vec!["file:missing"], vec![]),
            (Some("all"), vec![], vec!["a", "b", "both", "neither"]),
            (Some("any"), vec![], vec!["a", "b", "both", "neither"]),
        ] {
            let mut effect = json!({
                "type": "broadcast", "message": {"type": "text", "data": "ready"},
                "except_connection_ids": ["excluded"], "tags": tags,
            });
            if let Some(mode) = mode {
                effect["tag_match"] = json!(mode);
            }
            registry
                .apply(&actor, vec![serde_json::from_value(effect).unwrap()])
                .await;
            for (id, receiver) in &mut receivers {
                let delivered = receiver.try_recv().ok();
                assert_eq!(
                    delivered.is_some(),
                    expected.contains(id),
                    "mode={mode:?}, socket={id}"
                );
                if let Some(message) = delivered {
                    assert!(
                        matches!(message, OutboundMessage::Message(ActorSocketMessage::Text { data }) if data == "ready")
                    );
                }
                assert!(receiver.try_recv().is_err(), "duplicate delivery to {id}");
            }
        }
    }

    #[test]
    fn broadcast_rejects_invalid_tag_matching_mode() {
        assert!(
            serde_json::from_value::<ActorSocketEffect>(json!({
                "type": "broadcast", "message": {"type": "text", "data": "ready"},
                "except_connection_ids": [], "tags": [], "tag_match": "either",
            }))
            .is_err()
        );
    }

    #[test]
    fn accepts_structured_public_state_effects_and_rejects_invalid_snapshots() -> anyhow::Result<()>
    {
        let effects: Vec<ActorSocketEffect> = serde_json::from_value(json!([
            {"type":"state_snapshot","connection_id":"socket","state":{"count":1}},
            {"type":"state_update","changes":{"count":2},"removed":["optional"]}
        ]))?;
        validate_socket_effects(&effects)?;
        let invalid: Vec<ActorSocketEffect> = serde_json::from_value(json!([
            {"type":"state_snapshot","connection_id":"socket","state":null}
        ]))?;
        assert!(validate_socket_effects(&invalid).is_err());
        Ok(())
    }

    #[test]
    fn rejects_socket_effects_that_bypass_sdk_invariants() {
        let invalid = [
            ActorSocketEffect::Close {
                connection_id: "socket-1".into(),
                code: 1001,
                reason: String::new(),
            },
            ActorSocketEffect::SetTags {
                connection_id: "socket-1".into(),
                tags: vec!["x".repeat(257)],
            },
            ActorSocketEffect::SetMetadata {
                connection_id: "socket-1".into(),
                metadata: json!({ "data": "x".repeat(64 * 1024) }),
            },
            ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: ActorSocketMessage::Binary {
                    data: "not base64".into(),
                },
            },
        ];

        for effect in invalid {
            assert!(crate::actor::validate_socket_effects(&[effect]).is_err());
        }
    }

    #[tokio::test]
    async fn registry_retains_metadata_tags_and_outbound_messages() {
        let registry = SocketRegistry::default();
        let actor = ActorKey {
            actor_type: "ChatRoom".into(),
            actor_id: "room-1".into(),
        };
        let (outbound, mut messages) = socket_channel();
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    },
                    outbound,
                    None,
                )
                .await
        );
        registry.activate(&actor, "socket-1").await;

        registry
            .apply(
                &actor,
                vec![
                    ActorSocketEffect::SetMetadata {
                        connection_id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1", "ready": true }),
                    },
                    ActorSocketEffect::SetTags {
                        connection_id: "socket-1".into(),
                        tags: vec!["member".into()],
                    },
                    ActorSocketEffect::Send {
                        connection_id: "socket-1".into(),
                        message: ActorSocketMessage::Text {
                            data: "hello".into(),
                        },
                    },
                    ActorSocketEffect::Broadcast {
                        message: ActorSocketMessage::Text {
                            data: "everyone".into(),
                        },
                        except_connection_ids: Vec::new(),
                        tags: vec!["member".into()],
                        tag_match: ActorSocketTagMatch::All,
                    },
                ],
            )
            .await;

        assert_eq!(
            registry.connections(&actor).await,
            vec![ActorSocketConnection {
                id: "socket-1".into(),
                metadata: json!({ "userId": "user-1", "ready": true }),
                tags: vec!["member".into()],
            }]
        );
        assert!(matches!(
            messages.recv().await,
            Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "hello"
        ));
        assert!(matches!(
            messages.recv().await,
            Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "everyone"
        ));
    }
}
