use serde_json::json;

use super::*;
use crate::actor::validate_socket_effects;

#[tokio::test]
async fn inventory_notifies_on_activation_metadata_and_disconnect() -> anyhow::Result<()> {
    let registry = SocketRegistry::default();
    let mut changes = registry.inventory_changes();
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    let (sender, _receiver) = socket_channel();
    registry
        .insert(
            &actor,
            ActorSocketConnection {
                id: "socket".into(),
                metadata: json!({"name":"Ada"}),
                tags: vec![],
            },
            sender,
            None,
        )
        .await;
    assert!(registry.inventory().await.is_empty());
    assert!(!changes.has_changed()?);
    registry.activate(&actor, "socket").await;
    tokio::time::timeout(std::time::Duration::from_millis(100), changes.changed()).await??;
    assert_eq!(
        registry.inventory().await[0].connections[0].metadata,
        json!({"name":"Ada"})
    );
    registry
        .apply(
            &actor,
            vec![ActorSocketEffect::SetMetadata {
                connection_id: "socket".into(),
                metadata: json!({"name":"Grace"}),
            }],
        )
        .await;
    tokio::time::timeout(std::time::Duration::from_millis(100), changes.changed()).await??;
    assert_eq!(
        registry.inventory().await[0].connections[0].metadata,
        json!({"name":"Grace"})
    );
    registry.remove(&actor, "socket").await;
    tokio::time::timeout(std::time::Duration::from_millis(100), changes.changed()).await??;
    assert!(registry.inventory().await.is_empty());
    Ok(())
}

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
        project_id: "default".into(),
        actor_name: "Files".into(),
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
fn accepts_structured_public_state_effects_and_rejects_invalid_snapshots() -> anyhow::Result<()> {
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
        project_id: "default".into(),
        actor_name: "ChatRoom".into(),
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
