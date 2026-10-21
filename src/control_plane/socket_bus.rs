//! Delivers socket effects to every gateway holding sockets for an actor, and
//! asks those gateways about their connections.
//!
//! A browser's socket stays on whichever gateway it reached. A publish goes out
//! once, and each gateway applies it to its own sockets for that actor. Across
//! replicas that is Redis pub/sub; within one process (local development,
//! tests) gateways share a [`MemorySocketHub`].

use super::socket_inventory::SocketInventory;
use crate::{
    actor::{ActorKey, ActorSocketConnection, ActorSocketEffect},
    host_leases::ActorSocketInventory,
    sockets::SocketRegistry,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(super) trait SocketBus: Send + Sync {
    /// This gateway's sockets.
    fn registry(&self) -> &SocketRegistry;
    /// Changes whenever this gateway may have missed a delivery; `None` while
    /// it cannot receive any. Sockets opened under an older generation close.
    fn generation(&self) -> Option<u64>;
    /// Delivers `actor`'s publishes to this gateway. Call before a socket for
    /// `actor` opens here.
    async fn watch(&self, actor: &ActorKey) -> Result<()>;
    /// Undoes one [`SocketBus::watch`], after that socket closed.
    async fn unwatch(&self, actor: &ActorKey);
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()>;
    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>>;
    async fn count(&self, actor: &ActorKey) -> Result<usize>;
    async fn inventory(&self, project: &str) -> Result<SocketInventory>;
}

/// Gateways in one process. Every gateway that joins sees every publish.
#[derive(Clone, Default)]
pub(super) struct MemorySocketHub {
    gateways: Arc<Mutex<Vec<SocketRegistry>>>,
}

impl MemorySocketHub {
    /// A gateway's connection to the hub, with its own sockets.
    pub(super) fn join(&self, max_connections: usize) -> Arc<dyn SocketBus> {
        let registry = SocketRegistry::with_max_connections(max_connections);
        self.gateways.lock().unwrap().push(registry.clone());
        Arc::new(MemorySocketBus {
            hub: self.clone(),
            registry,
        })
    }

    fn gateways(&self) -> Vec<SocketRegistry> {
        self.gateways.lock().unwrap().clone()
    }
}

struct MemorySocketBus {
    hub: MemorySocketHub,
    registry: SocketRegistry,
}

#[async_trait]
impl SocketBus for MemorySocketBus {
    fn registry(&self) -> &SocketRegistry {
        &self.registry
    }

    fn generation(&self) -> Option<u64> {
        Some(0)
    }

    async fn watch(&self, _: &ActorKey) -> Result<()> {
        Ok(())
    }

    async fn unwatch(&self, _: &ActorKey) {}

    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        for gateway in self.hub.gateways() {
            gateway.apply(actor, effects.clone()).await;
        }
        Ok(())
    }

    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>> {
        let mut connections = Vec::new();
        for gateway in self.hub.gateways() {
            connections.extend(gateway.connections_with_tag(actor, tag).await);
        }
        Ok(connections)
    }

    async fn count(&self, actor: &ActorKey) -> Result<usize> {
        let mut count = 0;
        for gateway in self.hub.gateways() {
            count += gateway.count(actor).await;
        }
        Ok(count)
    }

    async fn inventory(&self, project: &str) -> Result<SocketInventory> {
        let mut rooms = Vec::new();
        for gateway in self.hub.gateways() {
            rooms.extend(gateway.inventory(project).await);
        }
        Ok(SocketInventory {
            rooms: merge_inventory(rooms),
            complete: true,
        })
    }
}

/// One entry per actor, though its sockets may sit on several gateways.
fn merge_inventory(rooms: Vec<ActorSocketInventory>) -> Vec<ActorSocketInventory> {
    let mut merged: BTreeMap<(String, String), ActorSocketInventory> = BTreeMap::new();
    for room in rooms {
        let key = (room.actor.actor_name.clone(), room.actor.actor_id.clone());
        match merged.get_mut(&key) {
            Some(entry) => entry.connections.extend(room.connections),
            None => {
                merged.insert(key, room);
            }
        }
    }
    let mut rooms: Vec<_> = merged.into_values().collect();
    for room in &mut rooms {
        room.connections.sort_by(|a, b| a.id.cmp(&b.id));
    }
    rooms
}

/// How long a question waits for every gateway that received it to answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a socket waits for its gateway to subscribe to its actor.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
/// An actor's auto-response outlives its sockets this long, so a gateway that
/// opens the actor's next socket still answers it.
const AUTO_RESPONSE_TTL: Duration = Duration::from_secs(7 * 24 * 3600);

/// Gateways across replicas, through Redis pub/sub.
///
/// Every process subscribes to its own reply channel, gateways to a channel
/// all gateways share, and each gateway to the channel of every actor it holds
/// sockets for. A question goes to an actor's or the gateways' channel;
/// `PUBLISH` says how many gateways received it, so the asker waits for exactly
/// that many answers.
pub(super) struct RedisSocketBus {
    shared: Arc<Shared>,
    commands: mpsc::UnboundedSender<Command>,
    /// Sockets open here per actor; the actor's channel is subscribed while positive.
    watched: Mutex<HashMap<ActorKey, usize>>,
}

struct Shared {
    redis: crate::cache::Redis,
    registry: SocketRegistry,
    namespace: String,
    /// This process's reply channel.
    replies: String,
    connected: AtomicBool,
    generation: AtomicU64,
    /// Questions waiting for answers, by id.
    pending: Mutex<HashMap<String, mpsc::UnboundedSender<Answer>>>,
    /// Actors with sockets here, to resubscribe after reconnecting.
    subscribed: Mutex<Vec<String>>,
}

enum Command {
    Subscribe(String, oneshot::Sender<Result<()>>),
    Unsubscribe(String),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Envelope {
    Effects {
        actor: ActorKey,
        effects: Vec<ActorSocketEffect>,
    },
    Question {
        id: String,
        reply_to: String,
        question: Question,
    },
    Answer {
        id: String,
        answer: Answer,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Question {
    Count {
        actor: ActorKey,
    },
    Connections {
        actor: ActorKey,
        tag: Option<String>,
    },
    Inventory {
        project: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Answer {
    Count {
        count: usize,
    },
    Connections {
        connections: Vec<ActorSocketConnection>,
    },
    Inventory {
        rooms: Vec<ActorSocketInventory>,
    },
}

#[derive(Serialize, Deserialize)]
struct AutoResponse {
    request: Option<String>,
    response: Option<String>,
}

impl RedisSocketBus {
    /// `accepts_sockets` gateways answer inventory questions; others only
    /// publish and ask.
    pub(super) fn start(
        redis: crate::cache::Redis,
        namespace: &str,
        max_connections: usize,
        accepts_sockets: bool,
        stop: CancellationToken,
    ) -> Arc<dyn SocketBus> {
        let shared = Arc::new(Shared {
            redis,
            registry: SocketRegistry::with_max_connections(max_connections),
            namespace: namespace.to_owned(),
            replies: format!("{namespace}:replies:{}", uuid::Uuid::new_v4()),
            connected: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            pending: Mutex::default(),
            subscribed: Mutex::default(),
        });
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(shared.clone().listen(receiver, accepts_sockets, stop));
        Arc::new(Self {
            shared,
            commands,
            watched: Mutex::default(),
        })
    }

    /// Asks every gateway subscribed to `channel`. Errors unless every one answered.
    async fn ask(&self, channel: &str, question: Question) -> Result<Vec<Answer>> {
        let (answers, complete) = self.ask_partial(channel, question).await?;
        ensure!(complete, "a socket gateway did not answer");
        Ok(answers)
    }

    async fn ask_partial(&self, channel: &str, question: Question) -> Result<(Vec<Answer>, bool)> {
        ensure!(
            self.shared.connected.load(Ordering::SeqCst),
            "socket bus is reconnecting"
        );
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, mut answers) = mpsc::unbounded_channel();
        self.shared
            .pending
            .lock()
            .unwrap()
            .insert(id.clone(), sender);
        let asked = async {
            let receivers = self
                .shared
                .publish(
                    channel,
                    &Envelope::Question {
                        id: id.clone(),
                        reply_to: self.shared.replies.clone(),
                        question,
                    },
                )
                .await?;
            let mut received = Vec::new();
            let deadline = tokio::time::Instant::now() + ANSWER_TIMEOUT;
            while received.len() < receivers {
                match tokio::time::timeout_at(deadline, answers.recv()).await {
                    Ok(Some(answer)) => received.push(answer),
                    _ => break,
                }
            }
            let complete = received.len() == receivers;
            Ok((received, complete))
        }
        .await;
        self.shared.pending.lock().unwrap().remove(&id);
        asked
    }
}

impl Shared {
    fn actor_channel(&self, actor: &ActorKey) -> String {
        format!("{}:actor:{}", self.namespace, actor.storage_key().as_str())
    }

    fn gateways_channel(&self) -> String {
        format!("{}:gateways", self.namespace)
    }

    fn auto_response_key(&self, actor: &ActorKey) -> String {
        format!(
            "{}:auto-response:{}",
            self.namespace,
            actor.storage_key().as_str()
        )
    }

    /// Publishes `envelope` on `channel`; returns how many subscribers received it.
    async fn publish(&self, channel: &str, envelope: &Envelope) -> Result<usize> {
        let mut command = redis::cmd("PUBLISH");
        command.arg(channel).arg(serde_json::to_string(envelope)?);
        self.redis.run::<usize>(command).await
    }

    /// Keeps a pub/sub connection open, reconnecting after it drops. Each new
    /// connection starts a new generation, so sockets that may have missed
    /// publishes meanwhile close and their browsers reconnect.
    async fn listen(
        self: Arc<Self>,
        mut commands: mpsc::UnboundedReceiver<Command>,
        accepts_sockets: bool,
        stop: CancellationToken,
    ) {
        loop {
            let result = self
                .clone()
                .session(&mut commands, accepts_sockets, &stop)
                .await;
            self.connected.store(false, Ordering::SeqCst);
            match result {
                Ok(()) => return,
                Err(error) => tracing::warn!(error = %format!("{error:#}"),
                    "socket bus lost its Redis subscription; its sockets close and it reconnects"),
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    async fn session(
        self: Arc<Self>,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        accepts_sockets: bool,
        stop: &CancellationToken,
    ) -> Result<()> {
        let (mut sink, mut messages) = self.redis.pubsub().await?.split();
        sink.subscribe(&self.replies).await?;
        if accepts_sockets {
            sink.subscribe(self.gateways_channel()).await?;
        }
        let resubscribe = self.subscribed.lock().unwrap().clone();
        for channel in resubscribe {
            sink.subscribe(channel).await?;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.connected.store(true, Ordering::SeqCst);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                message = messages.next() => {
                    let message = message.context("Redis closed the pub/sub connection")?;
                    self.clone().receive(message.get_payload_bytes()).await;
                }
                command = commands.recv() => match command {
                    None => return Ok(()),
                    Some(Command::Subscribe(channel, done)) => {
                        let subscribed = sink.subscribe(&channel).await;
                        let failed = subscribed.is_err();
                        let _ = done.send(subscribed.map_err(Into::into));
                        if failed {
                            anyhow::bail!("subscribe to {channel} failed");
                        }
                    }
                    Some(Command::Unsubscribe(channel)) => sink.unsubscribe(&channel).await?,
                },
            }
        }
    }

    async fn receive(self: Arc<Self>, payload: &[u8]) {
        let envelope = match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                tracing::warn!(%error, "ignored an unreadable socket bus message");
                return;
            }
        };
        match envelope {
            // In order, on the listener: an actor's effects apply as published.
            Envelope::Effects { actor, effects } => self.registry.apply(&actor, effects).await,
            Envelope::Question {
                id,
                reply_to,
                question,
            } => {
                tokio::spawn(async move {
                    let answer = match question {
                        Question::Count { actor } => Answer::Count {
                            count: self.registry.count(&actor).await,
                        },
                        Question::Connections { actor, tag } => Answer::Connections {
                            connections: self
                                .registry
                                .connections_with_tag(&actor, tag.as_deref())
                                .await,
                        },
                        Question::Inventory { project } => Answer::Inventory {
                            rooms: self.registry.inventory(&project).await,
                        },
                    };
                    if let Err(error) = self
                        .publish(&reply_to, &Envelope::Answer { id, answer })
                        .await
                    {
                        tracing::warn!(error = %format!("{error:#}"), "socket bus answer failed");
                    }
                });
            }
            Envelope::Answer { id, answer } => {
                if let Some(waiting) = self.pending.lock().unwrap().get(&id) {
                    let _ = waiting.send(answer);
                }
            }
        }
    }
}

#[async_trait]
impl SocketBus for RedisSocketBus {
    fn registry(&self) -> &SocketRegistry {
        &self.shared.registry
    }

    fn generation(&self) -> Option<u64> {
        self.shared
            .connected
            .load(Ordering::SeqCst)
            .then(|| self.shared.generation.load(Ordering::SeqCst))
    }

    async fn watch(&self, actor: &ActorKey) -> Result<()> {
        ensure!(
            self.shared.connected.load(Ordering::SeqCst),
            "socket bus is reconnecting"
        );
        let channel = self.shared.actor_channel(actor);
        let (done, subscribed) = oneshot::channel();
        {
            // Commands are sent under the lock, so they reach Redis in the
            // order the counts changed.
            let mut watched = self.watched.lock().unwrap();
            let sockets = watched.entry(actor.clone()).or_default();
            *sockets += 1;
            if *sockets == 1 {
                self.shared.subscribed.lock().unwrap().push(channel.clone());
            }
            // Every watch waits for a subscribe of its own: one that arrives
            // while the first is in flight returns only once it is in place.
            self.commands
                .send(Command::Subscribe(channel, done))
                .ok()
                .context("socket bus stopped")?;
        }
        let subscribed = tokio::time::timeout(SUBSCRIBE_TIMEOUT, subscribed).await;
        if !matches!(subscribed, Ok(Ok(Ok(())))) {
            self.unwatch(actor).await;
            anyhow::bail!("socket bus could not subscribe to the actor");
        }
        // Subscribed before reading, so a change published meanwhile arrives too.
        let mut get = redis::cmd("GET");
        get.arg(self.shared.auto_response_key(actor));
        if let Some(stored) = self.shared.redis.run::<Option<String>>(get).await? {
            let AutoResponse { request, response } = serde_json::from_str(&stored)?;
            self.shared
                .registry
                .apply(
                    actor,
                    vec![ActorSocketEffect::SetAutoResponse { request, response }],
                )
                .await;
        }
        Ok(())
    }

    async fn unwatch(&self, actor: &ActorKey) {
        let mut watched = self.watched.lock().unwrap();
        let Some(sockets) = watched.get_mut(actor) else {
            return;
        };
        *sockets -= 1;
        if *sockets == 0 {
            watched.remove(actor);
            let channel = self.shared.actor_channel(actor);
            self.shared
                .subscribed
                .lock()
                .unwrap()
                .retain(|subscribed| subscribed != &channel);
            let _ = self.commands.send(Command::Unsubscribe(channel));
        }
    }

    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        if let Some((request, response)) = effects.iter().rev().find_map(|effect| match effect {
            ActorSocketEffect::SetAutoResponse { request, response } => {
                Some((request.clone(), response.clone()))
            }
            _ => None,
        }) {
            let key = self.shared.auto_response_key(actor);
            let command = if request.is_some() && response.is_some() {
                let mut set = redis::cmd("SET");
                set.arg(key)
                    .arg(serde_json::to_string(&AutoResponse { request, response })?)
                    .arg("PX")
                    .arg(AUTO_RESPONSE_TTL.as_millis() as u64);
                set
            } else {
                let mut delete = redis::cmd("DEL");
                delete.arg(key);
                delete
            };
            self.shared.redis.run::<redis::Value>(command).await?;
        }
        self.shared
            .publish(
                &self.shared.actor_channel(actor),
                &Envelope::Effects {
                    actor: actor.clone(),
                    effects,
                },
            )
            .await?;
        Ok(())
    }

    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>> {
        let answers = self
            .ask(
                &self.shared.actor_channel(actor),
                Question::Connections {
                    actor: actor.clone(),
                    tag: tag.map(str::to_owned),
                },
            )
            .await?;
        Ok(answers
            .into_iter()
            .flat_map(|answer| match answer {
                Answer::Connections { connections } => connections,
                _ => vec![],
            })
            .collect())
    }

    async fn count(&self, actor: &ActorKey) -> Result<usize> {
        let answers = self
            .ask(
                &self.shared.actor_channel(actor),
                Question::Count {
                    actor: actor.clone(),
                },
            )
            .await?;
        Ok(answers
            .into_iter()
            .map(|answer| match answer {
                Answer::Count { count } => count,
                _ => 0,
            })
            .sum())
    }

    async fn inventory(&self, project: &str) -> Result<SocketInventory> {
        let (answers, complete) = self
            .ask_partial(
                &self.shared.gateways_channel(),
                Question::Inventory {
                    project: project.to_owned(),
                },
            )
            .await?;
        let rooms = answers
            .into_iter()
            .flat_map(|answer| match answer {
                Answer::Inventory { rooms } => rooms,
                _ => vec![],
            })
            .collect();
        Ok(SocketInventory {
            rooms: merge_inventory(rooms),
            complete,
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/socket_bus.rs"]
mod tests;
