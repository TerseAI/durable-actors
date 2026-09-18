use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Display, Formatter},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixListener,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::{ActorInvocationFailure, ActorKey, ActorSocketSource};

const ACTOR_EXECUTOR_PROTOCOL_VERSION: u32 = 16;
const MAX_PENDING_EXECUTOR_COMMANDS: usize = 64;
pub(crate) const MAX_ACTOR_EXECUTOR_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct ActorMethodInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub method: String,
    pub args: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct ActorMethodEviction {
    pub actor: ActorKey,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorSocketConnection {
    pub id: String,
    pub metadata: Value,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketMessage {
    Text { data: String },
    Binary { data: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEvent {
    Connect {
        connection: ActorSocketConnection,
    },
    Message {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Disconnect {
        connection: ActorSocketConnection,
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

#[derive(Debug, Serialize)]
pub struct ActorSocketInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub event: ActorSocketEvent,
    pub connections: Vec<ActorSocketConnection>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorSocketTagMatch {
    #[default]
    All,
    Any,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEffect {
    StateSnapshot {
        connection_id: String,
        state: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    StateUpdate {
        changes: Value,
        removed: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        except_connection_ids: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    Send {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Broadcast {
        message: ActorSocketMessage,
        except_connection_ids: Vec<String>,
        tags: Vec<String>,
        #[serde(default)]
        tag_match: ActorSocketTagMatch,
    },
    Close {
        connection_id: String,
        code: u16,
        reason: String,
    },
    Reject {
        connection_id: String,
        code: u16,
        reason: String,
    },
    SetMetadata {
        connection_id: String,
        metadata: Value,
    },
    SetTags {
        connection_id: String,
        tags: Vec<String>,
    },
}

#[derive(Debug, PartialEq)]
pub enum ActorMethodOutcome {
    Completed {
        result: Value,
        state: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[derive(Debug, PartialEq)]
pub enum ActorSocketOutcome {
    Handled {
        state: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[async_trait]
pub trait ActorExecutor: Send + Sync {
    fn supports(&self, actor_type: &str) -> bool;

    fn resident_actors(&self) -> Option<Vec<ActorKey>> {
        None
    }

    fn residency_changes(&self) -> Option<watch::Receiver<()>> {
        None
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<&Value>,
    ) -> Result<ActorMethodOutcome>;

    async fn handle_socket(
        &self,
        _invocation: ActorSocketInvocation,
        _state: Option<&Value>,
    ) -> Result<ActorSocketOutcome> {
        Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
            code: "socket_not_supported".into(),
            message: "actor executor does not support sockets".into(),
        }))
    }

    // Queued executors can retain immutable snapshots; borrowed implementations keep their defaults.
    async fn invoke_shared(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<Arc<Value>>,
    ) -> Result<ActorMethodOutcome> {
        self.invoke(invocation, state.as_deref()).await
    }

    async fn handle_socket_shared(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<Arc<Value>>,
    ) -> Result<ActorSocketOutcome> {
        self.handle_socket(invocation, state.as_deref()).await
    }

    async fn evict(&self, _eviction: ActorMethodEviction) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub(crate) trait ActorSocketPublisher: Send + Sync {
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()>;
}

pub(crate) struct ActorExecutorListener {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl ActorExecutorListener {
    pub(crate) async fn bind(socket_path: impl Into<PathBuf>) -> Result<Self> {
        let socket_path = socket_path.into();
        prepare_socket_path(&socket_path).await?;
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create actor executor directory {}", parent.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind actor executor socket {}", socket_path.display()))?;
        Ok(Self {
            listener,
            socket_path,
        })
    }

    pub(crate) async fn accept(self) -> Result<ActorExecutorConnection> {
        let result = self.accept_connection().await;
        let cleanup = remove_socket(&self.socket_path).await;
        match (result, cleanup) {
            (Ok(connection), Ok(())) => Ok(connection),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn accept_connection(&self) -> Result<ActorExecutorConnection> {
        let (stream, _) =
            self.listener.accept().await.with_context(|| {
                format!("accept actor executor at {}", self.socket_path.display())
            })?;
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let attach = match read_client_message(&mut reader).await? {
            Some(ActorExecutorClientMessage::Attach {
                protocol,
                actor_types,
            }) => {
                ensure!(
                    protocol == ACTOR_EXECUTOR_PROTOCOL_VERSION,
                    "customer actor executor uses unsupported protocol version {protocol}"
                );
                ensure!(
                    !actor_types.is_empty(),
                    "customer actor executor did not advertise any actor types"
                );
                actor_types
            }
            Some(_) => {
                anyhow::bail!("first customer actor executor message must attach the process")
            }
            None => anyhow::bail!("customer actor executor disconnected before attaching"),
        };

        let (executor, task) = JsActorExecutor::start(reader, writer, attach);
        debug!(
            socket = %self.socket_path.display(),
            actor_types = ?executor.actor_types,
            "customer JavaScript process connected to actor executor"
        );
        Ok(ActorExecutorConnection { executor, task })
    }
}

pub(crate) struct ActorExecutorConnection {
    executor: Arc<JsActorExecutor>,
    task: JoinHandle<Result<()>>,
}

impl ActorExecutorConnection {
    pub(crate) fn executor(&self) -> Arc<dyn ActorExecutor> {
        self.executor.clone()
    }

    pub(crate) async fn mark_ready(
        &self,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
    ) -> Result<()> {
        self.executor.mark_ready(publisher, sockets).await?;
        info!(
            actor_types = ?self.executor.actor_types,
            "customer JavaScript process attached to actor executor"
        );
        Ok(())
    }

    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        tokio::select! {
            result = &mut self.task => {
                match result {
                    Ok(result) => result,
                    Err(error) => Err(error.into()),
                }
            }
            _ = shutdown.cancelled() => {
                self.task.abort();
                let _ = (&mut self.task).await;
                Ok(())
            }
        }
    }
}

impl Drop for ActorExecutorConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Residency = Arc<Mutex<Option<(Instant, Vec<ActorKey>)>>>;

struct JsActorExecutor {
    changes: watch::Sender<()>,
    residency: Residency,
    actor_types: HashSet<String>,
    commands: mpsc::Sender<ExecutorRequest>,
}

#[async_trait]
impl ActorExecutor for JsActorExecutor {
    fn residency_changes(&self) -> Option<watch::Receiver<()>> {
        Some(self.changes.subscribe())
    }
    fn resident_actors(&self) -> Option<Vec<ActorKey>> {
        self.residency
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(at, _)| at.elapsed() < Duration::from_secs(5))
            .map(|(_, actors)| actors.clone())
    }

    fn supports(&self, actor_type: &str) -> bool {
        self.actor_types.contains(actor_type)
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<&Value>,
    ) -> Result<ActorMethodOutcome> {
        self.invoke_shared(invocation, state.cloned().map(Arc::new))
            .await
    }

    async fn handle_socket(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<&Value>,
    ) -> Result<ActorSocketOutcome> {
        self.handle_socket_shared(invocation, state.cloned().map(Arc::new))
            .await
    }

    async fn invoke_shared(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<Arc<Value>>,
    ) -> Result<ActorMethodOutcome> {
        match self
            .exchange(ExecutorCommand::Invoke(invocation), state)
            .await?
        {
            ExecutorReply::Invoked {
                result,
                state,
                effects,
            } => Ok(ActorMethodOutcome::Completed {
                result,
                state,
                effects,
            }),
            ExecutorReply::Failed { code, message } => {
                Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Evicted | ExecutorReply::StateRequired => {
                anyhow::bail!("actor executor returned eviction reply to invocation")
            }
            ExecutorReply::WebsocketHandled { .. } => {
                anyhow::bail!("actor executor returned socket reply to invocation")
            }
        }
    }

    async fn handle_socket_shared(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<Arc<Value>>,
    ) -> Result<ActorSocketOutcome> {
        match self
            .exchange(ExecutorCommand::WebsocketEvent(invocation), state)
            .await?
        {
            ExecutorReply::WebsocketHandled { state, effects } => {
                Ok(ActorSocketOutcome::Handled { state, effects })
            }
            ExecutorReply::Failed { code, message } => {
                Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Invoked { .. }
            | ExecutorReply::Evicted
            | ExecutorReply::StateRequired => {
                anyhow::bail!("actor executor returned the wrong reply to socket event")
            }
        }
    }

    async fn evict(&self, eviction: ActorMethodEviction) -> Result<()> {
        match self
            .exchange(ExecutorCommand::Evict(eviction), None)
            .await?
        {
            ExecutorReply::Evicted => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor executor rejected eviction ({code}): {message}")
            }
            ExecutorReply::Invoked { .. } => {
                anyhow::bail!("actor executor returned the wrong reply to eviction")
            }
            ExecutorReply::WebsocketHandled { .. } | ExecutorReply::StateRequired => {
                anyhow::bail!("actor executor returned socket reply to eviction")
            }
        }
    }
}

impl JsActorExecutor {
    fn start(
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
        actor_types: Vec<String>,
    ) -> (Arc<Self>, JoinHandle<Result<()>>) {
        let (commands, incoming) = mpsc::channel(MAX_PENDING_EXECUTOR_COMMANDS);
        let residency = Arc::new(Mutex::new(None));
        let (changes, _) = watch::channel(());
        let executor = Arc::new(Self {
            changes: changes.clone(),
            residency: residency.clone(),
            actor_types: actor_types.into_iter().collect(),
            commands,
        });
        let task = tokio::spawn(run_executor_connection(
            reader, writer, incoming, residency, changes,
        ));
        (executor, task)
    }

    async fn mark_ready(
        &self,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
    ) -> Result<()> {
        let (reply, ready) = oneshot::channel();
        self.commands
            .send(ExecutorRequest::Ready(reply, publisher, sockets))
            .await
            .context("actor executor stopped")?;
        ready
            .await
            .context("actor executor disconnected before readiness")?
    }

    async fn exchange(
        &self,
        command: ExecutorCommand,
        state: Option<Arc<Value>>,
    ) -> Result<ExecutorReply> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ExecutorRequest::Exchange(Box::new(PendingCommand {
                command,
                state,
                reply,
                resident_only: false,
            })))
            .await
            .context("actor executor stopped")?;
        response
            .await
            .context("customer actor executor disconnected before replying")?
    }
}

async fn run_executor_connection(
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    commands: mpsc::Receiver<ExecutorRequest>,
    residency: Residency,
    changes: watch::Sender<()>,
) -> Result<()> {
    let (outbound, writes) = mpsc::channel(MAX_PENDING_EXECUTOR_COMMANDS + 1);
    let (inbound, replies) = mpsc::channel(MAX_PENDING_EXECUTOR_COMMANDS);
    let driver = ExecutorDriver {
        changes,
        residency,
        pending: HashMap::new(),
        residents: HashSet::new(),
        next_message_id: 1,
        outbound,
        publisher: None,
        sockets: None,
        publishing: JoinSet::new(),
        publishing_ids: HashSet::new(),
        loading_connections: JoinSet::new(),
        connection_lookup_ids: HashSet::new(),
    };
    tokio::try_join!(
        driver.run(commands, replies),
        read_executor_messages(reader, inbound),
        write_executor_messages(writer, writes)
    )?;
    Ok(())
}

struct ExecutorDriver {
    changes: watch::Sender<()>,
    residency: Residency,
    pending: HashMap<u64, PendingCommand>,
    residents: HashSet<ActorKey>,
    next_message_id: u64,
    outbound: mpsc::Sender<ExecutorWrite>,
    publisher: Option<Arc<dyn ActorSocketPublisher>>,
    sockets: Option<Arc<dyn ActorSocketSource>>,
    publishing: JoinSet<(u64, Result<()>)>,
    publishing_ids: HashSet<u64>,
    loading_connections: JoinSet<(u64, Result<Vec<ActorSocketConnection>>)>,
    connection_lookup_ids: HashSet<u64>,
}

impl ExecutorDriver {
    async fn run(
        mut self,
        mut commands: mpsc::Receiver<ExecutorRequest>,
        mut replies: mpsc::Receiver<Result<ActorExecutorClientMessage>>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                reply = replies.recv() => {
                    match reply.context("actor executor reader stopped")?? {
                        ActorExecutorClientMessage::Residency { actors } => {
                            ensure!(actors.len() <= 32, "too many resident actors");
                            for actor in &actors { actor.validate()?; }
                            let mut current = self.residency.lock().unwrap();
                            let changed = current.as_ref().is_none_or(|(_, previous)| previous != &actors);
                            *current = Some((Instant::now(), actors));
                            if changed { self.changes.send_replace(()); }
                        }
                        ActorExecutorClientMessage::Reply { message_id, reply } => self.deliver(message_id, reply)?,
                        ActorExecutorClientMessage::SocketEffects { message_id, effects } => self.publish(message_id, effects)?,
                        ActorExecutorClientMessage::GetConnections { message_id } => self.load_connections(message_id)?,
                        ActorExecutorClientMessage::Attach { .. } => anyhow::bail!("customer actor executor attached more than once"),
                    }
                }
                published = self.publishing.join_next(), if !self.publishing.is_empty() => {
                    let (message_id, result) = published.context("socket publisher stopped")??;
                    self.publishing_ids.remove(&message_id);
                    self.outbound.send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::SocketEffectsPublished {
                            message_id,
                            error: result.err().map(|error| format!("{error:#}")),
                        })?,
                        written: None,
                    }).await.context("actor executor writer stopped")?;
                }
                loaded = self.loading_connections.join_next(), if !self.loading_connections.is_empty() => {
                    let (message_id, result) = loaded.context("connection lookup stopped")??;
                    self.connection_lookup_ids.remove(&message_id);
                    let (connections, error) = match result {
                        Ok(connections) => (connections, None),
                        Err(error) => (Vec::new(), Some(format!("{error:#}"))),
                    };
                    self.outbound.send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::SocketConnections {
                            message_id, connections, error,
                        })?,
                        written: None,
                    }).await.context("actor executor writer stopped")?;
                }
                command = commands.recv(), if self.pending.len() < MAX_PENDING_EXECUTOR_COMMANDS => match command {
                    Some(command) => self.handle_command(command)?,
                    None => return Ok(()),
                }
            }
        }
    }

    fn handle_command(&mut self, command: ExecutorRequest) -> Result<()> {
        match command {
            ExecutorRequest::Exchange(mut pending) => {
                let resident = self.residents.remove(pending.command.actor());
                pending.resident_only =
                    resident && !matches!(pending.command, ExecutorCommand::Evict(_));
                self.enqueue(*pending)
            }
            ExecutorRequest::Ready(written, publisher, sockets) => {
                self.publisher = publisher;
                self.sockets = sockets;
                self.outbound
                    .try_send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::Attached {
                            supports_residency: true,
                            protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION,
                        })?,
                        written: Some(written),
                    })
                    .map_err(|_| {
                        anyhow::anyhow!("actor executor writer stopped or filled its queue")
                    })
            }
        }
    }

    fn load_connections(&mut self, message_id: u64) -> Result<()> {
        let pending = self
            .pending
            .get(&message_id)
            .context("connection lookup has no active actor invocation")?;
        ensure!(
            !matches!(pending.command, ExecutorCommand::Evict(_)),
            "cannot load connections during eviction"
        );
        ensure!(
            self.connection_lookup_ids.insert(message_id),
            "actor sent concurrent connection lookups"
        );
        let actor = pending.command.actor().clone();
        let sockets = self.sockets.clone();
        self.loading_connections.spawn(async move {
            let result = async {
                sockets
                    .context("actor connection lookup is unavailable")?
                    .connections(&actor)
                    .await
            }
            .await;
            (message_id, result)
        });
        Ok(())
    }

    fn publish(&mut self, message_id: u64, effects: Vec<ActorSocketEffect>) -> Result<()> {
        let pending = self
            .pending
            .get(&message_id)
            .context("socket output has no active actor invocation")?;
        ensure!(
            self.publishing_ids.insert(message_id),
            "actor sent concurrent socket publications"
        );
        let actor = pending.command.actor().clone();
        let connecting = matches!(&pending.command, ExecutorCommand::WebsocketEvent(invocation) if matches!(invocation.event, ActorSocketEvent::Connect { .. }));
        let publisher = self.publisher.clone();
        self.publishing.spawn(async move {
            let result = async {
                ensure!(
                    !connecting,
                    "socket output cannot precede connection acceptance"
                );
                super::validate_socket_effects(&effects)?;
                publisher
                    .context("actor socket publishing is unavailable")?
                    .publish(&actor, effects)
                    .await
            }
            .await;
            (message_id, result)
        });
        Ok(())
    }

    fn enqueue(&mut self, pending: PendingCommand) -> Result<()> {
        let message_id = self.next_message_id;
        self.next_message_id = message_id
            .checked_add(1)
            .context("actor executor message ID overflow")?;
        let state = if pending.resident_only || matches!(pending.command, ExecutorCommand::Evict(_))
        {
            None
        } else {
            Some(pending.state.as_deref().unwrap_or(&Value::Null))
        };
        let bytes = encode_server_message(&ActorExecutorServerMessage::Command {
            message_id,
            command: ExecutorCommandEnvelope {
                command: &pending.command,
                state,
                resident_only: pending.resident_only,
            },
        });
        match bytes {
            Ok(bytes) => {
                // Each pending command has at most one queued write; readiness has its own extra slot.
                self.outbound
                    .try_send(ExecutorWrite {
                        bytes,
                        written: None,
                    })
                    .map_err(|_| {
                        anyhow::anyhow!("actor executor writer stopped or filled its queue")
                    })?;
                self.pending.insert(message_id, pending);
            }
            Err(error) => {
                let reply = if error.is::<ActorExecutorMessageTooLarge>() {
                    Ok(ExecutorReply::Failed {
                        code: "resource_exhausted".into(),
                        message: error.to_string(),
                    })
                } else {
                    Err(error)
                };
                let _ = pending.reply.send(reply);
            }
        }
        Ok(())
    }

    fn deliver(&mut self, message_id: u64, reply: ExecutorReply) -> Result<()> {
        ensure!(
            !self.connection_lookup_ids.contains(&message_id),
            "actor completed before connection lookup finished"
        );
        ensure!(
            !self.publishing_ids.contains(&message_id),
            "actor completed before socket output was acknowledged"
        );
        let mut pending = self
            .pending
            .remove(&message_id)
            .with_context(|| format!("actor executor replied to unknown message {message_id}"))?;
        if matches!(reply, ExecutorReply::StateRequired) {
            if pending.resident_only {
                pending.resident_only = false;
                return self.enqueue(pending);
            }
            let _ = pending.reply.send(Err(anyhow::anyhow!(
                "actor executor refused explicit hydration"
            )));
            return Ok(());
        }
        if matches!(
            reply,
            ExecutorReply::Invoked { .. } | ExecutorReply::WebsocketHandled { .. }
        ) {
            if self.residents.len() >= 4096 {
                self.residents.clear();
            }
            self.residents.insert(pending.command.actor().clone());
        }
        let _ = pending.reply.send(Ok(reply));
        Ok(())
    }
}

async fn read_executor_messages(
    mut reader: BufReader<OwnedReadHalf>,
    inbound: mpsc::Sender<Result<ActorExecutorClientMessage>>,
) -> Result<()> {
    loop {
        let reply = match read_client_message(&mut reader).await {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(anyhow::anyhow!(
                "customer JavaScript actor executor disconnected"
            )),
            Err(error) => Err(error),
        };
        let stopped = reply.is_err();
        inbound
            .send(reply)
            .await
            .context("actor executor driver stopped")?;
        if stopped {
            return Ok(());
        }
    }
}

async fn write_executor_messages(
    mut writer: OwnedWriteHalf,
    mut writes: mpsc::Receiver<ExecutorWrite>,
) -> Result<()> {
    while let Some(write) = writes.recv().await {
        let result = writer
            .write_all(&write.bytes)
            .await
            .context("write actor executor command");
        if let Some(written) = write.written {
            let _ = written.send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| anyhow::anyhow!("{error:#}")),
            );
        }
        result?;
    }
    Ok(())
}

enum ExecutorRequest {
    Exchange(Box<PendingCommand>),
    Ready(
        oneshot::Sender<Result<()>>,
        Option<Arc<dyn ActorSocketPublisher>>,
        Option<Arc<dyn ActorSocketSource>>,
    ),
}

struct PendingCommand {
    command: ExecutorCommand,
    state: Option<Arc<Value>>,
    resident_only: bool,
    reply: oneshot::Sender<Result<ExecutorReply>>,
}

struct ExecutorWrite {
    bytes: Vec<u8>,
    written: Option<oneshot::Sender<Result<()>>>,
}

impl ExecutorCommand {
    fn actor(&self) -> &ActorKey {
        match self {
            Self::Invoke(invocation) => &invocation.actor,
            Self::WebsocketEvent(invocation) => &invocation.actor,
            Self::Evict(eviction) => &eviction.actor,
        }
    }
}

async fn read_client_message(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Option<ActorExecutorClientMessage>> {
    let mut document = Vec::new();
    let bytes = reader
        .take((MAX_ACTOR_EXECUTOR_MESSAGE_BYTES + 1) as u64)
        .read_until(b'\n', &mut document)
        .await?;
    if bytes == 0 {
        return Ok(None);
    }
    ensure!(
        bytes <= MAX_ACTOR_EXECUTOR_MESSAGE_BYTES,
        "customer actor executor message exceeds {MAX_ACTOR_EXECUTOR_MESSAGE_BYTES} bytes"
    );
    serde_json::from_slice(trim_ascii_end(&document))
        .map(Some)
        .context("decode customer actor executor message")
}

fn trim_ascii_end(mut document: &[u8]) -> &[u8] {
    while document.last().is_some_and(u8::is_ascii_whitespace) {
        document = &document[..document.len() - 1];
    }
    document
}

fn encode_server_message(message: &ActorExecutorServerMessage<'_>) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_ACTOR_EXECUTOR_MESSAGE_BYTES {
        return Err(ActorExecutorMessageTooLarge.into());
    }
    Ok(bytes)
}

#[derive(Debug)]
struct ActorExecutorMessageTooLarge;

impl Display for ActorExecutorMessageTooLarge {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "actor executor command exceeds {MAX_ACTOR_EXECUTOR_MESSAGE_BYTES} bytes"
        )
    }
}

impl Error for ActorExecutorMessageTooLarge {}

async fn prepare_socket_path(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket(),
                "refusing to replace non-socket actor executor path {}",
                path.display()
            );
            tokio::fs::remove_file(path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn remove_socket(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {
            debug!(socket = %path.display(), "actor executor socket removed");
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorServerMessage<'a> {
    SocketConnections {
        message_id: u64,
        connections: Vec<ActorSocketConnection>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    SocketEffectsPublished {
        message_id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Attached {
        supports_residency: bool,
        protocol: u32,
    },
    Command {
        message_id: u64,
        command: ExecutorCommandEnvelope<'a>,
    },
}

#[derive(Debug, Serialize)]
struct ExecutorCommandEnvelope<'a> {
    #[serde(flatten)]
    command: &'a ExecutorCommand,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'a Value>,
    resident_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorClientMessage {
    Residency {
        actors: Vec<ActorKey>,
    },
    GetConnections {
        message_id: u64,
    },
    SocketEffects {
        message_id: u64,
        effects: Vec<ActorSocketEffect>,
    },
    Attach {
        protocol: u32,
        actor_types: Vec<String>,
    },
    Reply {
        message_id: u64,
        reply: ExecutorReply,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorCommand {
    Invoke(ActorMethodInvocation),
    WebsocketEvent(ActorSocketInvocation),
    Evict(ActorMethodEviction),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorReply {
    StateRequired,
    Invoked {
        result: Value,
        state: Value,
        #[serde(default)]
        effects: Vec<ActorSocketEffect>,
    },
    WebsocketHandled {
        state: Value,
        effects: Vec<ActorSocketEffect>,
    },
    Failed {
        code: String,
        message: String,
    },
    Evicted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
        time::{Duration, timeout},
    };

    #[tokio::test]
    async fn residency_reports_are_separate_from_invocation_cache_hints() -> Result<()> {
        let root = TempDir::new_in("/tmp")?;
        let socket = root.path().join("residency.sock");
        let listener = ActorExecutorListener::bind(&socket).await?;
        let mut peer = BufReader::new(UnixStream::connect(&socket).await?);
        write_json_line(
            &mut peer,
            &json!({"type":"attach", "protocol":16, "actor_types":["Room"]}),
        )
        .await?;
        let connection = listener.accept().await?;
        connection.mark_ready(None, None).await?;
        let executor = connection.executor();
        assert!(executor.resident_actors().is_none());
        let mut changes = executor
            .residency_changes()
            .expect("residency notifications");
        let actor = ActorKey {
            actor_type: "Room".into(),
            actor_id: "one".into(),
        };
        for actors in [vec![actor], vec![]] {
            write_json_line(&mut peer, &json!({"type":"residency", "actors":actors})).await?;
            timeout(Duration::from_secs(1), changes.changed()).await??;
            timeout(Duration::from_secs(1), async {
                while executor.resident_actors() != Some(actors.clone()) {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn connection_lookup_is_scoped_to_its_invocation_and_does_not_block_other_actors()
    -> Result<()> {
        struct Source {
            requested: mpsc::UnboundedSender<ActorKey>,
            release: Arc<tokio::sync::Semaphore>,
        }
        #[async_trait]
        impl ActorSocketSource for Source {
            async fn connections(&self, actor: &ActorKey) -> Result<Vec<ActorSocketConnection>> {
                self.requested.send(actor.clone())?;
                self.release.acquire().await?.forget();
                Ok(vec![ActorSocketConnection {
                    id: actor.actor_id.clone(),
                    metadata: json!({}),
                    tags: vec![],
                }])
            }
        }
        let (host, customer) = UnixStream::pair()?;
        let (reader, writer) = host.into_split();
        let (executor, running) =
            JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
        let (requested, mut requests) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        executor
            .mark_ready(
                None,
                Some(Arc::new(Source {
                    requested,
                    release: release.clone(),
                })),
            )
            .await?;
        let peer = async {
            let mut customer = BufReader::new(customer);
            assert_eq!(read_json_line(&mut customer).await?["type"], "attached");
            let slow = read_json_line(&mut customer).await?;
            write_json_line(&mut customer, &json!({"type":"get_connections", "message_id":slow["message_id"], "actor": {"actor_type":"forged","actor_id":"forged"}})).await?;
            let fast = read_json_line(&mut customer).await?;
            assert_eq!(fast["command"]["actor"]["actor_id"], "fast");
            write_json_line(&mut customer, &json!({"type":"reply", "message_id":fast["message_id"], "reply":{"type":"invoked", "result":42, "state":{}}})).await?;
            let loaded = read_json_line(&mut customer).await?;
            assert_eq!(loaded["type"], "socket_connections");
            assert_eq!(loaded["message_id"], slow["message_id"]);
            assert_eq!(loaded["connections"][0]["id"], "slow");
            write_json_line(&mut customer, &json!({"type":"reply", "message_id":slow["message_id"], "reply":{"type":"invoked", "result":1, "state":{}}})).await?;
            anyhow::Ok(())
        };
        let invoke = |id: &str| {
            executor.invoke(
                ActorMethodInvocation {
                    request_id: id.into(),
                    actor: ActorKey {
                        actor_type: "counter".into(),
                        actor_id: id.into(),
                    },
                    method: "run".into(),
                    args: vec![],
                },
                None,
            )
        };
        let callers = async {
            let fast = async {
                let actor = requests.recv().await.context("no lookup")?;
                assert_eq!(actor.actor_type, "counter");
                assert_eq!(actor.actor_id, "slow");
                assert!(
                    matches!(invoke("fast").await?, ActorMethodOutcome::Completed { result, .. } if result == json!(42))
                );
                release.add_permits(1);
                anyhow::Ok(())
            };
            let (slow, ()) = tokio::try_join!(invoke("slow"), fast)?;
            assert!(
                matches!(slow, ActorMethodOutcome::Completed { result, .. } if result == json!(1))
            );
            anyhow::Ok(())
        };
        let result = timeout(Duration::from_secs(2), async {
            tokio::try_join!(peer, callers)
        })
        .await;
        running.abort();
        result??;
        Ok(())
    }

    #[tokio::test]
    async fn multiplexes_out_of_order_replies_before_peer_disconnect() -> Result<()> {
        let (host, customer) = UnixStream::pair()?;
        let (reader, writer) = host.into_split();
        let (executor, running) =
            JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
        let peer = tokio::spawn(async move {
            let mut customer = BufReader::new(customer);
            let first = read_json_line(&mut customer).await?;
            let second = read_json_line(&mut customer).await?;
            for command in [second, first] {
                write_json_line(&mut customer, &json!({
                    "type": "reply", "message_id": command["message_id"],
                    "reply": {"type": "invoked", "result": command["command"]["request_id"], "state": {}}
                })).await?;
            }
            anyhow::Ok(())
        });
        let invoke = |id: &str| {
            executor.invoke(
                ActorMethodInvocation {
                    request_id: id.into(),
                    actor: ActorKey {
                        actor_type: "counter".into(),
                        actor_id: id.into(),
                    },
                    method: "get".into(),
                    args: Vec::new(),
                },
                None,
            )
        };
        let replies = timeout(Duration::from_secs(2), async {
            tokio::try_join!(invoke("first"), invoke("second"))
        })
        .await?;
        peer.await??;
        let _ = running.await;
        let (first, second) = replies?;
        assert!(
            matches!(first, ActorMethodOutcome::Completed { result, .. } if result == json!("first"))
        );
        assert!(
            matches!(second, ActorMethodOutcome::Completed { result, .. } if result == json!("second"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_a_peer_that_stopped_reading() -> Result<()> {
        let root = TempDir::new_in("/tmp")?;
        let socket = root.path().join("executor.sock");
        let listener = ActorExecutorListener::bind(&socket).await?;
        let customer = tokio::spawn(async move {
            let stream = UnixStream::connect(socket).await?;
            let mut stream = BufReader::new(stream);
            write_json_line(
                &mut stream,
                &json!({"type":"attach", "protocol":16, "actor_types":["counter"]}),
            )
            .await?;
            let _ = read_json_line(&mut stream).await?;
            std::future::pending::<Result<()>>().await
        });
        let connection = listener.accept().await?;
        connection.mark_ready(None, None).await?;
        let executor = connection.executor();
        let shutdown = CancellationToken::new();
        let mut running = tokio::spawn(connection.run(shutdown.clone()));
        let mut call = tokio::spawn(async move {
            executor
                .invoke(
                    ActorMethodInvocation {
                        request_id: "blocked-write".into(),
                        actor: ActorKey {
                            actor_type: "counter".into(),
                            actor_id: "one".into(),
                        },
                        method: "accept".into(),
                        args: vec![json!("x".repeat(8 * 1024 * 1024))],
                    },
                    None,
                )
                .await
        });
        assert!(timeout(Duration::from_millis(30), &mut call).await.is_err());
        shutdown.cancel();
        let stopped = timeout(Duration::from_millis(200), &mut running).await;
        running.abort();
        call.abort();
        customer.abort();
        stopped.context("executor shutdown waited for a blocked socket writer")???;
        Ok(())
    }

    #[tokio::test]
    async fn one_javascript_executor_runs_until_host_shutdown() -> Result<()> {
        let root = TempDir::new_in("/tmp")?;
        let socket = root.path().join("actor-executor.sock");
        let host = ActorExecutorListener::bind(&socket).await?;
        let customer = tokio::spawn(run_incrementing_customer(socket.clone()));
        let connection = host.accept().await?;
        let executor = connection.executor();
        connection.mark_ready(None, None).await?;
        assert!(executor.supports("counter"));

        let shutdown = CancellationToken::new();
        let connection_task = tokio::spawn(connection.run(shutdown.clone()));
        let outcome = executor
            .invoke(
                ActorMethodInvocation {
                    request_id: "request-1".into(),
                    actor: ActorKey {
                        actor_type: "counter".into(),
                        actor_id: "counter-1".into(),
                    },
                    method: "increment".into(),
                    args: vec![json!(2)],
                },
                None,
            )
            .await?;
        assert_eq!(
            outcome,
            ActorMethodOutcome::Completed {
                result: json!(2),
                state: json!({ "count": 2 }),
                effects: Vec::new(),
            }
        );
        let socket_outcome = executor
            .handle_socket(
                ActorSocketInvocation {
                    request_id: "socket-request-1".into(),
                    actor: ActorKey {
                        actor_type: "counter".into(),
                        actor_id: "counter-1".into(),
                    },
                    event: ActorSocketEvent::Connect {
                        connection: ActorSocketConnection {
                            id: "socket-1".into(),
                            metadata: json!({ "userId": "user-1" }),
                            tags: Vec::new(),
                        },
                    },
                    connections: vec![ActorSocketConnection {
                        id: "socket-1".into(),
                        metadata: json!({ "userId": "user-1" }),
                        tags: Vec::new(),
                    }],
                },
                Some(&json!({ "count": 2 })),
            )
            .await?;
        assert_eq!(
            socket_outcome,
            ActorSocketOutcome::Handled {
                state: json!({ "count": 3 }),
                effects: vec![ActorSocketEffect::Send {
                    connection_id: "socket-1".into(),
                    message: ActorSocketMessage::Text {
                        data: "ready".into()
                    },
                }],
            }
        );
        shutdown.cancel();
        connection_task.await??;
        customer.await??;
        Ok(())
    }

    #[tokio::test]
    async fn resident_commands_omit_state_and_retry_only_an_explicit_hydration_request()
    -> Result<()> {
        let (host, customer) = UnixStream::pair()?;
        let (reader, writer) = host.into_split();
        let (executor, running) =
            JsActorExecutor::start(BufReader::new(reader), writer, vec!["counter".into()]);
        let mut reader = BufReader::new(customer);
        let customer = async {
            let first = read_json_line(&mut reader).await?;
            assert_eq!(first["command"]["state"], json!({"count": 9}));
            write_json_line(&mut reader, &json!({"type":"reply", "message_id":first["message_id"], "reply":json!({"type":"invoked", "result":10,"state":{"count":10}})})).await?;
            let warm = read_json_line(&mut reader).await?;
            assert!(warm["command"].get("state").is_none());
            assert_eq!(warm["command"]["resident_only"], true);
            write_json_line(&mut reader, &json!({"type":"reply", "message_id":warm["message_id"], "reply":json!({"type":"state_required"})})).await?;
            let retry = read_json_line(&mut reader).await?;
            assert_eq!(
                retry["command"]["request_id"],
                warm["command"]["request_id"]
            );
            assert_eq!(retry["command"]["state"], json!({"count": 10}));
            assert_eq!(retry["command"]["resident_only"], false);
            write_json_line(&mut reader, &json!({"type":"reply", "message_id":retry["message_id"], "reply":json!({"type":"invoked", "result":11,"state":{"count":11}})})).await?;
            anyhow::Ok(())
        };
        let invoke = async {
            for count in [9, 10] {
                let outcome = executor
                    .invoke(
                        ActorMethodInvocation {
                            request_id: format!("request-{count}"),
                            actor: ActorKey {
                                actor_type: "counter".into(),
                                actor_id: "one".into(),
                            },
                            method: "increment".into(),
                            args: vec![],
                        },
                        Some(&json!({"count":count})),
                    )
                    .await?;
                assert!(
                    matches!(outcome, ActorMethodOutcome::Completed {result, ..} if result == json!(count + 1))
                );
            }
            anyhow::Ok(())
        };
        tokio::try_join!(customer, invoke)?;
        running.abort();
        Ok(())
    }

    #[tokio::test]
    async fn oversized_commands_are_reported_as_resource_exhausted() -> Result<()> {
        let root = TempDir::new_in("/tmp")?;
        let socket = root.path().join("actor-executor.sock");
        let host = ActorExecutorListener::bind(&socket).await?;
        let customer = tokio::spawn(run_attached_customer(socket.clone()));
        let connection = host.accept().await?;
        let executor = connection.executor();
        connection.mark_ready(None, None).await?;

        let shutdown = CancellationToken::new();
        let connection_task = tokio::spawn(connection.run(shutdown.clone()));
        let outcome = executor
            .invoke(
                ActorMethodInvocation {
                    request_id: "request-1".into(),
                    actor: ActorKey {
                        actor_type: "counter".into(),
                        actor_id: "counter-1".into(),
                    },
                    method: "accept".into(),
                    args: vec![json!("x".repeat(MAX_ACTOR_EXECUTOR_MESSAGE_BYTES))],
                },
                None,
            )
            .await?;

        assert!(matches!(
            outcome,
            ActorMethodOutcome::Failed(ref failure) if failure.code == "resource_exhausted"
        ));
        shutdown.cancel();
        connection_task.await??;
        customer.await??;
        Ok(())
    }

    #[tokio::test]
    async fn oversized_client_messages_are_rejected_before_newline() -> Result<()> {
        let (host, mut customer) = UnixStream::pair()?;
        let (reader, _) = host.into_split();
        let mut reader = BufReader::new(reader);
        let customer = tokio::spawn(async move {
            let chunk = vec![b'x'; 64 * 1024];
            for _ in 0..=MAX_ACTOR_EXECUTOR_MESSAGE_BYTES / chunk.len() {
                customer.write_all(&chunk).await?;
            }
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        });

        let result = timeout(Duration::from_secs(5), read_client_message(&mut reader)).await;
        customer.abort();
        let error = result
            .context("oversized actor executor message was not rejected before newline")?
            .expect_err("oversized actor executor message should fail");
        assert!(error.to_string().contains("exceeds"));
        Ok(())
    }

    async fn run_incrementing_customer(socket: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(socket).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer
            .write_all(b"{\"type\":\"attach\",\"protocol\":16,\"actor_types\":[\"counter\"]}\n")
            .await?;
        ensure!(
            read_json_line(&mut reader).await?
                == json!({ "type": "attached", "protocol": 16, "supports_residency": true })
        );

        let invocation = read_json_line(&mut reader).await?;
        let invocation_id = invocation["message_id"]
            .as_u64()
            .context("invocation message ID")?;
        ensure!(invocation["command"]["type"] == "invoke");
        ensure!(invocation["command"].get("timeout_ms").is_none());
        write_json_line(
            &mut writer,
            &json!({
                "type": "reply",
                "message_id": invocation_id,
                "reply": {
                    "type": "invoked",
                    "result": 2,
                    "state": { "count": 2 }
                }
            }),
        )
        .await?;

        let socket_event = read_json_line(&mut reader).await?;
        let socket_event_id = socket_event["message_id"]
            .as_u64()
            .context("socket event message ID")?;
        ensure!(socket_event["command"]["type"] == "websocket_event");
        ensure!(socket_event["command"]["event"]["type"] == "connect");
        write_json_line(
            &mut writer,
            &json!({
                "type": "reply",
                "message_id": socket_event_id,
                "reply": {
                    "type": "websocket_handled",
                    "state": { "count": 3 },
                    "effects": [{
                        "type": "send",
                        "connection_id": "socket-1",
                        "message": { "type": "text", "data": "ready" }
                    }]
                }
            }),
        )
        .await?;

        let mut trailing = String::new();
        ensure!(
            reader.read_line(&mut trailing).await? == 0,
            "expected Rust host to close the actor executor"
        );
        Ok(())
    }

    async fn run_attached_customer(socket: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(socket).await?;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        writer
            .write_all(b"{\"type\":\"attach\",\"protocol\":16,\"actor_types\":[\"counter\"]}\n")
            .await?;
        ensure!(
            read_json_line(&mut reader).await?
                == json!({ "type": "attached", "protocol": 16, "supports_residency": true })
        );
        let mut trailing = String::new();
        ensure!(
            reader.read_line(&mut trailing).await? == 0,
            "oversized command reached the customer actor executor"
        );
        Ok(())
    }

    async fn read_json_line<R>(reader: &mut R) -> Result<Value>
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        let mut line = String::new();
        ensure!(reader.read_line(&mut line).await? > 0, "expected JSON line");
        Ok(serde_json::from_str(line.trim_end())?)
    }

    async fn write_json_line<W>(writer: &mut W, value: &Value) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        writer
            .write_all(serde_json::to_string(value)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        Ok(())
    }
}
