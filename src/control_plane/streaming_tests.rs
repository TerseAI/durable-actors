use super::tests::{FakeLeaseStore, FakeStorageUrls, FakeWarmProvisioner, test_issuer};
use super::*;
use crate::{
    actor::{ActorExecutorListener, ActorSocketPublisher, ActorSocketSource},
    control_plane::{ActorTokenPurpose, ControlPlaneClient, admin::LocalAdminRegistry},
    grpc::ActorHostGrpcService,
    host::{ActorHost, HostEndpoint},
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
    placement::testing::LocalObjectPlacementStore,
    state_transport::{StateTransport, StateWrite},
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::HashMap, process::Stdio, sync::Mutex};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn browser_ticket_connects_renews_without_resetting_metadata_and_expires() -> Result<()> {
    let mut stack = Stack::start().await?;
    let grant = stack
        .grant(None, serde_json::json!({"user":"one"}), 3_000)
        .await?;
    let mut socket = browser_connect(&grant).await?;
    let snapshot = receive(&mut socket).await?;
    assert_eq!(snapshot["type"], "state");
    let ready = receive(&mut socket).await?;
    assert_eq!(ready["type"], "ready");
    let id = ready["connectionId"].as_str().context("connection ID")?;
    let renewal = stack
        .grant(Some(id), serde_json::json!({"user":"one"}), 1_000)
        .await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"renew", "key":renewal["key"]})
                .to_string()
                .into(),
        ))
        .await?;
    assert_eq!(receive(&mut socket).await?["type"], "renewed");
    assert_eq!(
        stack.invoke("clients", vec![]).await?[0]["metadata"]["name"],
        "member"
    );
    assert_eq!(
        stack.invoke("clients", vec![]).await?[0]["tags"],
        serde_json::json!(["member"])
    );
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4408));
    let mut invalid = browser_connect(&renewal).await?;
    let frame = invalid.next().await.context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4401));
    stack.child.kill().await?;
    Ok(())
}

async fn browser_connect(grant: &serde_json::Value) -> Result<Socket> {
    let mut request = grant["websocketUrl"]
        .as_str()
        .context("socket URL")?
        .into_client_request()?;
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "little-actors.v1".parse()?);
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"authorize", "key":grant["key"]})
                .to_string()
                .into(),
        ))
        .await?;
    Ok(socket)
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn generated_browser_sdk_connects_through_proxy_and_renews_automatically() -> Result<()> {
    let mut stack = Stack::start().await?;
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist");
    let script = stack.directory.path().join("browser-test.mjs");
    std::fs::write(
        &script,
        format!(
            r#"
import assert from 'node:assert/strict';
import {{ writeFile }} from 'node:fs/promises';
import {{ setTimeout }} from 'node:timers/promises';
import {{ ActorCompiler }} from {compiler};
import {{ generateClient }} from {generator};
import {{ build }} from 'esbuild';

const directory = {directory};
const actors = new ActorCompiler().compile(directory + '/actors.ts');
await generateClient(actors.map(actor => actor.contract), directory + '/generated');
await build({{entryPoints:[directory + '/generated/frontend.ts'],outfile:directory + '/browser.mjs',
    bundle:true,platform:'browser',format:'esm',alias:{{'little-actors/browser':{browser}}}}});
await build({{entryPoints:[directory + '/generated/proxy.ts'],outfile:directory + '/proxy.mjs',
    bundle:true,platform:'node',format:'esm',external:['little-actors/proxy']}});
const {{ ActorClient }} = await import(directory + '/browser.mjs');
const {{ ActorProxy }} = await import(directory + '/proxy.mjs');
let authorizations = 0;
const client = ActorClient({{ fetch: async () => {{
    authorizations++;
    return Response.json(await ActorProxy.handle({{actorType:'Counter',actorId:'counter-1',metadata:{{user:'one'}},authorizationLifetimeMs:1000}},
        {{controlPlaneUrl:{gateway},apiKey:'test-api-key',namespaceId:'project-1'}}));
}} }});
const room = client.Counter.get('counter-1');
const values = [];
room.subscribe('count', value => values.push(value));
const errors = [];
room.on('error', error => errors.push(error));
await room.connect();
assert.deepEqual(values, [0]);
assert.equal(room.state.secret, undefined);
const last = new Promise(resolve => room.on('message', message => {{ if (message.delta === 'last') resolve(); }}));
room.send({{type:'start'}});
while (authorizations < 2) await setTimeout(10);
await setTimeout(250);
await writeFile(directory + '/release', '');
await last;
assert.equal(room.status, 'open');
assert.equal(authorizations, 2);
assert.deepEqual(errors, []);
room.close();
assert.throws(() => room.send({{type:'start'}}), /not open/i);
"#,
            compiler = serde_json::to_string(&format!(
                "file://{}",
                sdk.join("compiler/actor-compiler.js").display()
            ))?,
            generator = serde_json::to_string(&format!(
                "file://{}",
                sdk.join("compiler/client-generator.js").display()
            ))?,
            browser = serde_json::to_string(&sdk.join("browser.js"))?,
            directory = serde_json::to_string(stack.directory.path())?,
            gateway = serde_json::to_string(&stack.gateway)?,
        ),
    )?;
    let mut command = tokio::process::Command::new("node");
    command.arg(script).kill_on_drop(true);
    let result = tokio::time::timeout(Duration::from_secs(15), command.output()).await??;
    ensure!(
        result.status.success(),
        "browser SDK integration failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn browser_receives_committed_public_updates_and_renews_during_handlers() -> Result<()> {
    let mut stack = Stack::start().await?;
    let grant = stack
        .grant(None, serde_json::json!({"user":"one"}), 1_000)
        .await?;
    let mut socket = browser_connect(&grant).await?;
    let snapshot = receive(&mut socket).await?;
    assert!(snapshot["state"].get("secret").is_none());
    assert!(snapshot["state"].get("internal").is_none());
    let ready = receive(&mut socket).await?;
    let id = ready["connectionId"].as_str().context("connection ID")?;
    stack.invoke("change", vec![]).await?;
    let update = receive(&mut socket).await?;
    assert_eq!(update["type"], "state_update");
    assert_eq!(update["changes"], serde_json::json!({"count":2}));
    assert!(update["version"].as_u64() > snapshot["version"].as_u64());
    assert!(stack.invoke("fail", vec![]).await.is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), socket.next())
            .await
            .is_err()
    );
    socket
        .send(Message::Text(
            serde_json::json!({"type":"message","data":{"type":"start"}})
                .to_string()
                .into(),
        ))
        .await?;
    assert_eq!(receive(&mut socket).await?["data"]["delta"], "first");
    let renewal = stack
        .grant(Some(id), serde_json::json!({"user":"one"}), 3_000)
        .await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"renew","key":renewal["key"]})
                .to_string()
                .into(),
        ))
        .await?;
    assert_eq!(receive(&mut socket).await?["type"], "renewed");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    std::fs::write(stack.directory.path().join("release"), "")?;
    assert_eq!(receive(&mut socket).await?["data"]["delta"], "last");
    let changed = stack
        .grant(Some(id), serde_json::json!({"user":"two"}), 3_000)
        .await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"renew","key":changed["key"]})
                .to_string()
                .into(),
        ))
        .await?;
    let frame = socket.next().await.context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4409));
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn browser_rejects_wrong_renewal_and_expires_during_pending_actor_execution() -> Result<()> {
    let mut stack = Stack::start().await?;
    let grant = stack.grant(None, serde_json::json!({}), 3_000).await?;
    let mut socket = browser_connect(&grant).await?;
    receive(&mut socket).await?;
    receive(&mut socket).await?;
    let wrong = stack
        .grant(Some("another-connection"), serde_json::json!({}), 3_000)
        .await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"renew","key":wrong["key"]})
                .to_string()
                .into(),
        ))
        .await?;
    let frame = socket.next().await.context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4403));
    let grant = stack.grant(None, serde_json::json!({}), 1_000).await?;
    let mut socket = browser_connect(&grant).await?;
    receive(&mut socket).await?;
    receive(&mut socket).await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"message","data":{"type":"start"}})
                .to_string()
                .into(),
        ))
        .await?;
    assert_eq!(receive(&mut socket).await?["data"]["delta"], "first");
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4408));
    std::fs::write(stack.directory.path().join("release"), "")?;
    let grant = stack.grant(None, serde_json::json!({}), 3_000).await?;
    let mut socket = browser_connect(&grant).await?;
    receive(&mut socket).await?;
    receive(&mut socket).await?;
    socket
        .send(Message::Text(
            serde_json::json!({"type":"message","data":{"type":"invalid"}})
                .to_string()
                .into(),
        ))
        .await?;
    let frame = socket.next().await.context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4400));
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn broadcast_tag_modes_reach_only_matching_websockets() -> Result<()> {
    let mut stack = Stack::start().await?;
    let mut first = stack.connect().await?;
    receive(&mut first).await?;
    let first_id = stack.invoke("clients", vec![]).await?[0]["id"].clone();
    stack
        .invoke(
            "watchFiles",
            vec![first_id.clone(), serde_json::json!(["file:a"])],
        )
        .await?;

    let mut second = stack.connect().await?;
    receive(&mut second).await?;
    let clients = stack.invoke("clients", vec![]).await?;
    let second_id = clients
        .as_array()
        .unwrap()
        .iter()
        .find(|client| client["id"] != first_id)
        .unwrap()["id"]
        .clone();
    stack
        .invoke(
            "watchFiles",
            vec![second_id, serde_json::json!(["file:a", "file:b"])],
        )
        .await?;

    for mode in ["all", "any"] {
        stack
            .invoke("notifyFiles", vec![serde_json::json!(mode)])
            .await?;
        if mode == "any" {
            assert_eq!(
                receive(&mut first).await?,
                serde_json::json!({"text":"matched"})
            );
        }
        assert_eq!(
            receive(&mut first).await?,
            serde_json::json!({"text":"done"})
        );
        assert_eq!(
            receive(&mut second).await?,
            serde_json::json!({"text":"matched"})
        );
        assert_eq!(
            receive(&mut second).await?,
            serde_json::json!({"text":"done"})
        );
    }
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn ordinary_methods_list_and_address_gateway_connections() -> Result<()> {
    let mut stack = Stack::start().await?;
    assert_eq!(
        stack.invoke("clients", vec![]).await?,
        serde_json::json!([])
    );
    let mut socket = stack.connect().await?;
    receive(&mut socket).await?;
    let clients = stack.invoke("clients", vec![]).await?;
    assert_eq!(clients.as_array().unwrap().len(), 1);
    assert_eq!(
        clients[0]["metadata"],
        serde_json::json!({"name": "member"})
    );
    assert_eq!(clients[0]["tags"], serde_json::json!(["member"]));
    let mut outside = stack.actor.clone();
    outside.namespace_id = "another-project".into();
    assert!(stack.publisher.connections(&outside).await.is_err());
    outside = stack.actor.clone();
    outside.actor_id = "another-actor".into();
    assert!(stack.publisher.connections(&outside).await.is_err());
    let response = reqwest::Client::new()
        .get(format!(
            "{}/v1/namespaces/project-1/actors/Counter/counter-1/connections",
            stack.gateway
        ))
        .bearer_auth(&stack.workflow_token)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    stack
        .invoke("notifyClient", vec![clients[0]["id"].clone()])
        .await?;
    assert_eq!(
        receive(&mut socket).await?,
        serde_json::json!({"text": "from method"})
    );
    assert_eq!(
        stack.invoke("clients", vec![]).await?[0]["metadata"]["notified"],
        true
    );
    socket.close(None).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if stack.invoke("clients", vec![]).await? == serde_json::json!([]) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    stack
        .leases
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    assert!(stack.publisher.connections(&stack.actor).await.is_err());
    assert!(stack.invoke("clients", vec![]).await.is_err());
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build; exercises a handler longer than 30 seconds"]
async fn streams_through_real_worker_host_and_gateway_then_catches_up_reconnect() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let mut stack = Stack::start().await?;
    let mut first = stack.connect().await?;
    assert_eq!(receive(&mut first).await?["state"]["history"], "");
    first
        .send(Message::Text(
            serde_json::json!({"type": "start"}).to_string().into(),
        ))
        .await?;
    assert_eq!(receive(&mut first).await?["delta"], "first");

    let mut late = stack.connect().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), late.next())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_secs(31)).await;
    std::fs::write(stack.directory.path().join("release"), "")?;
    assert_eq!(receive(&mut first).await?["delta"], "last");
    assert_eq!(receive(&mut late).await?["state"]["history"], "firstlast");

    let mut outside = stack.actor.clone();
    outside.namespace_id = "another-project".into();
    assert!(stack.publisher.publish(&outside, vec![]).await.is_err());
    stack
        .leases
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    assert!(stack.publisher.publish(&stack.actor, vec![]).await.is_err());
    first.close(None).await?;
    late.close(None).await?;
    stack.child.kill().await?;
    Ok(())
}

struct Stack {
    directory: tempfile::TempDir,
    tasks: JoinSet<()>,
    child: tokio::process::Child,
    gateway: String,
    workflow_token: String,
    actor: ActorKey,
    host_id: HostId,
    leases: Arc<FakeLeaseStore>,
    publisher: Arc<ControlPlaneClient>,
    host: Arc<ActorHost>,
}

impl Stack {
    async fn grant(
        &self,
        connection_id: Option<&str>,
        metadata: serde_json::Value,
        lifetime: u64,
    ) -> Result<serde_json::Value> {
        reqwest::Client::new().post(format!("{}/v1/namespaces/project-1/actors/Counter/counter-1/socket-ticket", self.gateway))
            .bearer_auth("test-api-key")
            .json(&serde_json::json!({"metadata":metadata,"connectionId":connection_id,"authorizationLifetimeMs":lifetime}))
            .send().await?.error_for_status()?.json().await.map_err(Into::into)
    }
    async fn start() -> Result<Self> {
        let directory = tempfile::TempDir::new_in(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk"),
        )?;
        let mut tasks = JoinSet::new();
        let issuer = test_issuer()?;
        let actor = ActorKey {
            namespace_id: "project-1".into(),
            actor_type: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        let host_id = HostId::new("host.v1.project-1.revision.session");
        let host_listener = TcpListener::bind("127.0.0.1:0").await?;
        let host_route = format!("http://{}", host_listener.local_addr()?);
        let leases = Arc::new(FakeLeaseStore {
            leases: Mutex::new(HashMap::new()),
        });
        leases
            .register(&HostLeaseRequest {
                id: host_id.clone(),
                session_id: "00000000-0000-4000-8000-000000000001".into(),
                route: host_route.clone(),
                duration_ms: 60_000,
            })
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        placements
            .claim(&actor.storage_key(), None, &host_id, "us-east")
            .await?;
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .ensure_namespace_and_register_deployment(&HostLaunchSpec {
                namespace_id: actor.namespace_id.clone(),
                code_revision: "revision".into(),
                image_ref: "test-image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
                socket_gateway_url: None,
            })
            .await?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let service = ControlPlaneService::new(
            leases.clone(),
            placements,
            Arc::new(FakeStorageUrls(&["us-east"])),
            auth,
            registry.clone(),
            issuer.clone(),
            Arc::new(FakeWarmProvisioner {
                warmed: tokio::sync::mpsc::unbounded_channel().0,
            }),
        );
        let control_plane = serve_control_plane(&mut tasks, service.clone()).await?;
        let gateway = serve_gateway(
            &mut tasks,
            service,
            AdminService::new("test-api-key".into(), registry, issuer.clone())?,
        )
        .await?;
        let token = issuer
            .issue_host(
                "project-1",
                &host_id,
                "00000000-0000-4000-8000-000000000001",
                "revision",
                "us-east",
            )?
            .token;
        let publisher = Arc::new(
            ControlPlaneClient::connect(control_plane, token)
                .await?
                .with_socket_gateway(&gateway),
        );

        let (child, connection) = start_worker(directory.path()).await?;
        connection.mark_ready(Some(publisher.clone())).await?;
        let host = Arc::new(ActorHost::new(
            HostEndpoint {
                id: host_id.clone(),
                route: host_route,
            },
            "project-1".into(),
            connection.executor(),
            publisher.clone(),
            Arc::new(MemoryState::default()),
            publisher.clone(),
            publisher.clone(),
        ));
        tasks.spawn(async move {
            let _ = connection
                .run(tokio_util::sync::CancellationToken::new())
                .await;
        });
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let serving_host = host.clone();
        tasks.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(ActorHostGrpcService::new(serving_host, auth).into_service())
                .serve_with_incoming(TcpListenerStream::new(host_listener))
                .await;
        });
        let workflow_token = issuer
            .issue_workflow(
                "project-1",
                "test-workflow",
                "us-east",
                (unix_seconds()? + 60) * 1000,
            )?
            .token;
        Ok(Self {
            directory,
            tasks,
            child,
            gateway,
            workflow_token,
            actor,
            host_id,
            leases,
            publisher,
            host,
        })
    }

    async fn invoke(
        &self,
        method: &str,
        args: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let result = self
            .host
            .invoke_actor(
                crate::actor::ActorInvocation {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    actor: self.actor.clone(),
                    method: method.into(),
                    args,
                },
                1,
                0,
                String::new(),
            )
            .await?;
        match result {
            crate::actor::ActorExecutionResult::Completed { result, .. } => Ok(result),
            other => anyhow::bail!("actor call failed: {other:?}"),
        }
    }

    async fn connect(&self) -> Result<Socket> {
        let mut request = format!(
            "{}/v1/namespaces/project-1/actors/Counter/counter-1/websocket",
            self.gateway.replace("http://", "ws://")
        )
        .into_client_request()?;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", self.workflow_token).parse()?,
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
        socket
            .send(Message::Text(
                r#"{"type":"initialize","metadata":{}}"#.into(),
            ))
            .await?;
        Ok(socket)
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

async fn serve_control_plane(
    tasks: &mut JoinSet<()>,
    service: ControlPlaneService,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tasks.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service.into_internal_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Ok(url)
}

async fn serve_gateway(
    tasks: &mut JoinSet<()>,
    service: ControlPlaneService,
    admin: AdminService,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let admin = admin.with_socket_origin(&url)?;
    tasks.spawn(async move {
        let _ = axum::serve(listener, super::super::public_api::router(service, admin)).await;
    });
    Ok(url)
}

async fn start_worker(
    directory: &std::path::Path,
) -> Result<(tokio::process::Child, crate::actor::ActorExecutorConnection)> {
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist");
    ensure!(
        sdk.join("host.js").exists(),
        "run pnpm --dir sdk build before this test"
    );
    let entrypoint = directory.join("actors.ts");
    std::fs::write(
        &entrypoint,
        format!(
            r#"
import {{ Actor, Persisted, Emittable, type ActorSocket }} from {};
import {{ existsSync }} from 'node:fs';
import {{ setTimeout }} from 'node:timers/promises';
export class Counter extends Actor<{{name?:string; notified?:boolean; user?:string}}, {{type:"start"}}, {{delta:string}} | {{text:string}}> {{
    @Persisted history = '';
    @Persisted @Emittable count = 0;
    @Persisted private secret = 'private';
    @Persisted protected internal = 'internal';
    async change() {{ this.count++; this.count++; this.secret = 'changed'; }}
    async fail() {{ this.count++; throw new Error('failed'); }}
    async onConnect(socket: ActorSocket<{{name?:string; notified?:boolean; user?:string}}, {{delta:string}} | {{text:string}}, "member">) {{
        socket.metadata = {{ name: 'member' }};
        socket.setTags('member');
    }}
    async clients() {{
        return this.connections.map(socket => ({{ id: socket.id, metadata: socket.metadata, tags: socket.tags }}));
    }}
    async watchFiles(id: string, tags: string[]) {{
        this.connections.find(socket => socket.id === id)!.setTags(...tags);
    }}
    async notifyFiles(tagMatch: "all" | "any") {{
        this.broadcast({{ text: 'matched' }}, {{ tags: ['file:a', 'file:b'], tagMatch }});
        this.broadcast({{ text: 'done' }});
    }}
    async notifyClient(id: string) {{
        const socket = this.connections.find(socket => socket.id === id)!;
        socket.metadata = {{ ...socket.metadata, notified: true }};
        socket.send({{ text: 'from method' }});
    }}
    async onMessage() {{
        if (process.env.TEST_ACTOR_SECRET !== 'injected') throw new Error('actor secret missing');
        this.history += 'first';
        this.broadcast({{ delta: 'first' }});
        while (!existsSync({})) await setTimeout(10);
        this.history += 'last';
        this.broadcast({{ delta: 'last' }});
    }}
}}
"#,
            serde_json::to_string(&format!("{}", sdk.join("index.js").display()))?,
            serde_json::to_string(&directory.join("release"))?
        ),
    )?;
    std::fs::write(
        directory.join("tsconfig.json"),
        r#"{"compilerOptions":{"target":"ES2022","module":"NodeNext","moduleResolution":"NodeNext","strict":true,"skipLibCheck":true},"include":["actors.ts"]}"#,
    )?;
    let socket = directory.join("executor.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let bootstrap = directory.join("host.mjs");
    std::fs::write(
        &bootstrap,
        format!(
            "import {{ runDurableObjectHost }} from {}; await runDurableObjectHost();",
            serde_json::to_string(&format!("file://{}", sdk.join("host.js").display()))?
        ),
    )?;
    let child = tokio::process::Command::new("node")
        .arg(bootstrap)
        .env("DURABLE_OBJECT_ENTRYPOINT", entrypoint)
        .env("DURABLE_OBJECT_EXECUTOR_SOCKET", socket)
        .env("TEST_ACTOR_SECRET", "injected")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let connection = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await??;
    Ok((child, connection))
}

async fn receive(socket: &mut Socket) -> Result<serde_json::Value> {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await?
        .context("socket closed")??;
    ensure!(frame.is_text(), "unexpected socket frame: {frame:?}");
    serde_json::from_str(frame.to_text()?)
        .with_context(|| format!("invalid socket JSON: {frame:?}"))
}

#[derive(Default)]
struct MemoryState(Mutex<Vec<u8>>);

#[async_trait]
impl StateTransport for MemoryState {
    async fn read(&self, _: &str) -> Result<bytes::Bytes> {
        Ok(self.0.lock().unwrap().clone().into())
    }
    async fn write(&self, _: &str, bytes: Vec<u8>) -> Result<StateWrite> {
        *self.0.lock().unwrap() = bytes;
        Ok(StateWrite::Written)
    }
}
