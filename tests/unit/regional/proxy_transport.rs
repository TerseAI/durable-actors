use super::*;
use futures_util::SinkExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone)]
struct Primary(Arc<AtomicUsize>);

#[tonic::async_trait]
impl ActorService for Primary {
    async fn invoke(&self, request: Request<ActorCall>) -> Result<Response<ActorReply>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer primary-capability"
        );
        assert_eq!(
            request.metadata().get("x-request-id").unwrap(),
            "correlation"
        );
        self.0.fetch_add(1, Ordering::SeqCst);
        let call = request.into_inner();
        if call.method == "lost" {
            return Err(Status::unavailable("response lost after commit"));
        }
        if call.method == "slow" {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(Response::new(ActorReply {
            reply_json: serde_json::to_vec(
                &serde_json::json!({"type":"completed","result":call.request_id}),
            )
            .unwrap(),
        }))
    }

    async fn publish(&self, _: Request<SocketEffects>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    type SocketSessionStream = ReceiverStream<Result<SocketFrame, Status>>;
    async fn socket_session(
        &self,
        request: Request<Streaming<SocketFrame>>,
    ) -> Result<Response<Self::SocketSessionStream>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer primary-capability"
        );
        let mut inbound = request.into_inner();
        let (send, receive) = mpsc::channel(2);
        tokio::spawn(async move {
            let initial = encode_frame(Message::Text("{\"type\":\"state\",\"version\":7}".into()));
            if send.send(Ok(initial)).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) = inbound.message().await {
                if send.send(Ok(frame)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receive)))
    }
}

#[tokio::test]
async fn proxy_forwards_once_with_scope_trace_and_deadline() -> Result<()> {
    let (proxy_url, issuer, config, primary_url, calls, servers) = fixture().await?;
    let mut client = forward::client(&proxy_url)?;
    let token = ticket(&issuer, &config, &primary_url, ProxyTransport::Invocation)?;
    for method in ["increment", "lost", "slow"] {
        let mut request = Request::new(call(method));
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
        request
            .metadata_mut()
            .insert("x-request-id", "correlation".parse()?);
        request.set_timeout(Duration::from_millis(100));
        let started = std::time::Instant::now();
        let result = client.invoke(request).await;
        assert!(started.elapsed() < Duration::from_millis(500));
        match method {
            "increment" => assert!(result.is_ok(), "{result:?}"),
            "lost" => assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable),
            "slow" => assert!(matches!(
                result.unwrap_err().code(),
                tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
            )),
            _ => unreachable!(),
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let mut request = Request::new(ActorCall {
        actor_id: "other-tenant".into(),
        ..call("increment")
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    assert_eq!(
        client.invoke(request).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    for server in servers {
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn proxy_readiness_accepts_only_the_scoped_invocation_ticket_without_dispatch() -> Result<()>
{
    let (origin, issuer, config, primary, calls, servers) = fixture().await?;
    let client = reqwest::Client::new();
    let url = format!("{origin}/v1/projects/project/actors/Counter/one/invoke");
    let invocation = ticket(&issuer, &config, &primary, ProxyTransport::Invocation)?;
    let socket = ticket(&issuer, &config, &primary, ProxyTransport::Socket)?;
    assert_eq!(
        client
            .head(&url)
            .bearer_auth(&invocation)
            .send()
            .await?
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        client.head(&url).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client.head(&url).bearer_auth(socket).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .head(url.replace("/one/", "/other/"))
            .bearer_auth(invocation)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for server in servers {
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn proxy_websocket_receives_unsolicited_primary_effects_and_forwards_client_frames()
-> Result<()> {
    let (proxy_url, issuer, config, primary_url, _, servers) = fixture().await?;
    let token = ticket(&issuer, &config, &primary_url, ProxyTransport::Socket)?;
    let url = format!(
        "{}/v1/socket?key={token}",
        proxy_url.replace("http:", "ws:")
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await?;
    let initial = socket
        .next()
        .await
        .transpose()?
        .context("primary did not send initial state")?;
    assert!(initial.to_text()?.contains("\"version\":7"));
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            "hello".into(),
        ))
        .await?;
    assert_eq!(
        socket.next().await.transpose()?.unwrap().to_text()?,
        "hello"
    );
    socket.close(None).await?;
    for server in servers {
        server.abort();
    }
    Ok(())
}

fn call(method: &str) -> ActorCall {
    ActorCall {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
        request_id: "correlation".into(),
        owner_epoch: 3,
        method: method.into(),
        args_json: b"[]".to_vec(),
    }
}

fn ticket(
    issuer: &crate::control_plane::ActorJwtIssuer,
    config: &ProxyConfig,
    primary: &str,
    kind: ProxyTransport,
) -> Result<String> {
    issuer.issue_proxy(&ProxyTicket::new(
        config,
        ProxyDestination {
            route: primary.into(),
            token: "primary-capability".into(),
            owner_epoch: 3,
            expires_at_ms: crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64 + 60_000,
            authorized_until_ms: (kind == ProxyTransport::Socket).then_some(
                crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64 + 60_000,
            ),
            kind,
        },
    )?)
}

type TestServer = tokio::task::JoinHandle<std::io::Result<()>>;

async fn fixture() -> Result<(
    String,
    crate::control_plane::ActorJwtIssuer,
    ProxyConfig,
    String,
    Arc<AtomicUsize>,
    Vec<TestServer>,
)> {
    use base64::Engine;
    let key = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(
        &aws_lc_rs::rand::SystemRandom::new(),
    )?;
    let issuer = crate::control_plane::ActorJwtIssuer::from_base64_pkcs8(
        &base64::engine::general_purpose::STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(86400),
    )?;
    let config = ProxyConfig {
        actor: ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        session: uuid::Uuid::new_v4().to_string(),
        region: Region::East,
        keys: issuer.verifier_keys_json()?,
        issuer: "issuer".into(),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let primary = tonic::service::Routes::new(ActorServiceServer::new(Primary(calls.clone())))
        .into_axum_router();
    let (primary_url, primary_server) = serve(primary).await?;
    let (proxy_url, proxy_server) = serve(router(config.clone())?).await?;
    Ok((
        proxy_url,
        issuer,
        config,
        primary_url,
        calls,
        vec![primary_server, proxy_server],
    ))
}

async fn serve(routes: Router) -> Result<(String, TestServer)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    Ok((
        url,
        tokio::spawn(async move { axum::serve(listener, routes).await }),
    ))
}

#[tokio::test]
async fn proxy_socket_bursts_apply_backpressure_without_losing_frames() -> Result<()> {
    let (proxy_url, issuer, config, primary_url, _, servers) = fixture().await?;
    let token = ticket(&issuer, &config, &primary_url, ProxyTransport::Socket)?;
    let (socket, _) = tokio_tungstenite::connect_async(format!(
        "{}/v1/socket?key={token}",
        proxy_url.replace("http:", "ws:")
    ))
    .await?;
    let (mut send, mut receive) = socket.split();
    receive.next().await.transpose()?.context("initial frame")?;
    let payload = "x".repeat(16 * 1024);
    let sent = async {
        for index in 0..256 {
            send.send(tokio_tungstenite::tungstenite::Message::Text(
                format!("{index}:{payload}").into(),
            ))
            .await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let received = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for index in 0..256 {
            let message = receive
                .next()
                .await
                .transpose()?
                .context("burst connection closed")?;
            assert_eq!(message.to_text()?, format!("{index}:{payload}"));
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::try_join!(sent, received)
    })
    .await??;
    for server in servers {
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn proxy_enforces_socket_expiry_independently_of_primary_delivery() -> Result<()> {
    let (origin, issuer, config, primary, _, servers) = fixture().await?;
    let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
    let grant = ProxyTicket::new(
        &config,
        ProxyDestination {
            route: primary,
            token: "primary-capability".into(),
            owner_epoch: 3,
            expires_at_ms: now + 2_000,
            authorized_until_ms: Some(now + 2_000),
            kind: ProxyTransport::Socket,
        },
    )?;
    let token = issuer.issue_proxy(&grant)?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!(
        "{}/v1/socket?key={token}",
        origin.replace("http:", "ws:")
    ))
    .await?;
    socket.next().await.transpose()?.context("initial frame")?;
    let closed = tokio::time::timeout(Duration::from_secs(4), socket.next()).await;
    for server in servers {
        server.abort();
    }
    let frame = closed?
        .transpose()?
        .context("proxy closed without a close frame")?;
    assert!(
        matches!(frame, tokio_tungstenite::tungstenite::Message::Close(Some(frame)) if u16::from(frame.code) == 4408)
    );
    Ok(())
}
