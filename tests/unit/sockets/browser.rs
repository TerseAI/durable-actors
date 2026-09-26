use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Dispatcher(AtomicBool);
#[async_trait]
impl SocketDispatcher for Dispatcher {
    async fn authorize(&self, _: &SocketTicket) -> Result<()> {
        Ok(())
    }
    fn ensure_authority(&self) -> Result<()> {
        ensure!(self.0.load(Ordering::SeqCst), "lease expired");
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &SocketTicket,
        _: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        Ok(vec![])
    }
}
struct SlowSocket(Duration);
#[async_trait]
impl SocketTransport for SlowSocket {
    async fn recv(&mut self) -> Option<Result<Message>> {
        std::future::pending().await
    }
    async fn send(&mut self, _: Message) -> Result<()> {
        tokio::time::sleep(self.0).await;
        Ok(())
    }
}
fn session() -> Result<(Session, Arc<Dispatcher>)> {
    let dispatcher = Arc::new(Dispatcher(AtomicBool::new(true)));
    let ticket: SocketTicket = serde_json::from_value(serde_json::json!({
        "iss":"test", "aud":"test", "scope":"actor:socket", "iat":0,"nbf":0,"exp":0,
        "actor":{"project_id":"project","actor_name":"Counter","actor_id":"one"},
        "region":"north-america-west", "target":null,"metadata":{},
        "connectByMs":now_ms()+60000,"authorizedUntilMs":now_ms()+60000
    }))?;
    Ok((
        Session {
            state: SocketServerState {
                registry: SocketRegistry::default(),
                verifier: SocketTicketVerifier::new(
                    r#"{"keys":[]}"#,
                    "test".into(),
                    "test".into(),
                )?,
                dispatcher: dispatcher.clone(),
                stop: CancellationToken::new(),
            },
            ticket,
            connection: ActorSocketConnection {
                id: "socket".into(),
                metadata: serde_json::json!({}),
                tags: vec![],
            },
            outbound: socket_channel().1,
            pending: VecDeque::new(),
            handler: None,
        },
        dispatcher,
    ))
}
#[tokio::test]
async fn actor_input_retains_a_burst_while_its_handler_is_busy() -> Result<()> {
    let (mut session, _) = session()?;
    session.handler = Some(tokio::spawn(std::future::pending()));
    for index in 0..2048 {
        assert!(
            session
                .enqueue(ActorSocketMessage::Text {
                    data: index.to_string()
                })
                .is_ok()
        );
    }
    for index in 0..2048 {
        assert!(
            matches!(session.pending.pop_front(), Some((ActorSocketMessage::Text {data}, _)) if data == index.to_string())
        );
    }
    session.handler.take().unwrap().abort();
    Ok(())
}
#[tokio::test]
async fn slow_delivery_can_finish_after_five_seconds() -> Result<()> {
    let (session, _) = session()?;
    assert!(
        session
            .send_frame(
                &mut SlowSocket(Duration::from_secs(6)),
                Message::Text("hello".into())
            )
            .await
            .is_ok()
    );
    Ok(())
}
#[tokio::test]
async fn slow_delivery_still_obeys_shutdown_and_lease_loss() -> Result<()> {
    for shutdown in [true, false] {
        let (session, dispatcher) = session()?;
        let stop = session.state.stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if shutdown {
                stop.cancel();
            } else {
                dispatcher.0.store(false, Ordering::SeqCst);
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            session.send_frame(
                &mut SlowSocket(Duration::from_secs(60)),
                Message::Text("hello".into()),
            ),
        )
        .await?;
        assert_eq!(result.unwrap_err().0, 1012);
    }
    Ok(())
}

#[tokio::test]
async fn slow_delivery_stops_at_authorization_expiry() -> Result<()> {
    let (mut session, _) = session()?;
    session.ticket.authorized_until_ms = now_ms() + 100;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        session.send_frame(
            &mut SlowSocket(Duration::from_secs(60)),
            Message::Text("hello".into()),
        ),
    )
    .await?;
    assert_eq!(result.unwrap_err(), (4408, "socket authorization expired"));
    Ok(())
}
