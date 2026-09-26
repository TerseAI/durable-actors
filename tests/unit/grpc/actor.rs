use super::*;
use axum::extract::ws::{CloseFrame, Message};

#[test]
fn socket_frames_preserve_text_control_messages_and_close_reasons() -> anyhow::Result<()> {
    for message in [
        Message::Text("{\"type\":\"state\",\"version\":7}".into()),
        Message::Ping(vec![1, 2].into()),
        Message::Pong(vec![3].into()),
        Message::Close(Some(CloseFrame {
            code: 1012,
            reason: "primary replaced".into(),
        })),
    ] {
        let wire = encode_frame(message.clone());
        let decoded = decode_frame(wire)?;
        assert_eq!(format!("{message:?}"), format!("{decoded:?}"));
    }
    assert!(decode_frame(SocketFrame { payload: None }).is_err());
    assert!(
        decode_frame(SocketFrame {
            payload: Some(socket_frame::Payload::Close(SocketClose {
                code: 100_000,
                reason: String::new()
            }))
        })
        .is_err()
    );
    Ok(())
}
