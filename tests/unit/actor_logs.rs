use super::*;

fn chat_room(actor_id: &str) -> ActorKey {
    ActorKey {
        project_id: "local".into(),
        actor_name: "ChatRoom".into(),
        actor_id: actor_id.into(),
    }
}

fn line(sequence: u64) -> LogLine {
    LogLine {
        sequence,
        timestamp_ms: 1_000 + sequence,
        stream: LogStream::Stdout,
        message: format!("line {sequence}"),
    }
}

fn query(before: Option<String>) -> LogQuery {
    LogQuery {
        actor_name: "ChatRoom".into(),
        actor_id: "lobby".into(),
        session_id: None,
        before,
        limit: Some(3),
    }
}

fn messages(page: &LogPage) -> Vec<&str> {
    page.lines
        .iter()
        .map(|event| event.line.message.as_str())
        .collect()
}

async fn store() -> Result<LogStore> {
    Ok(LogStore::new(Arc::new(
        sqlite::SqliteLogs::open(":memory:".into()).await?,
    )))
}

#[tokio::test]
async fn pages_backwards_through_one_actor() -> Result<()> {
    let store = store().await?;
    let lines = (0..5).map(line).collect();
    store
        .record(&chat_room("lobby"), "host-1", "session-1", lines)
        .await?;
    store
        .record(&chat_room("other"), "host-2", "session-2", vec![line(9)])
        .await?;

    let newest = store.page(&query(None).filter("local")?).await?;
    assert_eq!(messages(&newest), ["line 2", "line 3", "line 4"]);

    let older = store
        .page(&query(newest.before.clone()).filter("local")?)
        .await?;
    assert_eq!(messages(&older), ["line 0", "line 1"]);
    assert!(older.before.is_none());
    Ok(())
}

#[tokio::test]
async fn filters_one_session() -> Result<()> {
    let store = store().await?;
    let lobby = chat_room("lobby");
    store
        .record(&lobby, "host-1", "session-1", vec![line(0)])
        .await?;
    store
        .record(&lobby, "host-2", "session-2", vec![line(1)])
        .await?;
    let mut filtered = query(None);
    filtered.session_id = Some("session-1".into());
    let page = store.page(&filtered.filter("local")?).await?;
    assert_eq!(messages(&page), ["line 0"]);
    Ok(())
}

#[tokio::test]
async fn rejects_oversized_batches() -> Result<()> {
    let lines = (0..=LOG_BATCH_LINES as u64).map(line).collect();
    let recorded = store()
        .await?
        .record(&chat_room("lobby"), "host-1", "session-1", lines)
        .await;
    assert!(recorded.is_err());
    Ok(())
}

#[test]
fn rejects_malformed_cursors() {
    assert!(query(Some("not-a-cursor".into())).filter("local").is_err());
}
