use super::*;
use crate::{
    actor::ActorKey,
    actor_logs::{LogLine, LogQuery, LogStore, LogStream},
};

fn lobby() -> ActorKey {
    ActorKey {
        project_id: "local".into(),
        actor_name: "ChatRoom".into(),
        actor_id: "lobby".into(),
    }
}

fn lines(count: u64) -> Vec<LogLine> {
    (0..count)
        .map(|sequence| LogLine {
            sequence,
            timestamp_ms: 1_000 + sequence,
            stream: LogStream::Stdout,
            message: format!("line {sequence}"),
        })
        .collect()
}

async fn messages(store: &LogStore) -> Result<Vec<String>> {
    let query = LogQuery {
        actor_name: "ChatRoom".into(),
        actor_id: "lobby".into(),
        session_id: None,
        before: None,
        limit: None,
    };
    let page = store.page(&query.filter("local")?).await?;
    Ok(page
        .lines
        .into_iter()
        .map(|event| event.line.message)
        .collect())
}

#[tokio::test]
async fn logs_survive_reopening_the_database() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor-logs.sqlite3");
    LogStore::new(Arc::new(SqliteLogs::open(path.clone()).await?))
        .record(&lobby(), "host-1", "session-1", lines(2))
        .await?;
    let reopened = LogStore::new(Arc::new(SqliteLogs::open(path).await?));
    assert_eq!(messages(&reopened).await?, ["line 0", "line 1"]);
    Ok(())
}

#[tokio::test]
async fn keeps_only_the_newest_lines() -> Result<()> {
    let logs = SqliteLogs::open_retaining(":memory:".into(), 2).await?;
    let store = LogStore::new(Arc::new(logs));
    store
        .record(&lobby(), "host-1", "session-1", lines(3))
        .await?;
    assert_eq!(messages(&store).await?, ["line 1", "line 2"]);
    Ok(())
}
