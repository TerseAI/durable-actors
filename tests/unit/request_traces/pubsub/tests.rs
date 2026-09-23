use super::*;
use crate::request_traces::{persistence::tests::event, sink::TraceScope};
use std::sync::Mutex;

struct Publisher {
    messages: Mutex<Vec<serde_json::Value>>,
    ready: tokio::sync::Semaphore,
    fail: bool,
}
#[async_trait]
impl EventPublisher for Publisher {
    async fn publish(&self, data: Vec<u8>) -> Result<()> {
        self.messages
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&data)?);
        self.ready.acquire().await?.forget();
        ensure!(!self.fail, "publish failed");
        Ok(())
    }
}
struct Clock;
impl crate::clock::Clock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(86_400_000)
    }
}
fn scope() -> TraceScope {
    TraceScope {
        project_id: "tenant".into(),
        host_id: "host".into(),
        session_id: "session".into(),
        region: "us".into(),
    }
}
fn publisher(permits: usize, fail: bool) -> Arc<Publisher> {
    Arc::new(Publisher {
        messages: Mutex::new(vec![]),
        ready: tokio::sync::Semaphore::new(permits),
        fail,
    })
}
fn sink(publisher: Arc<Publisher>) -> PubSubTraceSink {
    PubSubTraceSink::new(
        publisher,
        Arc::new(Clock),
        "production".into(),
        vec!["name".into()],
        30,
        1,
    )
}

#[tokio::test]
async fn export_preserves_identity_and_only_approved_metadata() -> Result<()> {
    let publisher = publisher(1, false);
    let sink = sink(publisher.clone());
    let mut trace = event("stable").trace;
    trace.metadata = Some(serde_json::json!({"name":"Ada", "token":"secret"}));
    sink.record(&scope(), vec![trace], 0).await?;
    let messages = publisher.messages.lock().unwrap();
    assert_eq!(messages[0]["event_id"], "stable");
    assert_eq!(messages[0]["project_id"], "tenant");
    assert_eq!(messages[0]["schema_version"], 1);
    assert_eq!(messages[0]["started_at"], "1970-01-01T00:00:00.001Z");
    assert_eq!(messages[0]["metadata"], r#"{"name":"Ada"}"#);
    Ok(())
}

#[tokio::test]
async fn cancelled_reports_keep_capacity_until_publish_finishes() -> Result<()> {
    let publisher = publisher(0, false);
    let sink = Arc::new(sink(publisher.clone()));
    let task_sink = sink.clone();
    let task =
        tokio::spawn(async move { task_sink.record(&scope(), vec![event("a").trace], 0).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while publisher.messages.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    task.abort();
    assert!(
        sink.record(&scope(), vec![event("b").trace], 0)
            .await
            .is_err()
    );
    publisher.ready.add_permits(1);
    sink.flush().await?;
    assert_eq!(sink.pending.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn failure_never_acknowledges_durable_publication() {
    let publisher = publisher(1, true);
    assert!(
        sink(publisher)
            .record(&scope(), vec![event("a").trace], 0)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn invalid_timestamps_are_rejected_before_publication() {
    let publisher = publisher(2, false);
    let sink = sink(publisher.clone());
    let mut trace = event("a").trace;
    trace.started_at_ms = 86_400_000 + 300_001;
    assert!(sink.record(&scope(), vec![trace], 0).await.is_err());
    assert!(publisher.messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn published_json_matches_the_bigquery_table_schema() -> Result<()> {
    let publisher = publisher(1, false);
    let sink = sink(publisher.clone());
    sink.record(&scope(), vec![event("event").trace], 0).await?;
    let schema: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("../../../fixtures/analytics-schema.json"))?;
    let messages = publisher.messages.lock().unwrap();
    let object = messages[0].as_object().unwrap();
    for (key, value) in object {
        let column = schema
            .iter()
            .find(|field| field["name"] == *key)
            .expect("publisher field must exist in table");
        if value.is_null() {
            assert_eq!(column["mode"], "NULLABLE", "{key}");
            continue;
        }
        match column["type"].as_str().unwrap() {
            "TIMESTAMP" => {
                chrono::DateTime::parse_from_rfc3339(value.as_str().unwrap())?;
            }
            "INTEGER" => assert!(value.is_i64()),
            "FLOAT" => assert!(value.is_number()),
            "JSON" => {
                serde_json::from_str::<serde_json::Value>(value.as_str().unwrap())?;
            }
            "STRING" => assert!(value.is_string()),
            kind => panic!("unhandled type {kind}"),
        }
    }
    for column in schema.iter().filter(|field| field["mode"] == "REQUIRED") {
        let name = column["name"].as_str().unwrap();
        assert!(
            object.contains_key(name)
                || ["subscription_name", "message_id", "publish_time"].contains(&name)
        );
    }
    Ok(())
}
