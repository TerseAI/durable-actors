use super::*;
use crate::request_traces::query::SqlQuery;
use serde_json::json;

#[tokio::test]
async fn sql_supports_bound_filters_and_aggregations() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    store
        .append(&[super::tests::event("one"), super::tests::event("two")])
        .await?;
    let result = store.query(&SqlQuery { sql: "SELECT actor_type, COUNT(*) AS total FROM request_events WHERE actor_id = ? GROUP BY actor_type".into(), params: vec![json!("one")] }).await?;
    assert_eq!(
        result.rows,
        vec![json!({"actor_type":"Counter", "total":2})]
    );
    let result = store
        .query(&SqlQuery {
            sql: "SELECT event_id FROM request_events WHERE actor_id = ?".into(),
            params: vec![json!("one' OR 1=1 --")],
        })
        .await?;
    assert!(result.rows.is_empty());
    Ok(())
}

#[tokio::test]
async fn sql_supports_sqlite_functions_without_a_custom_allowlist() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let result = store
        .query(&SqlQuery {
            sql: "SELECT replace('request-events', '-', '_') AS name, json_array(1, 2) AS values_json"
                .into(),
            params: vec![],
        })
        .await?;
    assert_eq!(
        result.rows,
        vec![json!({"name": "request_events", "values_json": "[1,2]"})]
    );
    Ok(())
}

#[tokio::test]
async fn sql_cannot_modify_storage() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    for sql in [
        "DELETE FROM traces",
        "DROP TABLE traces",
        "PRAGMA user_version",
        "ATTACH DATABASE ':memory:' AS other",
        "SELECT load_extension('bad')",
        "SELECT * FROM pragma_table_info('traces')",
        "SELECT 1; SELECT 2",
        "SELECT 1; DELETE FROM traces",
    ] {
        assert!(
            store
                .query(&SqlQuery {
                    sql: sql.into(),
                    params: vec![]
                })
                .await
                .is_err(),
            "accepted {sql}"
        );
    }
    store
        .append(&[super::tests::event("still-writable")])
        .await
        .context("append after denied queries")?;
    assert_eq!(
        store
            .query(&SqlQuery {
                sql: "SELECT COUNT(*) AS total FROM request_events".into(),
                params: vec![]
            })
            .await?
            .rows,
        vec![json!({"total":1})]
    );
    Ok(())
}

#[tokio::test]
async fn sql_bounds_results_and_interrupts_expensive_queries() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    let result = store.query(&SqlQuery { sql: "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<600) SELECT x FROM n".into(), params: vec![] }).await?;
    assert_eq!(result.rows.len(), 500);
    assert!(result.truncated);
    assert!(store.query(&SqlQuery { sql: "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n) SELECT SUM(x) FROM n".into(), params: vec![] }).await.is_err());
    assert!(
        store
            .query(&SqlQuery {
                sql: "SELECT printf('%100000000s', 'x') AS huge".into(),
                params: vec![]
            })
            .await
            .is_err()
    );
    store
        .append(&[super::tests::event("after-interrupt")])
        .await?;
    Ok(())
}

#[tokio::test]
async fn existing_version_two_history_gains_sql_views_without_losing_events() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("traces.sqlite3");
    let store = SqliteTracePersistence::new(path.clone());
    store.append(&[super::tests::event("saved")]).await?;
    store
        .run(|connection| {
            connection.execute_batch(
                "DROP VIEW request_events; DROP VIEW request_history; PRAGMA user_version = 2;",
            )?;
            Ok(())
        })
        .await?;
    drop(store);
    let restored = SqliteTracePersistence::new(path);
    let result = restored
        .query(&SqlQuery {
            sql: "SELECT event_id, actor_type FROM request_events".into(),
            params: vec![],
        })
        .await?;
    assert_eq!(
        result.rows,
        vec![json!({"event_id":"saved","actor_type":"Counter"})]
    );
    Ok(())
}
