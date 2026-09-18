use std::time::{Duration, Instant};

use anyhow::{Result, bail, ensure};
use rusqlite::{
    Connection,
    hooks::{AuthAction, AuthContext, Authorization},
    limits::Limit,
    params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use serde_json::{Map, Value};

use crate::request_traces::query::{SqlQuery, SqlQueryError, SqlResult};

pub(super) fn query(connection: &Connection, query: &SqlQuery) -> Result<SqlResult> {
    let _guard = QueryGuard::new(connection)?;
    execute(connection, query).map_err(|error| SqlQueryError(error.to_string()).into())
}

fn execute(connection: &Connection, query: &SqlQuery) -> Result<SqlResult> {
    let mut statement = connection.prepare(&query.sql)?;
    ensure!(
        statement.readonly() && statement.column_count() > 0,
        "only read queries are allowed"
    );
    let columns: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    ensure!(
        columns
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            == columns.len(),
        "give duplicate result columns distinct aliases"
    );
    let params = query
        .params
        .iter()
        .map(parameter)
        .collect::<Result<Vec<_>>>()?;
    let mut cursor = statement.query(params_from_iter(params))?;
    let mut rows = Vec::new();
    let mut bytes = 0;
    while let Some(row) = cursor.next()? {
        if rows.len() == 500 {
            return Ok(SqlResult {
                rows,
                truncated: true,
            });
        }
        let mut object = Map::new();
        for (index, name) in columns.iter().enumerate() {
            object.insert(name.clone(), json_value(row.get_ref(index)?)?);
        }
        let value = Value::Object(object);
        bytes += serde_json::to_vec(&value)?.len();
        ensure!(
            bytes <= 4 * 1024 * 1024,
            "query result exceeds 4 MiB; select fewer rows or columns"
        );
        rows.push(value);
    }
    Ok(SqlResult {
        rows,
        truncated: false,
    })
}

fn parameter(value: &Value) -> Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(*value)),
        Value::Number(value) if value.is_f64() => SqlValue::Real(value.as_f64().unwrap()),
        Value::Number(value) => SqlValue::Integer(
            value
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("integer parameter is out of range"))?,
        ),
        Value::String(value) => SqlValue::Text(value.clone()),
        _ => bail!("parameters must be JSON scalars"),
    })
}

fn json_value(value: ValueRef<'_>) -> Result<Value> {
    Ok(match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => value.into(),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .ok_or_else(|| anyhow::anyhow!("non-finite query result"))?
            .into(),
        ValueRef::Text(value) => std::str::from_utf8(value)?.into(),
        ValueRef::Blob(_) => bail!("binary results are not supported"),
    })
}

fn authorize(context: AuthContext<'_>) -> Authorization {
    use AuthAction::*;
    let allowed = match context.action {
        Select | Recursive | Read { .. } => true,
        Function { function_name } => function_name != "load_extension",
        _ => false,
    };
    if allowed {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}

struct QueryGuard<'a> {
    connection: &'a Connection,
    value_limit: i32,
}

impl<'a> QueryGuard<'a> {
    fn new(connection: &'a Connection) -> Result<Self> {
        let guard = Self {
            connection,
            value_limit: connection.set_limit(Limit::SQLITE_LIMIT_LENGTH, 1_048_576)?,
        };
        connection.authorizer(Some(authorize))?;
        let started = Instant::now();
        connection.progress_handler(
            1000,
            Some(move || started.elapsed() >= Duration::from_millis(500)),
        )?;
        Ok(guard)
    }
}

impl Drop for QueryGuard<'_> {
    fn drop(&mut self) {
        // Restore the writer connection even after a denied or interrupted query.
        let _ = self
            .connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        let _ = self.connection.progress_handler(0, None::<fn() -> bool>);
        let _ = self
            .connection
            .set_limit(Limit::SQLITE_LIMIT_LENGTH, self.value_limit);
    }
}
