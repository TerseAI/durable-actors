use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json::{
    Value,
    value::{RawValue, to_raw_value},
};
use std::collections::BTreeMap;

#[async_trait]
pub trait ActorDatabase: Send + Sync {
    /// A statement writes its rows to `rows` and returns [`DatabaseResult::Rows`].
    async fn call(&self, call: DatabaseCall, rows: Box<dyn RowSink>) -> Result<DatabaseResult>;
}

/// Receives the rows of a statement as SQLite produces them, so a result is never held whole.
pub trait RowSink: Send {
    fn columns(&mut self, names: &[&str]) -> Result<()>;
    fn row(&mut self, cells: &[SqlCell<'_>]) -> Result<()>;
    /// Called once after the last row.
    fn finish(&mut self) -> Result<()>;
}

/// A value borrowed from the row SQLite is positioned on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SqlCell<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

#[derive(Debug)]
pub enum DatabaseCall {
    Exec {
        sql: String,
        bindings: Vec<SqlValue>,
        immediate: bool,
    },
    /// Every stored field, or only `names` when given.
    Fields {
        names: Option<Vec<String>>,
    },
    InitializeFields {
        fields: BTreeMap<String, Box<RawValue>>,
    },
    PersistField {
        name: String,
        value: Box<RawValue>,
        immediate: bool,
    },
    DeleteField {
        name: String,
        immediate: bool,
    },
    ReplaceFields {
        fields: BTreeMap<String, Box<RawValue>>,
    },
    Alarm,
    SetAlarm {
        timestamp_ms: i64,
        immediate: bool,
    },
    DeleteAlarm {
        immediate: bool,
    },
    Commit,
    Rollback,
}

/// A statement's bound values are not JSON: they follow the call on the database channel.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum DatabaseCallWire {
    Exec {
        sql: String,
        #[serde(default)]
        immediate: bool,
    },
    Fields {
        #[serde(default)]
        names: Option<Vec<String>>,
    },
    InitializeFields {
        fields: BTreeMap<String, Value>,
    },
    PersistField {
        name: String,
        value: Value,
        #[serde(default)]
        immediate: bool,
    },
    DeleteField {
        name: String,
        #[serde(default)]
        immediate: bool,
    },
    ReplaceFields {
        fields: BTreeMap<String, Value>,
    },
    Alarm,
    SetAlarm {
        timestamp_ms: i64,
        #[serde(default)]
        immediate: bool,
    },
    DeleteAlarm {
        #[serde(default)]
        immediate: bool,
    },
    Commit,
    Rollback,
}

impl<'de> Deserialize<'de> for DatabaseCall {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match DatabaseCallWire::deserialize(deserializer)? {
            DatabaseCallWire::Exec { sql, immediate } => Self::Exec {
                sql,
                bindings: Vec::new(),
                immediate,
            },
            DatabaseCallWire::Fields { names } => Self::Fields { names },
            DatabaseCallWire::InitializeFields { fields } => Self::InitializeFields {
                fields: raw_fields(fields)?,
            },
            DatabaseCallWire::PersistField {
                name,
                value,
                immediate,
            } => Self::PersistField {
                name,
                value: to_raw_value(&value).map_err(D::Error::custom)?,
                immediate,
            },
            DatabaseCallWire::DeleteField { name, immediate } => {
                Self::DeleteField { name, immediate }
            }
            DatabaseCallWire::ReplaceFields { fields } => Self::ReplaceFields {
                fields: raw_fields(fields)?,
            },
            DatabaseCallWire::Alarm => Self::Alarm,
            DatabaseCallWire::SetAlarm {
                timestamp_ms,
                immediate,
            } => Self::SetAlarm {
                timestamp_ms,
                immediate,
            },
            DatabaseCallWire::DeleteAlarm { immediate } => Self::DeleteAlarm { immediate },
            DatabaseCallWire::Commit => Self::Commit,
            DatabaseCallWire::Rollback => Self::Rollback,
        })
    }
}

fn raw_fields<E: Error>(
    fields: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Box<RawValue>>, E> {
    fields
        .into_iter()
        .map(|(name, value)| Ok((name, to_raw_value(&value).map_err(E::custom)?)))
        .collect()
}

impl DatabaseCall {
    pub(crate) fn immediate(&self) -> bool {
        matches!(
            self,
            Self::Exec {
                immediate: true,
                ..
            } | Self::PersistField {
                immediate: true,
                ..
            } | Self::DeleteField {
                immediate: true,
                ..
            } | Self::SetAlarm {
                immediate: true,
                ..
            } | Self::DeleteAlarm {
                immediate: true,
                ..
            }
        )
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatabaseResult {
    /// The rows went to the call's [`RowSink`].
    Rows,
    Fields {
        fields: BTreeMap<String, Box<RawValue>>,
    },
    Alarm {
        timestamp_ms: Option<i64>,
    },
    Done,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
