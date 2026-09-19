use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqlQuery {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Value>,
}

impl SqlQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.sql.trim().is_empty() && self.sql.len() <= 16_384,
            "SQL must be between 1 and 16384 bytes"
        );
        ensure!(self.params.len() <= 100, "too many SQL parameters");
        for value in &self.params {
            ensure!(
                value.is_null()
                    || value.is_boolean()
                    || value.is_number()
                    || value.as_str().is_some_and(|s| s.len() <= 16_384),
                "parameters must be JSON scalars of at most 16384 bytes"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SqlResult {
    pub rows: Vec<Value>,
    pub truncated: bool,
}

#[derive(Debug)]
pub(crate) struct SqlQueryError(pub String);
impl std::fmt::Display for SqlQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SqlQueryError {}
