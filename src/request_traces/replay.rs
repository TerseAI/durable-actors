use anyhow::{Result, ensure};

#[derive(Clone)]
pub(crate) struct ReplayQuery {
    pub cursor: Option<String>,
    pub limit: usize,
}
impl Default for ReplayQuery {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: super::TRACE_CAPACITY,
        }
    }
}
impl ReplayQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (1..=super::TRACE_CAPACITY).contains(&self.limit),
            "invalid replay limit"
        );
        ensure!(
            self.cursor.as_ref().is_none_or(|c| c.len() <= 4096),
            "invalid replay cursor"
        );
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct InvalidTraceCursor;

impl std::fmt::Display for InvalidTraceCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Invalid or incompatible request history cursor")
    }
}
impl std::error::Error for InvalidTraceCursor {}
