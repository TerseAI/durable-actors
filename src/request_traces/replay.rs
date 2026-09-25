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
