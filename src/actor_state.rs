use serde::{Deserialize, Serialize};
use std::fmt;

use anyhow::{Result, ensure};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorStorageKey(String);

impl ActorStorageKey {
    pub fn new<S>(id: S) -> Self
    where
        S: Into<String>,
    {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.0.is_empty(), "actor storage key must not be empty");
        ensure!(
            self.0.len() <= 255,
            "actor storage key must be at most 255 bytes"
        );
        ensure!(
            self.0 != "." && self.0 != "..",
            "actor storage key must not be a relative path component"
        );
        ensure!(
            !self
                .0
                .chars()
                .any(|character| character == '/' || character == '\\' || character.is_control()),
            "actor storage key must not contain path separators or control characters"
        );

        Ok(())
    }
}

impl fmt::Display for ActorStorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
#[path = "../tests/unit/actor_state.rs"]
mod tests;
