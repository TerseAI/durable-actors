use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ActorImage {
    pub reference: String,
    pub entrypoint: String,
}

impl ActorImage {
    pub fn validate(&self) -> Result<()> {
        crate::sandbox::validate_image(&self.reference)?;
        ensure!(
            matches!(self.entrypoint.as_str(), "actors.mjs" | "actors.pyz"),
            "image entrypoint must be actors.mjs or actors.pyz"
        );
        Ok(())
    }
}
