use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceArchive {
    pub sha256: String,
    pub entrypoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<SourceObject>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObject {
    pub bucket: String,
    pub name: String,
    pub generation: String,
}

impl SourceArchive {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.sha256.len() == 64
                && self
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "source archive requires a SHA256 digest"
        );
        crate::artifacts::validate_path(&self.entrypoint)?;
        ensure!(
            [".ts", ".tsx", ".js", ".mjs", ".py"]
                .iter()
                .any(|suffix| self.entrypoint.ends_with(suffix)),
            "unsupported actor source entrypoint"
        );
        if let Some(object) = &self.object {
            crate::storage::validate_bucket(&object.bucket)?;
            ensure!(
                !object.name.is_empty() && object.name.len() <= 1024 && !object.name.contains('\0'),
                "invalid source object name"
            );
            ensure!(
                object
                    .generation
                    .parse::<i64>()
                    .is_ok_and(|generation| generation > 0),
                "source archive requires an immutable object generation"
            );
        }
        Ok(())
    }

    pub(crate) fn cache_key(&self, project: &str, image: &str) -> String {
        format!(
            "{}builds/{}/{}.json",
            crate::storage_paths::ROOT,
            cache_scope(project, image),
            digest(
                &serde_json::to_vec(&(&self.sha256, &self.entrypoint)).expect("source identity")
            )
        )
    }
}

pub(crate) fn dependency_prefix(project: &str, image: &str) -> String {
    format!(
        "{}dependency-cache/{}/",
        crate::storage_paths::ROOT,
        cache_scope(project, image)
    )
}

fn cache_scope(project: &str, image: &str) -> String {
    digest(&serde_json::to_vec(&(project, image)).expect("build scope"))
}

fn digest(bytes: &[u8]) -> String {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/source.rs"]
mod tests;
