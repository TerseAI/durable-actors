use std::str::FromStr;

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Region {
    #[serde(rename = "north-america-west")]
    West,
    #[serde(rename = "north-america-central")]
    Central,
    #[serde(rename = "north-america-east")]
    East,
}

impl Region {
    pub const ALL: [Self; 3] = [Self::West, Self::Central, Self::East];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::West => "north-america-west",
            Self::Central => "north-america-central",
            Self::East => "north-america-east",
        }
    }

    pub fn attest(self, cloud: &str, region: &str) -> Result<()> {
        ensure!(
            matches!(cloud, "gcp" | "GCP" | "CLOUD_PROVIDER_GCP"),
            "Modal placement must use GCP, received {cloud:?}"
        );
        ensure!(
            region.parse::<Self>()? == self,
            "Modal placement does not match {}",
            self.as_str()
        );
        Ok(())
    }
}

impl FromStr for Region {
    type Err = anyhow::Error;

    fn from_str(region: &str) -> Result<Self> {
        match region {
            "us-west" | "north-america-west" | "us-west1" | "us-west2" | "us-west3"
            | "us-west4" => Ok(Self::West),
            "us-central" | "north-america-central" | "us-central1" => Ok(Self::Central),
            "us-east" | "north-america-east" | "us-east1" | "us-east4" | "us-east5" => {
                Ok(Self::East)
            }
            _ => bail!("unsupported hosted region {region:?}"),
        }
    }
}

pub fn attest_modal_environment() -> Result<()> {
    let Ok(requested) = std::env::var("DURABLE_ACTORS_GCP_REGION") else {
        return Ok(());
    };
    let cloud = std::env::var("MODAL_CLOUD_PROVIDER")?;
    let actual = std::env::var("MODAL_REGION")?;
    requested.parse::<Region>()?.attest(&cloud, &actual)
}
