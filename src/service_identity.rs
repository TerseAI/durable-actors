use anyhow::{Context, Result, ensure};
use google_cloud_auth::credentials::{
    external_account,
    idtoken::{self, IDTokenCredentials},
    subject_token,
};
use std::{fmt, sync::Arc, time::Duration};

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FederatedIdentity {
    provider: String,
    service_account: String,
}

impl FederatedIdentity {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        let config: Self =
            serde_json::from_str(value).context("parse control-plane service identity")?;
        let parts: Vec<_> = config.provider.split('/').collect();
        ensure!(
            parts.len() == 8
                && parts[0] == "projects"
                && !parts[1].is_empty()
                && parts[1].bytes().all(|b| b.is_ascii_digit())
                && parts[2] == "locations"
                && parts[3] == "global"
                && parts[4] == "workloadIdentityPools"
                && parts[6] == "providers"
                && [parts[5], parts[7]].iter().all(|name| !name.is_empty()
                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')),
            "invalid workload identity provider resource"
        );
        ensure!(
            config.service_account.ends_with(".iam.gserviceaccount.com")
                && config.service_account.split('@').count() == 2
                && config
                    .service_account
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._@".contains(&b)),
            "invalid service account identity"
        );
        Ok(config)
    }

    pub(crate) fn credentials(
        &self,
        origin: &str,
        subject_token: String,
    ) -> Result<IDTokenCredentials> {
        validate_audience(origin)?;
        ensure!(!subject_token.is_empty(), "Modal OIDC identity is missing");
        let source =
            external_account::ProgrammaticBuilder::new(Arc::new(ModalIdentity(subject_token)))
                .with_audience(format!("//iam.googleapis.com/{}", self.provider))
                .with_subject_token_type("urn:ietf:params:oauth:token-type:jwt")
                .with_token_url("https://sts.googleapis.com/v1/token")
                .with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
                .build()?;
        Ok(idtoken::impersonated::Builder::from_source_credentials(
            origin.trim_end_matches('/'),
            &self.service_account,
            source,
        )
        .with_include_email()
        .build()?)
    }
}

pub(crate) fn metadata_credentials(origin: &str) -> Result<IDTokenCredentials> {
    validate_audience(origin)?;
    Ok(idtoken::mds::Builder::new(origin.trim_end_matches('/'))
        .with_format(idtoken::mds::Format::Full)
        .build()?)
}

pub(crate) async fn authorization(credentials: &IDTokenCredentials) -> Result<String> {
    let token = tokio::time::timeout(Duration::from_secs(10), credentials.id_token())
        .await
        .context("service identity request timed out")?
        .context("obtain service identity")?;
    Ok(format!("Bearer {token}"))
}

fn validate_audience(origin: &str) -> Result<()> {
    crate::regional::proxy::validate_origin(origin)?;
    ensure!(
        origin.starts_with("https://"),
        "service identity requires HTTPS"
    );
    Ok(())
}

struct ModalIdentity(String);
impl fmt::Debug for ModalIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ModalIdentity([redacted])")
    }
}
impl subject_token::SubjectTokenProvider for ModalIdentity {
    type Error = google_cloud_auth::errors::CredentialsError;
    async fn subject_token(&self) -> std::result::Result<subject_token::SubjectToken, Self::Error> {
        Ok(subject_token::Builder::new(self.0.clone()).build())
    }
}

#[cfg(test)]
#[path = "../tests/unit/service_identity.rs"]
mod tests;
