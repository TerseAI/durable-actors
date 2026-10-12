use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;

pub(crate) const AUDIENCE: &str = "durable-actors-sandbox-assign";

pub(crate) struct AssignmentVerifier {
    keys: HashMap<String, DecodingKey>,
    validation: Validation,
    sandbox_uid: Box<dyn Fn() -> Result<String> + Send + Sync>,
}

impl AssignmentVerifier {
    pub fn new(
        keys: &str,
        issuer: &str,
        sandbox_uid: impl Fn() -> Result<String> + Send + Sync + 'static,
    ) -> Result<Self> {
        ensure!(!issuer.is_empty(), "assignment issuer missing");
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&[AUDIENCE]);
        validation.set_issuer(&[issuer]);
        validation.set_required_spec_claims(&["exp", "nbf", "sub", "iss", "aud"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        Ok(Self {
            keys: super::auth::decode_public_keys(keys)?,
            validation,
            sandbox_uid: Box::new(sandbox_uid),
        })
    }

    pub fn verify(&self, token: &str) -> Result<()> {
        let header = decode_header(token)?;
        let key = self
            .keys
            .get(header.kid.as_deref().context("assignment key ID missing")?)
            .context("unknown assignment key")?;
        let uid = (self.sandbox_uid)()?;
        ensure!(!uid.is_empty(), "assignment sandbox UID missing");
        let mut validation = self.validation.clone();
        validation.sub = Some(uid);
        let claims = decode::<Claims>(token, key, &validation)?.claims;
        ensure!(
            claims.scope == "sandbox:assign"
                && claims.exp > claims.iat
                && claims.exp - claims.iat <= 120,
            "invalid assignment capability"
        );
        Ok(())
    }
}

#[derive(Deserialize)]
struct Claims {
    scope: String,
    iat: i64,
    exp: i64,
}
