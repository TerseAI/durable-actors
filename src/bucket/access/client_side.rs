//! Client-side Credential Access Boundaries: one STS exchange yields an intermediary token and a
//! session key, and each scoped token is then minted locally by encrypting its boundary with that
//! key. Google enforces the boundary; anyone holding the intermediary and key could mint any
//! boundary up to this service account's own access, so both stay in this process's memory.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use aws_lc_rs::{
    aead::{AES_128_GCM, AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use prost::Message;

use super::{LIST_PREFIX_ATTRIBUTE, Rule, StorageToken, StorageTokenSource, Term};
use crate::clock::{Clock, SystemClock};

/// An intermediary must have this long left to mint from; hosts refresh 60 s before expiry.
const MINIMUM_LIFETIME: Duration = Duration::from_secs(120);
/// Below this, launches keep minting from the current intermediary while one is fetched behind them.
const REFRESH_MARGIN: Duration = Duration::from_secs(40 * 60);
/// Background refreshes start at most this often. The intermediary expires with the source token,
/// and the metadata server may keep handing out the same near-expiry source token for a while.
const BACKGROUND_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

pub(super) struct IntermediaryToken {
    pub access_token: String,
    pub expires_at_ms: u64,
    pub session_key: String,
}

#[async_trait]
pub(super) trait IntermediaryExchange: Send + Sync {
    async fn exchange(&self) -> Result<IntermediaryToken>;
}

pub(super) struct ClientSideTokens(Arc<Inner>);

struct Inner {
    exchange: Box<dyn IntermediaryExchange>,
    current: Mutex<Option<Arc<Intermediary>>>,
    refresh: tokio::sync::Mutex<()>,
    last_background_refresh: Mutex<Option<Instant>>,
}

impl ClientSideTokens {
    pub fn new(exchange: Box<dyn IntermediaryExchange>) -> Self {
        Self(Arc::new(Inner {
            exchange,
            current: Mutex::new(None),
            refresh: tokio::sync::Mutex::new(()),
            last_background_refresh: Mutex::new(None),
        }))
    }

    async fn intermediary(&self) -> Result<Arc<Intermediary>> {
        let now = SystemClock.now_ms()?;
        if let Some(current) = self.0.usable(now, MINIMUM_LIFETIME) {
            if current.remaining(now) < REFRESH_MARGIN && self.0.claim_background_refresh() {
                let inner = self.0.clone();
                tokio::spawn(async move {
                    if let Err(error) = inner.refresh(REFRESH_MARGIN).await {
                        tracing::warn!(%error, "intermediary GCS credential refresh failed");
                    }
                });
            }
            return Ok(current);
        }
        self.0.refresh(MINIMUM_LIFETIME).await
    }
}

#[async_trait]
impl StorageTokenSource for ClientSideTokens {
    async fn issue(&self, rules: &[Rule]) -> Result<StorageToken> {
        let restrictions = restrictions(rules)?;
        self.intermediary().await?.mint(&restrictions)
    }
}

impl Inner {
    fn usable(&self, now_ms: u64, lifetime: Duration) -> Option<Arc<Intermediary>> {
        self.current
            .lock()
            .unwrap()
            .clone()
            .filter(|current| current.remaining(now_ms) >= lifetime)
    }

    fn claim_background_refresh(&self) -> bool {
        let mut last = self.last_background_refresh.lock().unwrap();
        if last.is_some_and(|at| at.elapsed() < BACKGROUND_REFRESH_INTERVAL) {
            return false;
        }
        *last = Some(Instant::now());
        true
    }

    /// Single-flight: callers that queue behind a refresh reuse its result.
    async fn refresh(&self, needed: Duration) -> Result<Arc<Intermediary>> {
        let _single_flight = self.refresh.lock().await;
        if let Some(current) = self.usable(SystemClock.now_ms()?, needed) {
            return Ok(current);
        }
        let fresh = Arc::new(Intermediary::new(self.exchange.exchange().await?)?);
        let now = SystemClock.now_ms()?;
        let mut current = self.current.lock().unwrap();
        let replaced = current
            .as_ref()
            .is_none_or(|existing| fresh.expires_at_ms > existing.expires_at_ms);
        if replaced {
            *current = Some(fresh.clone());
        }
        tracing::info!(
            event = "storage_intermediary_refresh",
            expires_in_s = fresh.remaining(now).as_secs(),
            replaced,
        );
        let best = current.clone().context("intermediary credential missing")?;
        ensure!(
            best.remaining(now) >= MINIMUM_LIFETIME,
            "intermediary GCS credential lifetime is too short"
        );
        Ok(best)
    }
}

struct Intermediary {
    access_token: String,
    expires_at_ms: u64,
    key: LessSafeKey,
    output_prefix: Vec<u8>,
}

impl std::fmt::Debug for Intermediary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Intermediary")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

impl Intermediary {
    fn new(token: IntermediaryToken) -> Result<Self> {
        let (key, output_prefix) = session_key(&token.session_key)?;
        Ok(Self {
            access_token: token.access_token,
            expires_at_ms: token.expires_at_ms,
            key,
            output_prefix,
        })
    }

    fn remaining(&self, now_ms: u64) -> Duration {
        Duration::from_millis(self.expires_at_ms.saturating_sub(now_ms))
    }

    /// Tink AEAD ciphertext (prefix || nonce || ciphertext || tag) with no associated data,
    /// appended to the intermediary token.
    fn mint(&self, restrictions: &[u8]) -> Result<StorageToken> {
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow!("could not generate a credential nonce"))?;
        let mut sealed = restrictions.to_vec();
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut sealed,
            )
            .map_err(|_| anyhow!("could not encrypt the credential boundary"))?;
        let mut encrypted = Vec::with_capacity(self.output_prefix.len() + NONCE_LEN + sealed.len());
        encrypted.extend_from_slice(&self.output_prefix);
        encrypted.extend_from_slice(&nonce);
        encrypted.extend_from_slice(&sealed);
        Ok(StorageToken {
            access_token: format!(
                "{}.{}",
                self.access_token,
                URL_SAFE_NO_PAD.encode(encrypted)
            ),
            expires_at_ms: self.expires_at_ms,
        })
    }
}

const AES_GCM_KEY_TYPE: &str = "type.googleapis.com/google.crypto.tink.AesGcmKey";
const KEY_STATUS_ENABLED: i32 = 1;

/// Parses the session key, a serialized Tink keyset, into its primary AES-GCM key and the
/// output prefix Tink puts before each ciphertext.
fn session_key(encoded: &str) -> Result<(LessSafeKey, Vec<u8>)> {
    let keyset = Keyset::decode(
        STANDARD
            .decode(encoded)
            .context("session key is not base64")?
            .as_slice(),
    )
    .context("session key is not a Tink keyset")?;
    let mut primary = keyset
        .key
        .iter()
        .filter(|key| key.key_id == keyset.primary_key_id);
    let (Some(key), None) = (primary.next(), primary.next()) else {
        bail!("session key has no single primary key");
    };
    ensure!(
        key.status == KEY_STATUS_ENABLED,
        "session key is not enabled"
    );
    let data = key
        .key_data
        .as_ref()
        .context("session key has no key data")?;
    ensure!(
        data.type_url == AES_GCM_KEY_TYPE,
        "session key type {} is not supported",
        data.type_url
    );
    let aes = AesGcmKey::decode(data.value.as_slice()).context("session key is malformed")?;
    let algorithm = match aes.key_value.len() {
        16 => &AES_128_GCM,
        32 => &AES_256_GCM,
        length => bail!("session key has an unsupported length of {length} bytes"),
    };
    let key_id = key.key_id.to_be_bytes();
    let output_prefix = match key.output_prefix_type {
        1 => [&[1u8][..], &key_id].concat(),     // TINK
        2 | 4 => [&[0u8][..], &key_id].concat(), // LEGACY, CRUNCHY
        3 => Vec::new(),                         // RAW
        other => bail!("session key output prefix {other} is not supported"),
    };
    let key = UnboundKey::new(algorithm, &aes.key_value)
        .map_err(|_| anyhow!("session key is not a valid AES key"))?;
    Ok((LessSafeKey::new(key), output_prefix))
}

/// Serializes the boundary as `ClientSideAccessBoundary`, with each condition compiled to a
/// `cel.expr.Expr` tree.
fn restrictions(rules: &[Rule]) -> Result<Vec<u8>> {
    let access_boundary_rules = rules
        .iter()
        .map(|rule| {
            Ok(ClientSideAccessBoundaryRule {
                available_resource: rule.resource(),
                available_permissions: rule.permissions(),
                compiled_availability_condition: Some(condition(&rule.terms)?),
            })
        })
        .collect::<Result<_>>()?;
    Ok(ClientSideAccessBoundary {
        access_boundary_rules,
    }
    .encode_to_vec())
}

/// A rule without a condition grants its whole bucket, so an empty one is refused.
fn condition(terms: &[Term]) -> Result<Expr> {
    let mut ids = ExprIds(0);
    let mut expressions = terms
        .iter()
        .map(|term| ids.term(term))
        .collect::<Vec<_>>()
        .into_iter();
    let first = expressions
        .next()
        .context("credential boundary rule has no condition")?;
    Ok(expressions.fold(first, |left, right| {
        Expr::call(ids.next(), None, "_||_", vec![left, right])
    }))
}

/// Expression ids only need to be unique within one condition.
struct ExprIds(i64);

impl ExprIds {
    fn next(&mut self) -> i64 {
        self.0 += 1;
        self.0
    }

    fn resource_name(&mut self) -> Expr {
        let resource = Expr::ident(self.next(), "resource");
        Expr::select(self.next(), resource, "name")
    }

    fn term(&mut self, term: &Term) -> Expr {
        match term {
            Term::Equals(name) => {
                let operand = self.resource_name();
                let value = Expr::string(self.next(), name);
                Expr::call(self.next(), None, "_==_", vec![operand, value])
            }
            Term::StartsWith(prefix) => {
                let target = self.resource_name();
                let value = Expr::string(self.next(), prefix);
                Expr::call(self.next(), Some(target), "startsWith", vec![value])
            }
            Term::ListPrefix(prefix) => {
                let api = Expr::ident(self.next(), "api");
                let attribute = Expr::string(self.next(), LIST_PREFIX_ATTRIBUTE);
                let default = Expr::string(self.next(), "");
                let lookup = Expr::call(
                    self.next(),
                    Some(api),
                    "getAttribute",
                    vec![attribute, default],
                );
                let value = Expr::string(self.next(), prefix);
                Expr::call(self.next(), Some(lookup), "startsWith", vec![value])
            }
        }
    }
}

// Wire-compatible subsets of google.crypto.tink.Keyset / AesGcmKey,
// cloud.identity.unifiedauth.proto.ClientSideAccessBoundary and cel.expr.Expr.

#[derive(Clone, PartialEq, Message)]
struct Keyset {
    #[prost(uint32, tag = "1")]
    primary_key_id: u32,
    #[prost(message, repeated, tag = "2")]
    key: Vec<KeysetKey>,
}

#[derive(Clone, PartialEq, Message)]
struct KeysetKey {
    #[prost(message, optional, tag = "1")]
    key_data: Option<KeyData>,
    #[prost(int32, tag = "2")]
    status: i32,
    #[prost(uint32, tag = "3")]
    key_id: u32,
    #[prost(int32, tag = "4")]
    output_prefix_type: i32,
}

#[derive(Clone, PartialEq, Message)]
struct KeyData {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
    #[prost(int32, tag = "3")]
    key_material_type: i32,
}

#[derive(Clone, PartialEq, Message)]
struct AesGcmKey {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "3")]
    key_value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ClientSideAccessBoundary {
    #[prost(message, repeated, tag = "1")]
    access_boundary_rules: Vec<ClientSideAccessBoundaryRule>,
}

#[derive(Clone, PartialEq, Message)]
struct ClientSideAccessBoundaryRule {
    #[prost(string, tag = "1")]
    available_resource: String,
    #[prost(string, repeated, tag = "2")]
    available_permissions: Vec<String>,
    #[prost(message, optional, tag = "4")]
    compiled_availability_condition: Option<Expr>,
}

#[derive(Clone, PartialEq, Message)]
struct Expr {
    #[prost(int64, tag = "2")]
    id: i64,
    #[prost(message, optional, tag = "3")]
    const_expr: Option<Constant>,
    #[prost(message, optional, tag = "4")]
    ident_expr: Option<Ident>,
    #[prost(message, optional, tag = "5")]
    select_expr: Option<Select>,
    #[prost(message, optional, tag = "6")]
    call_expr: Option<Call>,
}

#[derive(Clone, PartialEq, Message)]
struct Constant {
    #[prost(string, optional, tag = "6")]
    string_value: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct Ident {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct Select {
    #[prost(message, optional, boxed, tag = "1")]
    operand: Option<Box<Expr>>,
    #[prost(string, tag = "2")]
    field: String,
    #[prost(bool, tag = "3")]
    test_only: bool,
}

#[derive(Clone, PartialEq, Message)]
struct Call {
    #[prost(message, optional, boxed, tag = "1")]
    target: Option<Box<Expr>>,
    #[prost(string, tag = "2")]
    function: String,
    #[prost(message, repeated, tag = "3")]
    args: Vec<Expr>,
}

impl Expr {
    fn ident(id: i64, name: &str) -> Self {
        Self {
            id,
            ident_expr: Some(Ident { name: name.into() }),
            ..Default::default()
        }
    }

    fn string(id: i64, value: &str) -> Self {
        Self {
            id,
            const_expr: Some(Constant {
                string_value: Some(value.into()),
            }),
            ..Default::default()
        }
    }

    fn select(id: i64, operand: Expr, field: &str) -> Self {
        Self {
            id,
            select_expr: Some(Select {
                operand: Some(Box::new(operand)),
                field: field.into(),
                test_only: false,
            }),
            ..Default::default()
        }
    }

    fn call(id: i64, target: Option<Expr>, function: &str, args: Vec<Expr>) -> Self {
        Self {
            id,
            call_expr: Some(Call {
                target: target.map(Box::new),
                function: function.into(),
                args,
            }),
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/client_side.rs"]
mod tests;
