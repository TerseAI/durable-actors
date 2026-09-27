use anyhow::{Context, Result, ensure};
use aws_lc_rs::digest::{SHA256, digest};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actor::{ActorExecutionResult, ActorInvocationFailure};

pub(crate) const RETENTION_MS: i64 = 300_000;
const MAX_RECEIPTS: usize = 256;
const MAX_RESULT_BYTES: usize = 65_536;
const MAX_RECEIPT_BYTES: usize = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InvocationIdentity {
    key: String,
    fingerprint: String,
    created_at_ms: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InvocationReceipts {
    retired_through_ms: i64,
    entries: Vec<Receipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    identity: InvocationIdentity,
    outcome: ReceiptOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum ReceiptOutcome {
    Pending,
    Completed(Value),
    Failed(ActorInvocationFailure),
    Unavailable,
}

impl InvocationIdentity {
    pub(crate) fn new(key: &str, subject: &str, method: &str, args: &[Value]) -> Result<Self> {
        ensure!(key.len() <= 255, "idempotency key is too large");
        let (time, nonce) = key
            .split_once('.')
            .context("idempotency key requires a timestamp and nonce")?;
        let created_at_ms = time
            .parse::<i64>()
            .context("invalid idempotency timestamp")?;
        ensure!(
            created_at_ms >= 0 && time == created_at_ms.to_string(),
            "invalid idempotency timestamp"
        );
        ensure!(
            !nonce.is_empty()
                && nonce
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
            "invalid idempotency nonce"
        );
        Ok(Self {
            key: hash(&serde_json::to_vec(&(subject, key))?),
            fingerprint: hash(&serde_json::to_vec(&(method, args))?),
            created_at_ms,
        })
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn same_input(&self, other: &Self) -> bool {
        self == other
    }

    pub(crate) fn live(&self, now: i64) -> bool {
        self.created_at_ms <= now.saturating_add(5000)
            && self.created_at_ms.saturating_add(RETENTION_MS) > now
    }
}

impl InvocationReceipts {
    pub(crate) fn is_empty(&self) -> bool {
        self.retired_through_ms == 0 && self.entries.is_empty()
    }

    pub(crate) fn admit(
        &mut self,
        identity: &InvocationIdentity,
        now: i64,
    ) -> Option<ActorExecutionResult> {
        if !identity.live(now) {
            return Some(failed(
                "idempotency_expired",
                "idempotency key is outside its retry window",
            ));
        }
        if let Some(receipt) = self.entries.iter().find(|r| r.identity.key == identity.key) {
            return Some(if receipt.identity.same_input(identity) {
                receipt.outcome.replay()
            } else {
                failed(
                    "idempotency_conflict",
                    "idempotency key was already used with different input",
                )
            });
        }
        if identity.created_at_ms <= self.retired_through_ms {
            // A bounded ledger must fence forgotten keys, not silently execute them again.
            return Some(failed(
                "idempotency_expired",
                "idempotency receipt has been retired",
            ));
        }
        // Other reentrant calls can commit this invocation's partial state before it finishes.
        self.entries.push(Receipt {
            identity: identity.clone(),
            outcome: ReceiptOutcome::Pending,
        });
        self.trim(now);
        None
    }

    pub(crate) fn complete(
        &mut self,
        identity: &InvocationIdentity,
        outcome: ReceiptOutcome,
        now: i64,
    ) {
        let outcome = if encoded_size(&outcome) <= MAX_RESULT_BYTES {
            outcome
        } else {
            ReceiptOutcome::Unavailable
        };
        self.entries
            .retain(|entry| entry.identity.key != identity.key);
        self.entries.push(Receipt {
            identity: identity.clone(),
            outcome,
        });
        self.trim(now);
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.entries.len() <= MAX_RECEIPTS && encoded_size(self) <= MAX_RECEIPT_BYTES,
            "invalid invocation receipt size"
        );
        let mut keys = std::collections::HashSet::new();
        for entry in &self.entries {
            ensure!(
                entry.identity.key.len() == 43
                    && entry.identity.fingerprint.len() == 43
                    && entry.identity.created_at_ms >= 0
                    && encoded_size(&entry.outcome) <= MAX_RESULT_BYTES
                    && keys.insert(&entry.identity.key),
                "invalid invocation receipt"
            );
        }
        Ok(())
    }

    fn trim(&mut self, now: i64) {
        self.entries
            .sort_by_key(|entry| entry.identity.created_at_ms);
        while self
            .entries
            .first()
            .is_some_and(|entry| entry.identity.created_at_ms.saturating_add(RETENTION_MS) <= now)
            || self.entries.len() > MAX_RECEIPTS
            || encoded_size(self) > MAX_RECEIPT_BYTES
        {
            let retired = self.entries.remove(0);
            self.retired_through_ms = self.retired_through_ms.max(retired.identity.created_at_ms);
        }
    }
}

impl ReceiptOutcome {
    fn replay(&self) -> ActorExecutionResult {
        match self {
            Self::Completed(result) => completed(result.clone()),
            Self::Failed(failure) => ActorExecutionResult::Failed {
                failure: failure.clone(),
            },
            Self::Pending => failed(
                "outcome_unknown",
                "recovered invocation may have partially committed; it will not be reexecuted",
            ),
            Self::Unavailable => failed(
                "idempotency_result_unavailable",
                "invocation completed but its result exceeds the replay limit",
            ),
        }
    }
}

fn hash(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, bytes).as_ref())
}

fn encoded_size(value: &impl Serialize) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

fn completed(result: Value) -> ActorExecutionResult {
    ActorExecutionResult::Completed {
        result,
        effects: vec![],
    }
}

pub(crate) fn failed(code: &str, message: &str) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}

#[cfg(test)]
#[path = "../tests/unit/idempotency.rs"]
mod tests;
