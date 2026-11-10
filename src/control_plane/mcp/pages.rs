use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct Page<T> {
    pub records: Vec<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Matching records across all pages, when the whole list is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
}

/// Pages a bounded list the API returns whole. The cursor keeps the first page's query window,
/// so later pages slice the same list rather than one that moved on.
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Cursor {
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_ms: Option<u64>,
}

impl Cursor {
    pub(super) fn decode(token: Option<&str>) -> Option<Self> {
        let Some(token) = token else {
            return Some(Self::default());
        };
        let bytes = URL_SAFE_NO_PAD.decode(token).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).expect("cursor serializes"))
    }
}

pub(super) fn slice<T>(items: Vec<T>, cursor: Cursor, limit: usize) -> Page<T> {
    let total = items.len();
    let records: Vec<T> = items.into_iter().skip(cursor.offset).take(limit).collect();
    let end = cursor.offset + records.len();
    let next_cursor = (end < total).then(|| {
        Cursor {
            offset: end,
            ..cursor
        }
        .encode()
    });
    Page {
        records,
        next_cursor,
        total: Some(total),
    }
}
