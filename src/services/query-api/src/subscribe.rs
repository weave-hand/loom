//! The subscribe feed's HTTP framing: the client-held opaque resume cursor and
//! (Task 5) the NDJSON stream assembly. The cursor is UNSIGNED by design — a
//! tampered cursor only corrupts the consumer's own resume position; it carries
//! no authority (every connect re-runs `resolve_governed`).

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// The client-held resume cursor: the type it was minted for (`t`, rejected on
/// mismatch so a cursor cannot be replayed across types) and the per-bucket
/// next-offset map (`b[bucket]` = first offset NOT yet consumed).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubscribeCursor {
    pub t: String,
    pub b: BTreeMap<i32, i64>,
}

impl SubscribeCursor {
    /// Unsigned base64url (no padding) of the serde_json bytes.
    #[must_use]
    pub fn to_opaque(&self) -> String {
        #[expect(
            clippy::expect_used,
            reason = "serde_json of an owned serializable type is infallible; matches GovernedStatementQuery::encode"
        )]
        let bytes = serde_json::to_vec(self).expect("SubscribeCursor is always serializable");
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Decode an opaque cursor. The error string is client-safe (a 400 body).
    pub fn from_opaque(s: &str) -> Result<Self, String> {
        let bytes = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_e| "malformed cursor (not base64url)".to_string())?;
        serde_json::from_slice(&bytes).map_err(|_e| "malformed cursor".to_string())
    }
}

/// The parsed `?cursor=` parameter. `earliest` boots from offset 0 across all
/// buckets; `latest` boots from the current high-water (join-the-tail).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorSpec {
    Earliest,
    Latest,
    Resume(SubscribeCursor),
}

/// Parse `?cursor=`: absent/`earliest`/`latest` sentinels, else the opaque form.
pub fn parse_cursor(raw: Option<&str>) -> Result<CursorSpec, String> {
    match raw {
        None | Some("earliest") => Ok(CursorSpec::Earliest),
        Some("latest") => Ok(CursorSpec::Latest),
        Some(op) => SubscribeCursor::from_opaque(op).map(CursorSpec::Resume),
    }
}
