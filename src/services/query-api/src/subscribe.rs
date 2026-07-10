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

/// `GET /objects/{type}/changes` query parameters.
#[derive(Debug, serde::Deserialize)]
pub struct ChangesQuery {
    pub cursor: Option<String>,
    pub fields: Option<String>,
    pub max_events: Option<u64>,
}

/// Feed pacing: max events fetched per scan pass (memory bound / natural
/// backpressure) and the poll-fallback interval bounding a missed notify.
pub const FEED_BATCH_LIMIT: usize = 256;
pub const FEED_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// The bound per-connect state driving [`ndjson_feed_stream`]: the engine, the
/// target changelog table, the boot positions, the resolved governance policy,
/// the validated `?fields=` projection, and the `?max_events=` budget. Built by
/// the `get_changes` handler after its governance/cursor/probe prologue.
pub(crate) struct FeedState {
    pub(crate) serving: std::sync::Arc<dyn crate::serving::ServingEngine>,
    pub(crate) table: control_plane_core::TableRef,
    pub(crate) type_name: String,
    pub(crate) positions: BTreeMap<i32, i64>,
    pub(crate) policy: crate::serving::ChangeFeedPolicy,
    /// `None` = all governed columns; `Some` = the validated `?fields=` subset.
    pub(crate) fields: Option<Vec<String>>,
    /// Remaining `?max_events=` budget; `None` = endless tail.
    pub(crate) remaining: Option<u64>,
}

/// The chunked NDJSON body: loop { scan a page; emit its lines; else wait for
/// the notify/poll wakeup }. Ends when the max_events budget is exhausted or an
/// engine fault occurs (logged server-side; a stream in flight cannot change
/// its status code). Client disconnect drops the stream (axum drops the body),
/// cancelling any in-flight scan/wait — no orphaned long-poll.
///
/// The per-event `cursor`: `pre` starts as the pre-scan `positions` (the exact
/// map the page was scanned from) and is folded forward one event at a time —
/// `pre.insert(ev.bucket, ev.offset + 1)` — so each emitted line's cursor is the
/// EXACT resume position immediately after that event (not the whole page's
/// `next`). After the last event in a page, `pre == page.next`.
pub(crate) fn ndjson_feed_stream(
    st: FeedState,
) -> impl futures::Stream<Item = Result<axum::body::Bytes, std::convert::Infallible>> {
    futures::stream::unfold(Some(st), |state| async move {
        let mut st = state?;
        loop {
            let batch = match st.remaining {
                Some(0) => return None,
                Some(r) => FEED_BATCH_LIMIT.min(usize::try_from(r).unwrap_or(FEED_BATCH_LIMIT)),
                None => FEED_BATCH_LIMIT,
            };
            match st
                .serving
                .changelog_feed(&st.table, &st.positions, batch, &st.policy)
                .await
            {
                Ok(page) if page.events.is_empty() => {
                    if let Err(e) = st
                        .serving
                        .await_changelog(&st.table, FEED_POLL_INTERVAL)
                        .await
                    {
                        tracing::error!(error = %e, "changelog wait fault; closing stream");
                        return None;
                    }
                }
                Ok(page) => {
                    // The concrete per-event cursor fold: `pre` walks forward from the
                    // pre-scan positions one event at a time, so `pre` at any point is
                    // the exact resume position after the events emitted so far.
                    let mut pre = st.positions.clone();
                    st.positions = page.next.clone();
                    let mut buf = String::new();
                    for ev in &page.events {
                        pre.insert(ev.bucket, ev.offset + 1);
                        // fields projection (validated at connect).
                        let fields: serde_json::Map<String, serde_json::Value> = match &st.fields {
                            None => ev.fields.clone(),
                            Some(want) => ev
                                .fields
                                .iter()
                                .filter(|(k, _)| want.iter().any(|w| w == *k))
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        };
                        let cursor = SubscribeCursor {
                            t: st.type_name.clone(),
                            b: pre.clone(),
                        };
                        let line = serde_json::json!({
                            "bucket": ev.bucket,
                            "offset": ev.offset,
                            "change_kind": ev.change_kind,
                            "fields": fields,
                            "cursor": cursor.to_opaque(),
                        });
                        // json! of Values never fails to serialize.
                        if let Ok(s) = serde_json::to_string(&line) {
                            buf.push_str(&s);
                            buf.push('\n');
                        }
                        if let Some(r) = st.remaining.as_mut() {
                            *r = r.saturating_sub(1);
                        }
                    }
                    let done = matches!(st.remaining, Some(0));
                    let next = if done { None } else { Some(st) };
                    return Some((Ok(axum::body::Bytes::from(buf)), next));
                }
                Err(e) => {
                    tracing::error!(error = %e, "changelog feed fault; closing stream");
                    return None;
                }
            }
        }
    })
}
