//! The stream-offset concern: a gapless, per-`(table, bucket)` monotonic offset
//! allocator. Offsets are what make the inline tier an ordered log; a later
//! slice stamps appended rows with `(bucket, offset)`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// The flavor of a declared stream table. `Log` = append-only (slice 1);
/// `Cdc` = PK table emitting +I/−U/+U/−D on mutation (slice 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Log,
    Cdc,
}

/// The replace-class merge policy for a CDC current-state base: which row wins
/// per identity when both fold sites (`consolidate_stream` compaction and
/// `build_merge_view` merge-on-read) collapse multiple physical rows. A `-D`
/// winner drops the identity under ALL engines. The durable changelog is
/// engine-agnostic — engines govern only current-state. See
/// `docs/superpowers/specs/2026-07-08-stream-merge-engines-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeEngine {
    /// Greatest `loom_offset` per identity wins (the default; byte-identical to
    /// the pre-engine fold).
    LastRow,
    /// Smallest `loom_offset` wins — "first write wins"; later events for that
    /// identity are ignored for current-state (they still land in the changelog).
    FirstRow,
    /// A user-declared domain `version` column sets precedence (highest version
    /// wins; `loom_offset` tie-breaks). Handles out-of-order arrival.
    Versioned,
}

impl MergeEngine {
    /// The persisted `stream.stream_table.merge_engine` wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MergeEngine::LastRow => "last_row",
            MergeEngine::FirstRow => "first_row",
            MergeEngine::Versioned => "versioned",
        }
    }
}

impl std::str::FromStr for MergeEngine {
    type Err = crate::error::ControlPlaneError;

    /// Parse the persisted token. Unknown tokens are a loud error (a corrupt
    /// row / a bad `?merge_engine=` query param), never a silent default.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "last_row" => Ok(MergeEngine::LastRow),
            "first_row" => Ok(MergeEngine::FirstRow),
            "versioned" => Ok(MergeEngine::Versioned),
            other => Err(crate::error::ControlPlaneError::Validation(format!(
                "unknown merge engine '{other}'"
            ))),
        }
    }
}

/// A declared stream table's metadata: its fixed bucket count, its kind, and —
/// for a CDC table — the identity column it buckets on (`hash(bucket_key) %
/// bucket_count`). `bucket_key` is `None` for a log table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMeta {
    pub bucket_count: i32,
    pub kind: StreamKind,
    pub bucket_key: Option<String>,
    /// The durable changelog table's `iceberg_mirror` `table_id` for a CDC table
    /// (slice 2b); `None` for a log table or a CDC table not yet given its
    /// changelog pointer. Soft pointer — no FK.
    pub changelog_table_id: Option<i64>,
    /// The replace-class merge engine governing this CDC table's current-state
    /// fold (compaction + merge-on-read). Default `LastRow`. Log tables carry
    /// `LastRow` too (unused — only CDC tables fold).
    pub merge_engine: MergeEngine,
}

/// One logical change event off a CDC table's changelog feed — the
/// transport-agnostic record (`road-stream-subscribe`): NDJSON-serialized on the
/// HTTP path today, protobuf-ready for a future gRPC duplex transport. `fields`
/// holds the governed USER columns only (masked columns arrive as the mask
/// marker); the `loom_*` framing surfaces only as this envelope's
/// `bucket`/`offset`/`change_kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub bucket: i32,
    pub offset: i64,
    /// `+I` | `-U` | `+U` | `-D` (the persisted `loom_change_kind` token).
    pub change_kind: String,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// One bounded page of the changelog feed scan: the events (ordered by
/// `(bucket, offset)`) plus the per-bucket resume positions AFTER them —
/// `next[bucket]` is the next offset a resumed scan should start from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeFeedPage {
    pub events: Vec<ChangeEvent>,
    pub next: std::collections::BTreeMap<i32, i64>,
}

#[async_trait]
pub trait BucketOffsets {
    /// Allocate a contiguous run of `count` offsets for `(table_id, bucket)` and
    /// return the FIRST offset in the run. Offsets start at 0; allocations are
    /// gapless and monotonic per bucket. When issued inside a transaction the
    /// offsets are assigned iff that transaction commits.
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64>;
    /// The high-water offset for `(table_id, bucket)` — the next offset that
    /// would be handed out (0 if none has been allocated yet).
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64>;
}

/// Direct registry writes for `stream.stream_table`. **Setup/test surface only.**
///
/// These bypass `reconcile_stream_mode` (the postgres adapter's declare seam,
/// which every production declare — HTTP `?mode=stream`/`?mode=cdc` and
/// `land`/`land_cdc` — goes through): no batch→stream conversion guard, no
/// bucket-count/kind/merge_engine/bucket_key reconciliation against a
/// concurrent winner, and — for `declare_cdc` — no changelog table
/// registration. They run standalone on the pool (autocommit), so a declare
/// made through them is committed independently of any write. Production code
/// declares by landing with a stream mode; these exist so tests and the
/// testkit contracts can put a table into a declared state without landing
/// data. (Verified: every `declare_stream`/`declare_cdc` caller in the tree is
/// a `tests/` file or the testkit contract suite — grep
/// `\.declare_stream(\|\.declare_cdc(` under `src/` to confirm.)
#[async_trait]
pub trait StreamTables {
    /// Declare table_id as a log table with bucket_count buckets. Idempotent: a
    /// redeclare is a no-op and the first declaration's bucket_count stands.
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()>;
    /// The bucket count if table_id is a declared log table, else None.
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>>;
    /// Declare table_id as a PK/CDC table with bucket_count buckets keyed on
    /// `bucket_key` (the identity column), folded by `merge_engine`. Idempotent,
    /// first-wins on all fields.
    async fn declare_cdc(
        &self,
        table_id: i64,
        bucket_count: i32,
        bucket_key: &str,
        merge_engine: MergeEngine,
    ) -> Result<()>;
    /// Full stream metadata for table_id if it is a declared stream table, else None.
    async fn stream_meta(&self, table_id: i64) -> Result<Option<StreamMeta>>;
    /// Point a CDC table's registry row at its durable changelog table's mirror
    /// `table_id`. Idempotent overwrite; only meaningful for a `kind='cdc'` row.
    async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()>;
}

/// The canonical watermark key for a standing query: the qualified name of the
/// table it materializes. Keyed by the OUTPUT (not the def name) so the
/// watermark survives a def rename exactly when the output — and therefore
/// resuming — is kept, and ad-hoc (nameless) runs need no special case.
#[must_use]
pub fn mv_key(output: &crate::TableRef) -> String {
    format!("{}.{}", output.schema, output.name)
}

/// One bucket's watermark CAS: advance `bucket` from `from` to `to`. `from` is
/// the LOWEST offset the delta actually carried for this bucket (`framing_bounds`
/// in the worker), which is at or above the committed watermark — equal to it when
/// the delta is gapless from it, and strictly above it when the offsets in between
/// no longer survive (a GC'd prefix, or a rounded-down bootstrap — `mv_bootstrap`
/// in the postgres adapter). The CAS therefore accepts `next_offset <= from` and
/// refuses anything above it: a watermark ahead of the delta means a concurrent run
/// already covered it. `from == 0` is the bootstrap case (may insert the row).
///
/// The CAS ALSO requires `next_offset < to`, so **monotonicity is enforced by the
/// mechanism, not owed by the caller**: `to > from` happens to hold for every advance
/// the worker's `framing_bounds` frames, but nothing between here and the backend
/// validates it, and without that conjunct a `{from: 900, to: 5}` advance would REWIND
/// a watermark at 100 and re-append the offsets in between forever.
///
/// Accepting `<=` cashes in an assumption worth naming: `mv_delta_scan` (engine-serving)
/// must read `loom_offset >= next_offset` **snapshot-consistently across both storage
/// tiers**, so that a delta's observed minimum sitting above the watermark PROVES the
/// offsets in between do not exist rather than merely being transiently invisible. A
/// flush end-caps the inline rows and publishes the Parquet file in one commit, so a row
/// is always live in exactly one tier. Under the old `=` predicate a transiently-invisible
/// row would have been a loud, permanent Conflict; under `<=` it would be a SILENT SKIP —
/// so a future non-atomic flush would break exactly-once here.
///
/// (Aside, for anyone diffing the two backends: memory's `current <= from` arm and
/// postgres's `where next_offset = 0` bootstrap predicate coincide for `from == 0` only
/// because offsets are assumed non-negative — an assumption nothing in the type states.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkAdvance {
    pub bucket: i32,
    pub from: i64,
    pub to: i64,
}

/// Per-standing-query offset watermarks: the next-unprocessed `loom_offset`
/// per `(mv, source_table_id, bucket)`. Advancing is a CAS — `Conflict` on a
/// stale `from` — and is issued inside the output-commit transaction by the
/// postgres adapter, so the watermark moves iff the output lands.
#[async_trait]
pub trait MvWatermarks {
    /// The recorded watermarks for `(mv, source_table_id)` — buckets with no
    /// row are absent (read as 0).
    async fn mv_watermarks(
        &self,
        mv: &str,
        source_table_id: i64,
    ) -> Result<std::collections::BTreeMap<i32, i64>>;
    /// CAS-advance each bucket; any stale `from` is `Conflict` (all-or-nothing
    /// is the postgres tx's job — this standalone surface applies in order and
    /// stops at the first conflict).
    async fn advance_mv_watermark(
        &self,
        mv: &str,
        source_table_id: i64,
        advances: &[WatermarkAdvance],
    ) -> Result<()>;
}
