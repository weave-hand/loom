//! The micro-batch MV job contract, shared by producers (the transforms concern's
//! `to_job`, data triggers) and the consumer (the zero-pool worker). Mirrors
//! `transform_job.rs`.

use crate::TableRef;

/// The queue `kind` for a micro-batch materialized-view job.
pub const STREAM_MV_JOB_KIND: &str = "stream_mv";

/// Payload of a `"stream_mv"` job: one micro-batch of a standing query — run
/// `sql` over `source`'s delta since the committed watermark and commit the
/// result to `output` (a declared log stream table with `buckets` buckets).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct StreamMvJob {
    pub source: TableRef,
    pub output: TableRef,
    pub buckets: i32,
    pub sql: String,
    /// The `TransformRun` this job executes (and the lineage `run_id`), when
    /// tracked. Absent on direct enqueues.
    #[serde(default)]
    pub run_id: Option<uuid::Uuid>,
    /// Slice-5 join MVs: the state-side table. `None` => a plain slice-4 MV.
    #[serde(default)]
    pub enrich: Option<TableRef>,
    /// Slice-5 lookup-join key contract. `None` with `enrich: Some` => state-join.
    #[serde(default)]
    pub on: Option<LookupOn>,
}

/// The lookup-join key contract: the delta column whose distinct values probe
/// the enrich table's `enrich_col`. MUST name the SQL's equijoin columns —
/// v1 does not parse the SQL to verify (a mismatch silently drops join
/// partners; `on: None` is the always-correct full-state default). See the
/// slice-5 spec's correctness contract.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LookupOn {
    pub source_col: String,
    pub enrich_col: String,
}

/// Above this many distinct lookup keys the worker falls back to the
/// full-state fetch (a superset — always correct); keeps the enrich ticket
/// bounded.
pub const MAX_LOOKUP_KEYS: usize = 10_000;
