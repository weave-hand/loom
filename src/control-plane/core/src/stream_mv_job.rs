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
}
