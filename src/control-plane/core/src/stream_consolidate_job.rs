//! The stream-consolidate job contract, shared by the producer (postgres control
//! plane, whatever enqueues consolidation) and the consumer (the worker). Lives in
//! core so a zero-pool worker can read it without depending on the postgres
//! adapter — mirrors `flush.rs`'s `FlushJob`/`FLUSH_JOB_KIND` shape.

/// The queue `kind` for a stream-consolidate job. Deliberately `const`, not
/// config: an identity/protocol invariant, not a deployment tunable.
pub const STREAM_CONSOLIDATE_JOB_KIND: &str = "stream_consolidate";

/// The payload of a `stream_consolidate` job: which CDC table's base to fold by
/// LastRow-per-identity (the engine-side `consolidate_stream` op).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StreamConsolidateJob {
    pub schema: String,
    pub name: String,
}
