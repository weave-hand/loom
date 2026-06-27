//! The flush-table job contract, shared by the producer (postgres `inline_append`)
//! and the consumer (the worker). Lives in core so a zero-pool worker can read it
//! without depending on the postgres adapter.

/// The queue `kind` for an inline-flush job. Deliberately `const`, not config: an
/// identity/protocol invariant, not a deployment tunable. See road-config-seam-unification.
pub const FLUSH_JOB_KIND: &str = "flush_table";

/// The payload of a `flush_table` job: which table to flush.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct FlushJob {
    pub schema: String,
    pub name: String,
}
