//! The compact-table job contract, shared by the producer (the operator ingest
//! endpoint) and the consumer (the zero-pool worker). Lives in core so a zero-pool
//! worker can read it without the postgres adapter. Mirrors `flush.rs`.

/// The queue `kind` for an operator-triggered compaction job.
pub const COMPACT_JOB_KIND: &str = "compact_table";

/// The payload of a `compact_table` job: which table to compact.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CompactJob {
    pub schema: String,
    pub name: String,
}
