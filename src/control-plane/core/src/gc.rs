//! The gc-table job contract, shared by the producer (operator HTTP endpoint)
//! and the consumer (the worker). Lives in core so a zero-pool worker can read it
//! without depending on the postgres adapter.

/// The queue `kind` for a physical-GC job.
pub const GC_JOB_KIND: &str = "gc_table";

/// The payload of a `gc_table` job: which table to GC.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GcJob {
    pub schema: String,
    pub name: String,
}
