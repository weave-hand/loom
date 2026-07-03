//! The transform job contracts, shared by producers and the consumer (the
//! zero-pool worker). Lives in core so a zero-pool worker can read them without
//! the postgres adapter. Mirrors `compact_job.rs`.

use crate::TableRef;

/// The queue `kind` for a physical SQL transform job.
pub const TRANSFORM_JOB_KIND: &str = "transform";

/// The queue `kind` for an ontology-typed SQL transform job.
pub const TYPED_TRANSFORM_JOB_KIND: &str = "typed-transform";

/// How a transform's result lands in the output table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    /// Add the result's files to the table (today's behavior).
    #[default]
    Append,
    /// Replace the table's live contents with the result (older snapshots time-travel).
    Overwrite,
}

/// Payload of a `"transform"` job: physical SQL over table-named inputs.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TransformJob {
    pub inputs: Vec<TableRef>,
    pub output: TableRef,
    pub sql: String,
    #[serde(default)]
    pub output_mode: OutputMode,
}

/// Payload of a `"typed-transform"` job: SQL over ontology-type-named inputs.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TypedTransformJob {
    pub inputs: Vec<String>,
    pub output: String,
    pub sql: String,
    #[serde(default)]
    pub output_mode: OutputMode,
}
