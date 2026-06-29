//! The build-vector-index job contract, shared by the producer (enqueue) and the
//! consumer (worker → engine RPC). Lives in core so a zero-pool worker can read it
//! without the postgres adapter.

/// The queue `kind` for a vector-index build. Protocol invariant, not a tunable.
pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";

/// Payload of a `build_vector_index` job. References a named index declaration
/// (`ontology.vector_index_definition`); the build resolves kind/metric/params
/// from the declaration — the job carries no build knobs.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub index_name: String,
}
