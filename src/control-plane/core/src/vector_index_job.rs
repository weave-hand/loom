//! The build-vector-index job contract, shared by the producer (enqueue) and the
//! consumer (worker → engine RPC). Mirrors `flush.rs`. Lives in core so a
//! zero-pool worker can read it without the postgres adapter.

/// The queue `kind` for a vector-index build. Protocol invariant, not a tunable.
pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";

/// Payload of a `build_vector_index` job: which `(schema, name)` table and which
/// `vector(N)` column to index.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub column: String,
}
