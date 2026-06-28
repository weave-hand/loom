//! The build-vector-index job contract, shared by the producer (enqueue) and the
//! consumer (worker → engine RPC). Mirrors `flush.rs`. Lives in core so a
//! zero-pool worker can read it without the postgres adapter.

use crate::error::Result;
use crate::vector_index::IndexSpec;

/// The queue `kind` for a vector-index build. Protocol invariant, not a tunable.
pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";

/// Payload of a `build_vector_index` job. `index_kind`/`nlist` are optional and
/// default to absent (⇒ exact `Flat`) so payloads written before IVF existed
/// still deserialize.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub column: String,
    #[serde(default)]
    pub index_kind: Option<String>,
    #[serde(default)]
    pub nlist: Option<u32>,
}

impl BuildVectorIndexJob {
    /// Resolve the payload's `(index_kind, nlist)` into an `IndexSpec`.
    pub fn index_spec(&self) -> Result<IndexSpec> {
        IndexSpec::from_label(self.index_kind.as_deref(), self.nlist)
    }
}
