//! loom ingest: the landing-edge materializer. Arrow batches -> registered
//! DuckLake snapshot + lineage via the part-1 snapshot-commit primitive, with an
//! optional model-conformance gate. See the spec under docs/superpowers/specs/.

pub mod gate;
pub mod infer;
pub mod materialize;
pub mod store;
pub mod write;

pub use gate::{ColumnShape, ModelShape, Violation, ViolationReason};
pub use materialize::{MaterializeRequest, materialize};

/// Everything that can go wrong landing data. Fail-fast: a failure before
/// `commit` leaves no catalog rows (commit is never reached).
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The batch did not satisfy the supplied model (rejected before any write).
    #[error("data does not conform to model: {} violation(s)", .0.len())]
    DoesNotConform(Vec<Violation>),
    #[error(transparent)]
    Infer(#[from] infer::InferError),
    #[error(transparent)]
    Write(#[from] write::WriteError),
    #[error(transparent)]
    Store(#[from] store::StoreError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    /// commit() returned None — a catalog op was staged yet no snapshot was
    /// produced. Indicates an adapter contract violation; surfaced, never ignored.
    #[error("commit produced no snapshot id")]
    NoSnapshot,
}
