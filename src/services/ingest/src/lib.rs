//! loom ingest: the landing-edge materializer. Arrow batches -> registered
//! snapshot + lineage via the part-1 snapshot-commit primitive, with an
//! optional model-conformance gate. See the spec under docs/superpowers/specs/.

pub mod bind;
pub mod config;
pub mod gate;
pub mod http;
pub mod landing;
pub mod materialize;
pub mod model;
pub mod openapi;

pub use config::RoutingTuning;
pub use openapi::build_openapi;

pub use bind::{BindError, BindViolation, BindViolationReason, bind, bind_link};
pub use gate::{ColumnShape, ModelShape, Violation, ViolationReason};
pub use materialize::{MaterializeRequest, materialize};
pub use model::model_shape_from_type;

/// Everything that can go wrong landing data. No partial catalog state is ever
/// committed:
/// - Failures before `cp.begin()` (gate / write) leave no open transaction
///   and no catalog rows.
/// - Failures after `begin()` but before `commit()` drop the `Tx`, which rolls
///   back (the postgres adapter auto-rolls back on drop; the memory adapter only
///   staged in memory). A write-then-commit failure may orphan the Parquet files
///   (documented in `materialize`); GC is a deferred concern.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The batch did not satisfy the supplied model (rejected before any write).
    #[error("data does not conform to model: {} violation(s)", .0.len())]
    DoesNotConform(Vec<Violation>),
    #[error(transparent)]
    Infer(#[from] datafusion_io::InferError),
    #[error(transparent)]
    Write(#[from] datafusion_io::WriteError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    /// commit() returned None — a catalog op was staged yet no snapshot was
    /// produced. Indicates an adapter contract violation; surfaced, never ignored.
    #[error("commit produced no snapshot id")]
    NoSnapshot,
}
