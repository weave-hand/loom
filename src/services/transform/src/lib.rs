//! loom transform service: queue-driven SQL transforms. A worker reads input
//! DuckLake table(s) with DataFusion, runs a SQL query, and commits the result as a
//! new snapshot + lineage. See docs/superpowers/specs/.

pub mod conform;
pub mod handler;
pub mod run;
pub mod typed;

pub use handler::{transform_handler, typed_transform_handler};
pub use run::{TransformError, TransformInput, TransformRequest, run_transform};
pub use typed::{TypedTransformError, run_typed_transform};
